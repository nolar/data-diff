"""Provides classes for performing a table diff
"""
import time
from abc import ABC, abstractmethod
from enum import Enum
from contextlib import contextmanager
from functools import partial
from typing import Any, Callable, Collection, Dict, Generator, Iterable, List, Sequence, Tuple, Iterator, \
    Optional, \
    TypeVar, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import attrs

from data_diff.info_tree import InfoTree, SegmentInfo

from .utils import PK, dbt_diff_string_template, run_as_daemon, safezip, getLogger, truncate_error, Vector
from .thread_utils import DiffOp, ThreadedYielder, DiffItem
from .table_segment import Range, TableSegment, create_mesh_from_points
from .tracking import create_end_event_json, create_start_event_json, send_event_json, is_tracking_enabled
from data_diff.sqeleton.abcs import IKey

logger = getLogger(__name__)
R = TypeVar('R')

# TODO: these are hgher-level things in this lower-level module, this introduced circular imports,
#   get rid of it somehow and make it strict again, e.g. by using attr classes instead of dicts
# from .hashdiff_tables import HashDifferStats
# from .joindiff_tables import JoinDifferStats
# DifferStats = Union[JoinDifferStats, HashDifferStats]
DifferStats = Dict[str, Any]


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


@attrs.define(kw_only=True)
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    def _thread_call(self, *funcs: Callable[[], R]) -> Iterable[R]:
        if not self.threaded:
            return (func() for func in funcs)

        max_workers = min(self.max_threadpool_size, len(funcs))
        with ThreadPoolExecutor(max_workers=max_workers) as task_pool:
            futures = [task_pool.submit(func) for func in funcs]
            for future in futures:
                yield future.result()

    def _thread_as_completed(self, *funcs: Callable[[], R]) -> Iterable[R]:
        if not self.threaded:
            return (func() for func in funcs)

        max_workers = min(self.max_threadpool_size, len(funcs))
        with ThreadPoolExecutor(max_workers=max_workers) as task_pool:
            futures = [task_pool.submit(func) for func in funcs]
            for future in as_completed(futures):
                yield future.result()

    @contextmanager
    def _run_in_background(self, *funcs: Optional[Callable[[], R]]) -> Iterator[None]:
        real_funcs = [func for func in funcs if func is not None]
        max_workers = min(self.max_threadpool_size, len(real_funcs))
        with ThreadPoolExecutor(max_workers=max_workers) as task_pool:
            futures = [task_pool.submit(func) for func in real_funcs]
            yield
            for f in futures:
                f.result()


@attrs.define(kw_only=True, frozen=True)
class DiffStats:
    diff_by_sign: Dict[DiffOp, int]
    table1_count: int
    table2_count: int
    unchanged: int
    diff_percent: float
    extra_column_diffs: Dict[str, int]


@attrs.define(frozen=True, kw_only=True)
class DiffResultWrapper:
    diff: Generator[DiffItem, None, None]
    info_tree: InfoTree
    stats: DifferStats
    result_list: List[DiffItem] = attrs.field(factory=list)

    def __iter__(self):
        yield from self.result_list
        for i in self.diff:
            self.result_list.append(i)
            yield i

    def _get_stats(self, is_dbt: bool = False) -> DiffStats:
        list(self)  # Consume the iterator into result_list, if we haven't already

        key_columns = self.info_tree.info.tables[0].key_columns
        len_key_columns = len(key_columns)
        diff_by_key: Dict[Tuple[PK, ...], DiffOp] = {}
        extra_column_values: Sequence[PK] = ()
        extra_column_diffs: Dict[str, int] = {}
        extra_columns: Sequence[str] = ()
        if is_dbt:
            extra_column_values_store = {}
            extra_columns: Sequence[str] = self.info_tree.info.tables[0].extra_columns
            extra_column_diffs = {k: 0 for k in extra_columns}

        for sign, values in self.result_list:
            k: Tuple[PK, ...] = tuple(values[:len_key_columns])
            if is_dbt:
                extra_column_values = values[len_key_columns:]
            if k in diff_by_key:
                assert sign != diff_by_key[k]
                diff_by_key[k] = "!"
                if is_dbt:
                    for i, k in enumerate(extra_columns):
                        if extra_column_values[i] != extra_column_values_store[k][i]:
                            extra_column_diffs[k] += 1
            else:
                diff_by_key[k] = sign
                if is_dbt:
                    extra_column_values_store[k] = extra_column_values

        diff_by_sign: Dict[DiffOp, int] = {k: 0 for k in "+-!"}
        for sign in diff_by_key.values():
            diff_by_sign[sign] += 1

        table1_count = self.info_tree.info.rowcounts[1]
        table2_count = self.info_tree.info.rowcounts[2]
        unchanged = table1_count - diff_by_sign["-"] - diff_by_sign["!"]
        diff_percent = 1 - unchanged / max(table1_count, table2_count)

        return DiffStats(diff_by_sign, table1_count, table2_count, unchanged, diff_percent, extra_column_diffs)

    def get_stats_string(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)

        if is_dbt:
            string_output = dbt_diff_string_template(
                diff_stats.diff_by_sign["+"],
                diff_stats.diff_by_sign["-"],
                diff_stats.diff_by_sign["!"],
                diff_stats.unchanged,
                diff_stats.extra_column_diffs,
                "Values Updated:",
            )

        else:
            string_output = ""
            string_output += f"{diff_stats.table1_count} rows in table A\n"
            string_output += f"{diff_stats.table2_count} rows in table B\n"
            string_output += f"{diff_stats.diff_by_sign['-']} rows exclusive to table A (not present in B)\n"
            string_output += f"{diff_stats.diff_by_sign['+']} rows exclusive to table B (not present in A)\n"
            string_output += f"{diff_stats.diff_by_sign['!']} rows updated\n"
            string_output += f"{diff_stats.unchanged} rows unchanged\n"
            string_output += f"{100*diff_stats.diff_percent:.2f}% difference score\n"

            if self.stats:
                string_output += "\nExtra-Info:\n"
                for k, v in sorted(self.stats.items()):
                    string_output += f"  {k} = {v}\n"

        return string_output

    def get_stats_dict(self, is_dbt: bool = False):
        diff_stats = self._get_stats(is_dbt)
        json_output = {
            "rows_A": diff_stats.table1_count,
            "rows_B": diff_stats.table2_count,
            "exclusive_A": diff_stats.diff_by_sign["-"],
            "exclusive_B": diff_stats.diff_by_sign["+"],
            "updated": diff_stats.diff_by_sign["!"],
            "unchanged": diff_stats.unchanged,
            "total": sum(diff_stats.diff_by_sign.values()),
            "stats": self.stats,
        }
        json_output["values"] = diff_stats.extra_column_diffs or {}
        return json_output


@attrs.define(kw_only=True)
class TableDiffer(ThreadBase, ABC):
    bisection_factor = 32
    stats: DifferStats = attrs.field(factory=dict)

    def diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree = None) -> DiffResultWrapper:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table

        Returns:
            An iterator that yield pair-tuples, representing the diff. Items can be either -
            ('-', row) for items in table1 but not in table2.
            ('+', row) for items in table2 but not in table1.
            Where `row` is a tuple of values, corresponding to the diffed columns.
        """
        if info_tree is None:
            info_tree = InfoTree(SegmentInfo([table1, table2]))
        return DiffResultWrapper(
            diff=self._diff_tables_wrapper(table1, table2, info_tree),
            info_tree=info_tree,
            stats=self.stats,
        )

    def _diff_tables_wrapper(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> Iterable[DiffItem]:
        if is_tracking_enabled():
            options = attrs.asdict(self, recurse=False)
            options["differ_name"] = type(self).__name__
            event_json = create_start_event_json(options)
            run_as_daemon(send_event_json, event_json)

        start = time.monotonic()
        error = None
        try:
            # Query and validate schema
            table1, table2 = self._thread_call(table1.with_schema, table2.with_schema)
            self._validate_and_adjust_columns(table1, table2)

            yield from self._diff_tables_root(table1, table2, info_tree)

        except BaseException as e:  # Catch KeyboardInterrupt too
            error = e
        finally:
            info_tree.aggregate_info()

            if is_tracking_enabled():
                runtime = time.monotonic() - start
                rowcounts = info_tree.info.rowcounts
                table1_count = rowcounts[1] if rowcounts else None
                table2_count = rowcounts[2] if rowcounts else None
                diff_count = info_tree.info.diff_count
                err_message = truncate_error(repr(error))
                event_json = create_end_event_json(
                    error is None,
                    runtime,
                    table1.database.name,
                    table2.database.name,
                    table1_count,
                    table2_count,
                    diff_count,
                    err_message,
                )
                send_event_json(event_json)

            if error:
                raise error

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment) -> Iterable[DiffItem]:
        yield from ()  # to make the function an empty generator

    def _diff_tables_root(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> Iterable[DiffItem]:
        return self._bisect_and_diff_tables(table1, table2, info_tree)

    @abstractmethod
    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        max_rows: int,
        # TODO: these three must be a single class which addresses the segment, including its parents!
        #       e.g. [5/32, 13/32, 12/16] for the 3rd level's 12th segment.
        #       And log them accordingly, in full, since in threaded mode it is unclear who is who.
        #       MAYBE even a part of TableSegments above, e.g. TableSegment.path, to avoid duplication.
        level: int = 0,
        segment_index: Optional[int] = None,
        segment_count: Optional[int] = None,
    ) -> Iterable[DiffItem]:
        raise NotImplementedError

    def _bisect_and_diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree) -> Iterable[DiffItem]:
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")

        key_types1 = [table1._schema[i] for i in table1.key_columns]
        key_types2 = [table2._schema[i] for i in table2.key_columns]

        for kt in key_types1 + key_types2:
            if not isinstance(kt, IKey):
                raise NotImplementedError(f"Cannot use a column of type {kt} as a key")

        for kt1, kt2 in safezip(key_types1, key_types2):
            if kt1.python_type is not kt2.python_type:
                raise TypeError(f"Incompatible key types: {kt1} and {kt2}")

        # Query min/max values
        key_ranges = self._thread_as_completed(table1.query_key_range, table2.query_key_range)

        # Start with the first completed value, so we don't waste time waiting
        min_key1, max_key1 = self._parse_key_range_result(key_types1, next(key_ranges))

        btable1 = table1.new_key_bounds(min_key=min_key1, max_key=max_key1)
        btable2 = table2.new_key_bounds(min_key=min_key1, max_key=max_key1)

        logger.info(
            f"Diffing segments at key-range: {btable1.min_key}..{btable2.max_key}. "
            f"size: table1 <= {btable1.approximate_size()}, table2 <= {btable2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(partial(
            self._bisect_and_diff_segments,
            ti=ti,
            table1=btable1,
            table2=btable2,
            info_tree=info_tree,
        ))

        # Now we check for the second min-max, to diff the portions we "missed".
        # This is achieved by subtracting the table ranges, and dividing the resulting space into aligned boxes.
        # For example, given tables A & B, and a 2D compound key, where A was queried first for key-range,
        # the regions of B we need to diff in this second pass are marked by B1..8:
        # ┌──┬──────┬──┐
        # │B1│  B2  │B3│
        # ├──┼──────┼──┤
        # │B4│  A   │B5│
        # ├──┼──────┼──┤
        # │B6│  B7  │B8│
        # └──┴──────┴──┘
        # Overall, the max number of new regions in this 2nd pass is 3^|k| - 1

        min_key2, max_key2 = self._parse_key_range_result(key_types1, next(key_ranges))

        points = [list(sorted(p)) for p in safezip(min_key1, min_key2, max_key1, max_key2)]
        box_mesh: Collection[Range] = create_mesh_from_points(*points)

        new_regions = [range for range in box_mesh if range.start < range.end and not (range.start >= min_key1 and range.end <= max_key1)]

        for range in new_regions:
            ti.submit(partial(
                self._bisect_and_diff_segments,
                ti=ti,
                table1=table1.new_key_bounds(min_key=range.start, max_key=range.end),
                table2=table2.new_key_bounds(min_key=range.start, max_key=range.end),
                info_tree=info_tree,
            ))

        return ti

    def _parse_key_range_result(self, key_types, key_range) -> Tuple[Vector, Vector]:
        min_key_values, max_key_values = key_range

        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            min_key = Vector(key_type.make_value(mn) for key_type, mn in safezip(key_types, min_key_values))
            max_key = Vector(key_type.make_value(mx) + 1 for key_type, mx in safezip(key_types, max_key_values))
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_types} to '{min_key_values}', '{max_key_values}'.") from e

        return min_key, max_key

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows: Optional[int] = None,
    ) -> None:
        assert table1.is_bounded and table2.is_bounded

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = table1 if table1.approximate_size() >= table2.approximate_size() else table2
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            info_node = info_tree.add_node(t1, t2, max_rows=max_rows)
            ti.submit(partial(
                self._diff_segments,
                ti=ti,
                table1=t1,
                table2=t2,
                info_tree=info_node,
                max_rows=max_rows,
                level=level + 1,
                segment_index=i + 1,
                segment_count=len(segmented1),
            ), priority=level)
