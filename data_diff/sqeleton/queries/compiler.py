import random
from datetime import datetime
from typing import Any, Dict, Sequence, List

import attrs

from ..utils import ArithString
from ..abcs import AbstractDatabase, AbstractDialect, DbPath, AbstractCompiler, Compilable

import contextvars

cv_params = contextvars.ContextVar("params")


class CompileError(Exception):
    pass


@attrs.define(kw_only=True, frozen=True)
class Root:
    "Nodes inheriting from Root can be used as root statements in SQL (e.g. SELECT yes, RANDOM() no)"


@attrs.define(kw_only=True, frozen=True)
class Compiler(AbstractCompiler):
    database: AbstractDatabase
    params: dict = attrs.field(factory=dict)
    in_select: bool = False  # Compilation runtime flag
    in_join: bool = False  # Compilation runtime flag

    _table_context: List = attrs.field(factory=list)  # List[ITable]
    _subqueries: Dict[str, Any] = attrs.field(factory=dict)  # XXX not thread-safe
    root: bool = True

    _counter: List = attrs.field(factory=lambda: [0])

    @property
    def dialect(self) -> AbstractDialect:
        return self.database.dialect

    def compile(self, elem, params=None) -> str:
        if params:
            cv_params.set(params)

        if self.root and isinstance(elem, Compilable) and not isinstance(elem, Root):
            from .ast_classes import Select

            elem = Select(columns=[elem])

        res = self._compile(elem)
        if self.root and self._subqueries:
            subq = ", ".join(f"\n  {k} AS ({v})" for k, v in self._subqueries.items())
            self._subqueries.clear()
            return f"WITH {subq}\n{res}"
        return res

    def _compile(self, elem) -> str:
        if elem is None:
            return "NULL"
        elif isinstance(elem, Compilable):
            return elem.compile(attrs.evolve(self, root=False))
        elif isinstance(elem, str):
            return f"'{elem}'"
        elif isinstance(elem, (int, float)):
            return str(elem)
        elif isinstance(elem, datetime):
            return self.dialect.timestamp_value(elem)
        elif isinstance(elem, bytes):
            return f"b'{elem.decode()}'"
        elif isinstance(elem, ArithString):
            return f"'{elem}'"
        assert False, elem

    def new_unique_name(self, prefix="tmp"):
        self._counter[0] += 1
        return f"{prefix}{self._counter[0]}"

    def new_unique_table_name(self, prefix="tmp") -> DbPath:
        self._counter[0] += 1
        return self.database.parse_table_name(f"{prefix}{self._counter[0]}_{'%x'%random.randrange(2**32)}")

    def add_table_context(self, *tables: Sequence, **kw):
        return attrs.evolve(self, table_context=self._table_context + list(tables), **kw)

    def quote(self, s: str):
        return self.dialect.quote(s)
