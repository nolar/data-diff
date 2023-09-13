from typing import Any, Dict
from abc import ABC, abstractmethod

import attrs


@attrs.define(kw_only=True, frozen=True)
class AbstractCompiler(ABC):
    @abstractmethod
    def compile(self, elem: Any, params: Dict[str, Any] = None) -> str:
        ...


@attrs.define(kw_only=True, frozen=False)
class Compilable(ABC):
    # TODO generic syntax, so we can write Compilable[T] for expressions returning a value of type T
    @abstractmethod
    def compile(self, c: AbstractCompiler) -> str:
        ...
