from collections.abc import Iterable
from typing import Generic, TypeVar

T = TypeVar("T")


class RDD(Generic[T]):
    """A tiny immutable local RDD for learning Spark's execution model."""

    def __init__(self, data: Iterable[T]) -> None:
        self._data = tuple(data)

    def collect(self) -> list[T]:
        return list(self._data)
