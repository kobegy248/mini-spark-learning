from collections.abc import Callable, Iterable
from typing import Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class RDD(Generic[T]):
    """A tiny immutable local RDD for learning Spark's execution model."""

    def __init__(
        self,
        data: Iterable[T] | None = None,
        parent: "RDD[object] | None" = None,
        transform: Callable[[Iterable[object]], Iterable[T]] | None = None,
    ) -> None:
        if data is None and parent is None:
            raise ValueError("RDD needs either source data or a parent RDD")
        if data is not None and parent is not None:
            raise ValueError("RDD cannot have both source data and a parent RDD")
        if parent is None and transform is not None:
            raise ValueError("Root RDD cannot have a transform")
        if parent is not None and transform is None:
            raise ValueError("Derived RDD needs a transform")

        self._data = tuple(data) if data is not None else None
        self._parent = parent
        self._transform = transform

    def map(self, function: Callable[[T], U]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (function(value) for value in values),
        )

    def filter(self, function: Callable[[T], bool]) -> "RDD[T]":
        return RDD(
            parent=self,
            transform=lambda values: (value for value in values if function(value)),
        )

    def flat_map(self, function: Callable[[T], Iterable[U]]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (
                item for value in values for item in function(value)
            ),
        )

    def collect(self) -> list[T]:
        return list(self._compute())

    def _compute(self) -> Iterable[T]:
        if self._parent is None:
            if self._data is None:
                raise RuntimeError("Root RDD has no source data")
            return self._data

        if self._transform is None:
            raise RuntimeError("Derived RDD has no transform")

        return self._transform(self._parent._compute())
