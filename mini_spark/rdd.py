from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import chain
from itertools import islice
from typing import Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True)
class Dependency:
    parent: "RDD[object]"
    kind: str


@dataclass(frozen=True)
class Partition(Generic[T]):
    index: int
    data: tuple[T, ...]


class RDD(Generic[T]):
    """A tiny immutable local RDD for learning Spark's execution model."""

    def __init__(
        self,
        data: Iterable[T] | None = None,
        parent: "RDD[object] | None" = None,
        transform: Callable[[Iterable[object]], Iterable[T]] | None = None,
        operation: str | None = None,
        dependency_kind: str = "narrow",
        num_slices: int = 1,
    ) -> None:
        if num_slices <= 0:
            raise ValueError("num_slices must be positive")
        if data is None and parent is None:
            raise ValueError("RDD needs either source data or a parent RDD")
        if data is not None and parent is not None:
            raise ValueError("RDD cannot have both source data and a parent RDD")
        if parent is None and transform is not None:
            raise ValueError("Root RDD cannot have a transform")
        if parent is not None and transform is None:
            raise ValueError("Derived RDD needs a transform")

        self._partitions = (
            self._split_partitions(tuple(data), num_slices)
            if data is not None
            else None
        )
        self._parent = parent
        self._transform = transform
        self._operation = operation or ("parallelize" if parent is None else "transform")
        self._dependency_kind = dependency_kind

    @staticmethod
    def _split_partitions(data: tuple[T, ...], num_slices: int) -> tuple[Partition[T], ...]:
        base_size, remainder = divmod(len(data), num_slices)
        partitions: list[Partition[T]] = []
        start = 0
        for index in range(num_slices):
            size = base_size + (1 if index < remainder else 0)
            end = start + size
            partitions.append(Partition(index=index, data=data[start:end]))
            start = end
        return tuple(partitions)

    def map(self, function: Callable[[T], U]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (function(value) for value in values),
            operation="map",
        )

    def filter(self, function: Callable[[T], bool]) -> "RDD[T]":
        return RDD(
            parent=self,
            transform=lambda values: (value for value in values if function(value)),
            operation="filter",
        )

    def flat_map(self, function: Callable[[T], Iterable[U]]) -> "RDD[U]":
        return RDD(
            parent=self,
            transform=lambda values: (
                item for value in values for item in function(value)
            ),
            operation="flat_map",
        )

    def collect(self) -> list[T]:
        return list(self._compute())

    def count(self) -> int:
        return sum(1 for _ in self._compute())

    def first(self) -> T:
        for value in self._compute():
            return value
        raise ValueError("first called on empty RDD")

    def take(self, count: int) -> list[T]:
        if count < 0:
            raise ValueError("take count must be non-negative")
        return list(islice(self._compute(), count))

    def reduce(self, function: Callable[[T, T], T]) -> T:
        iterator = iter(self._compute())
        try:
            result = next(iterator)
        except StopIteration as exc:
            raise ValueError("reduce called on empty RDD") from exc

        for value in iterator:
            result = function(result, value)
        return result

    def dependencies(self) -> list[Dependency]:
        if self._parent is None:
            return []
        return [Dependency(parent=self._parent, kind=self._dependency_kind)]

    def lineage(self) -> list[str]:
        if self._parent is None:
            return [self._operation]
        return [*self._parent.lineage(), self._operation]

    def to_debug_string(self) -> str:
        lines: list[str] = []
        self._append_debug_lines(lines, depth=0)
        return "\n".join(lines)

    def _append_debug_lines(self, lines: list[str], depth: int) -> None:
        lines.append(f"{'  ' * depth}{self._operation}")
        if self._parent is not None:
            self._parent._append_debug_lines(lines, depth + 1)

    def _compute(self) -> Iterable[T]:
        return chain.from_iterable(self._compute_partitions())

    def collect_partitions(self) -> list[list[T]]:
        return [list(partition_data) for partition_data in self._compute_partitions()]

    def partitions(self) -> list[Partition[T]]:
        if self._parent is None:
            if self._partitions is None:
                raise RuntimeError("Root RDD has no partitions")
            return list(self._partitions)

        return [
            Partition(index=index, data=tuple(partition_data))
            for index, partition_data in enumerate(self._compute_partitions())
        ]

    def num_partitions(self) -> int:
        if self._parent is None:
            if self._partitions is None:
                raise RuntimeError("Root RDD has no partitions")
            return len(self._partitions)
        return self._parent.num_partitions()

    def _compute_partitions(self) -> list[Iterable[T]]:
        if self._parent is None:
            if self._partitions is None:
                raise RuntimeError("Root RDD has no partitions")
            return [partition.data for partition in self._partitions]

        if self._transform is None:
            raise RuntimeError("Derived RDD has no transform")

        return [
            self._transform(parent_partition)
            for parent_partition in self._parent._compute_partitions()
        ]
