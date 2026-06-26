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
        wide_transform: Callable[[list[Iterable[object]]], list[Iterable[T]]] | None = None,
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
        if parent is None and (transform is not None or wide_transform is not None):
            raise ValueError("Root RDD cannot have a transform")
        if parent is not None and transform is None and wide_transform is None:
            raise ValueError("Derived RDD needs a transform")
        if transform is not None and wide_transform is not None:
            raise ValueError("RDD cannot have both narrow and wide transforms")

        self._partitions = (
            self._split_partitions(tuple(data), num_slices)
            if data is not None
            else None
        )
        self._parent = parent
        self._transform = transform
        self._wide_transform = wide_transform
        self._operation = operation or ("parallelize" if parent is None else "transform")
        self._dependency_kind = dependency_kind
        self._num_slices = num_slices
        self._last_job_tasks = []
        self._cache_enabled = False
        self._cached_partitions: tuple[tuple[T, ...], ...] | None = None
        self._cache_hits = 0
        self._cache_misses = 0

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

    def group_by_key(self, num_partitions: int | None = None):
        target_partitions = num_partitions or self.num_partitions()

        def wide_transform(parent_partitions):
            grouped = {}
            key_order = []
            for key, value in chain.from_iterable(parent_partitions):
                if key not in grouped:
                    grouped[key] = []
                    key_order.append(key)
                grouped[key].append(value)

            output = [[] for _ in range(target_partitions)]
            for key in key_order:
                output[self._partition_for_key(key, target_partitions)].append(
                    (key, grouped[key])
                )
            return output

        return RDD(
            parent=self,
            wide_transform=wide_transform,
            operation="group_by_key",
            dependency_kind="wide",
            num_slices=target_partitions,
        )

    def reduce_by_key(self, function, num_partitions: int | None = None):
        target_partitions = num_partitions or self.num_partitions()

        def wide_transform(parent_partitions):
            reduced = {}
            key_order = []
            for key, value in chain.from_iterable(parent_partitions):
                if key not in reduced:
                    reduced[key] = value
                    key_order.append(key)
                else:
                    reduced[key] = function(reduced[key], value)

            output = [[] for _ in range(target_partitions)]
            for key in key_order:
                output[self._partition_for_key(key, target_partitions)].append(
                    (key, reduced[key])
                )
            return output

        return RDD(
            parent=self,
            wide_transform=wide_transform,
            operation="reduce_by_key",
            dependency_kind="wide",
            num_slices=target_partitions,
        )

    @staticmethod
    def _partition_for_key(key, num_partitions: int) -> int:
        return hash(key) % num_partitions

    def cache(self) -> "RDD[T]":
        self._cache_enabled = True
        return self

    def persist(self) -> "RDD[T]":
        return self.cache()

    def is_cached(self) -> bool:
        return self._cache_enabled

    def cache_info(self) -> dict[str, int | bool]:
        return {
            "enabled": self._cache_enabled,
            "hits": self._cache_hits,
            "misses": self._cache_misses,
        }

    def collect(self) -> list[T]:
        from mini_spark.scheduler import LocalScheduler

        scheduler = LocalScheduler()
        results = scheduler.run_job(
            self,
            lambda values: list(values),
            operation="collect",
        )
        self._last_job_tasks = scheduler.last_tasks
        return list(chain.from_iterable(result.value for result in results))

    def count(self) -> int:
        from mini_spark.scheduler import LocalScheduler

        scheduler = LocalScheduler()
        results = scheduler.run_job(
            self,
            lambda values: sum(1 for _ in values),
            operation="count",
        )
        self._last_job_tasks = scheduler.last_tasks
        return sum(result.value for result in results)

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

    def last_job_tasks(self):
        return list(self._last_job_tasks)

    def dependencies(self) -> list[Dependency]:
        if self._parent is None:
            return []
        return [Dependency(parent=self._parent, kind=self._dependency_kind)]

    def dependency_kind(self) -> str | None:
        if self._parent is None:
            return None
        return self._dependency_kind

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
        if self._dependency_kind == "wide":
            return self._num_slices
        return self._parent.num_partitions()

    def _compute_partitions(self) -> list[Iterable[T]]:
        if self._cache_enabled and self._cached_partitions is not None:
            self._cache_hits += 1
            return [partition for partition in self._cached_partitions]

        computed = self._compute_partitions_uncached()
        if self._cache_enabled:
            self._cache_misses += 1
            self._cached_partitions = tuple(tuple(partition) for partition in computed)
            return [partition for partition in self._cached_partitions]
        return computed

    def _compute_partitions_uncached(self) -> list[Iterable[T]]:
        if self._parent is None:
            if self._partitions is None:
                raise RuntimeError("Root RDD has no partitions")
            return [partition.data for partition in self._partitions]

        if self._wide_transform is not None:
            return self._wide_transform(self._parent._compute_partitions())

        if self._transform is None:
            raise RuntimeError("Derived RDD has no transform")

        return [
            self._transform(parent_partition)
            for parent_partition in self._parent._compute_partitions()
        ]
