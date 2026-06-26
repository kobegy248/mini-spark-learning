from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
U = TypeVar("U")


@dataclass(frozen=True)
class Task:
    partition_index: int
    operation: str


@dataclass(frozen=True)
class TaskResult(Generic[U]):
    partition_index: int
    value: U


class LocalExecutor:
    """Runs one task in the current Python process."""

    def run(
        self,
        task: Task,
        partition_data: Iterable[T],
        function: Callable[[Iterable[T]], U],
    ) -> TaskResult[U]:
        return TaskResult(
            partition_index=task.partition_index,
            value=function(partition_data),
        )


class LocalScheduler:
    """Creates one local task per partition and runs them sequentially."""

    def __init__(self, executor: LocalExecutor | None = None) -> None:
        self._executor = executor or LocalExecutor()
        self.last_tasks: list[Task] = []

    def run_job(
        self,
        rdd,
        function: Callable[[Iterable[T]], U],
        operation: str,
    ) -> list[TaskResult[U]]:
        partition_data = rdd._compute_partitions()
        self.last_tasks = [
            Task(partition_index=index, operation=operation)
            for index, _ in enumerate(partition_data)
        ]

        return [
            self._executor.run(task, values, function)
            for task, values in zip(self.last_tasks, partition_data)
        ]
