from mini_spark import SparkContext
from mini_spark.scheduler import LocalScheduler, Task, TaskResult


def test_scheduler_creates_one_task_per_partition():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2)
    scheduler = LocalScheduler()

    results = scheduler.run_job(rdd, lambda values: list(values), operation="collect")

    assert scheduler.last_tasks == [
        Task(partition_index=0, operation="collect"),
        Task(partition_index=1, operation="collect"),
    ]
    assert results == [
        TaskResult(partition_index=0, value=[1, 2]),
        TaskResult(partition_index=1, value=[3, 4]),
    ]


def test_scheduler_runs_transformed_partitions():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)
    scheduler = LocalScheduler()

    results = scheduler.run_job(rdd, lambda values: sum(values), operation="sum")

    assert [result.value for result in results] == [30, 70]


def test_count_uses_scheduler_per_partition():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3, 4, 5], num_slices=2)

    assert rdd.count() == 5
    assert rdd.last_job_tasks() == [
        Task(partition_index=0, operation="count"),
        Task(partition_index=1, operation="count"),
    ]


def test_collect_uses_scheduler_per_partition():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3, 4], num_slices=2).map(lambda value: value * 10)

    assert rdd.collect() == [10, 20, 30, 40]
    assert rdd.last_job_tasks() == [
        Task(partition_index=0, operation="collect"),
        Task(partition_index=1, operation="collect"),
    ]
