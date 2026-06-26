from collections.abc import Iterable
from typing import TypeVar

from mini_spark.rdd import RDD

T = TypeVar("T")


class SparkContext:
    """Entry point for creating Mini Spark RDDs."""

    def parallelize(self, data: Iterable[T]) -> RDD[T]:
        return RDD(data)
