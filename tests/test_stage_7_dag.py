from mini_spark import SparkContext
from mini_spark.dag import DAGNode, ExecutionDAG, Stage


def test_dag_contains_operations_from_root_to_action_rdd():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).filter(
        lambda value: value > 10
    )

    dag = ExecutionDAG.from_rdd(rdd)

    assert dag.nodes == [
        DAGNode(id=0, operation="parallelize", dependency_kind=None),
        DAGNode(id=1, operation="map", dependency_kind="narrow"),
        DAGNode(id=2, operation="filter", dependency_kind="narrow"),
    ]


def test_narrow_dependencies_stay_in_one_stage():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10).filter(
        lambda value: value > 10
    )

    dag = ExecutionDAG.from_rdd(rdd)

    assert dag.stages() == [
        Stage(id=0, operations=["parallelize", "map", "filter"])
    ]


def test_dag_debug_string_marks_dependencies():
    sc = SparkContext()
    rdd = sc.parallelize([1, 2, 3]).map(lambda value: value * 10)

    dag = ExecutionDAG.from_rdd(rdd)

    assert dag.to_debug_string() == (
        "0: parallelize (root)\n"
        "1: map (narrow)"
    )


def test_wide_dependency_starts_new_stage_when_present():
    dag = ExecutionDAG(
        nodes=[
            DAGNode(id=0, operation="parallelize", dependency_kind=None),
            DAGNode(id=1, operation="map", dependency_kind="narrow"),
            DAGNode(id=2, operation="shuffle", dependency_kind="wide"),
            DAGNode(id=3, operation="reduce_by_key", dependency_kind="narrow"),
        ]
    )

    assert dag.stages() == [
        Stage(id=0, operations=["parallelize", "map"]),
        Stage(id=1, operations=["shuffle", "reduce_by_key"]),
    ]
