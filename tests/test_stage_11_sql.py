from mini_spark.sql import (
    Filter,
    MiniSparkSession,
    PhysicalPlan,
    Project,
    TableScan,
)


def test_sql_selects_columns_from_registered_table():
    spark = MiniSparkSession()
    spark.create_table(
        "people",
        [
            {"name": "alice", "age": 20},
            {"name": "bob", "age": 17},
        ],
    )

    result = spark.sql("select name from people").collect()

    assert result == [{"name": "alice"}, {"name": "bob"}]


def test_sql_where_filters_rows():
    spark = MiniSparkSession()
    spark.create_table(
        "people",
        [
            {"name": "alice", "age": 20},
            {"name": "bob", "age": 17},
        ],
    )

    result = spark.sql("select name, age from people where age > 18").collect()

    assert result == [{"name": "alice", "age": 20}]


def test_sql_builds_logical_plan():
    spark = MiniSparkSession()
    spark.create_table("people", [{"name": "alice", "age": 20}])

    query = spark.sql("select name from people where age > 18")

    assert query.logical_plan == Project(
        columns=["name"],
        child=Filter(
            column="age",
            operator=">",
            value=18,
            child=TableScan(table_name="people"),
        ),
    )


def test_sql_builds_physical_plan_from_logical_plan():
    spark = MiniSparkSession()
    spark.create_table("people", [{"name": "alice", "age": 20}])

    query = spark.sql("select name from people where age > 18")

    assert query.physical_plan == PhysicalPlan(
        steps=[
            "scan table people",
            "filter age > 18",
            "project name",
        ]
    )


def test_explain_returns_logical_and_physical_plan_text():
    spark = MiniSparkSession()
    spark.create_table("people", [{"name": "alice", "age": 20}])

    query = spark.sql("select name from people where age > 18")

    assert query.explain() == (
        "Logical Plan:\n"
        "Project[name]\n"
        "  Filter[age > 18]\n"
        "    TableScan[people]\n"
        "\n"
        "Physical Plan:\n"
        "scan table people\n"
        "filter age > 18\n"
        "project name"
    )
