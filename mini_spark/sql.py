import re
from dataclasses import dataclass
from typing import Any

from mini_spark import SparkContext


@dataclass(frozen=True)
class TableScan:
    table_name: str


@dataclass(frozen=True)
class Filter:
    column: str
    operator: str
    value: Any
    child: object


@dataclass(frozen=True)
class Project:
    columns: list[str]
    child: object


@dataclass(frozen=True)
class PhysicalPlan:
    steps: list[str]


class MiniSparkSession:
    def __init__(self) -> None:
        self._context = SparkContext()
        self._tables = {}

    def create_table(self, name: str, rows: list[dict[str, Any]]) -> None:
        self._tables[name] = self._context.parallelize(rows)

    def sql(self, query: str) -> "QueryResult":
        logical_plan = self._parse(query)
        physical_plan = self._build_physical_plan(logical_plan)
        return QueryResult(
            session=self,
            logical_plan=logical_plan,
            physical_plan=physical_plan,
        )

    def _parse(self, query: str) -> Project:
        match = re.fullmatch(
            r"\s*select\s+(.+?)\s+from\s+(\w+)(?:\s+where\s+(\w+)\s*(=|>|<)\s*(.+?))?\s*",
            query,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise ValueError("unsupported SQL")

        columns_text, table_name, filter_column, operator, raw_value = match.groups()
        columns = [column.strip() for column in columns_text.split(",")]
        child: object = TableScan(table_name=table_name)
        if filter_column is not None:
            child = Filter(
                column=filter_column,
                operator=operator,
                value=self._parse_value(raw_value),
                child=child,
            )
        return Project(columns=columns, child=child)

    @staticmethod
    def _parse_value(raw_value: str) -> Any:
        value = raw_value.strip().strip("'").strip('"')
        try:
            return int(value)
        except ValueError:
            return value

    def _build_physical_plan(self, logical_plan: object) -> PhysicalPlan:
        steps: list[str] = []

        def visit(plan: object) -> None:
            if isinstance(plan, Project):
                visit(plan.child)
                steps.append(f"project {', '.join(plan.columns)}")
            elif isinstance(plan, Filter):
                visit(plan.child)
                steps.append(f"filter {plan.column} {plan.operator} {plan.value}")
            elif isinstance(plan, TableScan):
                steps.append(f"scan table {plan.table_name}")
            else:
                raise TypeError(f"unknown logical plan: {plan!r}")

        visit(logical_plan)
        return PhysicalPlan(steps=steps)

    def _execute(self, logical_plan: object):
        if isinstance(logical_plan, Project):
            rdd = self._execute(logical_plan.child)
            return rdd.map(
                lambda row: {column: row[column] for column in logical_plan.columns}
            )

        if isinstance(logical_plan, Filter):
            rdd = self._execute(logical_plan.child)
            return rdd.filter(lambda row: self._matches(row, logical_plan))

        if isinstance(logical_plan, TableScan):
            if logical_plan.table_name not in self._tables:
                raise ValueError(f"unknown table: {logical_plan.table_name}")
            return self._tables[logical_plan.table_name]

        raise TypeError(f"unknown logical plan: {logical_plan!r}")

    @staticmethod
    def _matches(row: dict[str, Any], filter_plan: Filter) -> bool:
        left = row[filter_plan.column]
        right = filter_plan.value
        if filter_plan.operator == "=":
            return left == right
        if filter_plan.operator == ">":
            return left > right
        if filter_plan.operator == "<":
            return left < right
        raise ValueError(f"unsupported operator: {filter_plan.operator}")


@dataclass(frozen=True)
class QueryResult:
    session: MiniSparkSession
    logical_plan: Project
    physical_plan: PhysicalPlan

    def collect(self) -> list[dict[str, Any]]:
        return self.session._execute(self.logical_plan).collect()

    def explain(self) -> str:
        return (
            "Logical Plan:\n"
            f"{self._format_logical(self.logical_plan)}\n"
            "\n"
            "Physical Plan:\n"
            f"{chr(10).join(self.physical_plan.steps)}"
        )

    def _format_logical(self, plan: object, depth: int = 0) -> str:
        indent = "  " * depth
        if isinstance(plan, Project):
            return (
                f"{indent}Project[{', '.join(plan.columns)}]\n"
                f"{self._format_logical(plan.child, depth + 1)}"
            )
        if isinstance(plan, Filter):
            return (
                f"{indent}Filter[{plan.column} {plan.operator} {plan.value}]\n"
                f"{self._format_logical(plan.child, depth + 1)}"
            )
        if isinstance(plan, TableScan):
            return f"{indent}TableScan[{plan.table_name}]"
        raise TypeError(f"unknown logical plan: {plan!r}")
