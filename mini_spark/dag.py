from dataclasses import dataclass


@dataclass(frozen=True)
class DAGNode:
    id: int
    operation: str
    dependency_kind: str | None


@dataclass(frozen=True)
class Stage:
    id: int
    operations: list[str]


@dataclass(frozen=True)
class ExecutionDAG:
    nodes: list[DAGNode]

    @classmethod
    def from_rdd(cls, rdd) -> "ExecutionDAG":
        records: list[tuple[str, str | None]] = []
        current = rdd
        while current is not None:
            dependency_kind = None if current._parent is None else current._dependency_kind
            records.append((current._operation, dependency_kind))
            current = current._parent

        records.reverse()
        return cls(
            nodes=[
                DAGNode(id=index, operation=operation, dependency_kind=dependency_kind)
                for index, (operation, dependency_kind) in enumerate(records)
            ]
        )

    def stages(self) -> list[Stage]:
        stages: list[Stage] = []
        current_operations: list[str] = []

        for node in self.nodes:
            if node.dependency_kind == "wide" and current_operations:
                stages.append(Stage(id=len(stages), operations=current_operations))
                current_operations = []
            current_operations.append(node.operation)

        if current_operations:
            stages.append(Stage(id=len(stages), operations=current_operations))

        return stages

    def to_debug_string(self) -> str:
        return "\n".join(
            f"{node.id}: {node.operation} ({node.dependency_kind or 'root'})"
            for node in self.nodes
        )
