from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.LineageTableColumnId import LineageTableColumnId
from bifabrik.utils.lineage.LineageSerializationHelper import LineageSerializationHelper

class DataFrameLineageOutputColumn(LineageExpressionId):
    
    def __init__(self, column: LineageExpressionId = None, dependencies: list[LineageExpressionId] = []):
        if column is not None:
            self._id = column.id
            self._name = column.name
            self._sql = column.sql
        
        self._dependencies = dependencies

    @property
    def dependencies(self) -> list[LineageExpressionId]:
        return self._dependencies

    @property
    def tableColumnDependencies(self) -> list[LineageTableColumnId]:
         col_deps = list(filter(lambda x: isinstance(x, LineageTableColumnId), self._dependencies))
         return col_deps

    def __str__(self):
        res = f"{self.name}#{self.id} '{self.sql}'" #super().__str__()
        for dep in self._dependencies:
            res = res + f'\n--- {dep}'
        return res

    # Serialization to JSON
    def to_json_object(self):
        base = super().to_json_object()
        base['dependencies'] = [item.to_json_object() for item in self._dependencies]
        return base

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        #data = json.loads(json_str)
        instance = cls()
        instance._id = data.get("id")
        instance._name = data.get("name")
        instance._sql = data.get("sql")
        instance._dependencies = []
        deps = data.get("dependencies")
        for dep in deps:
            instance.dependencies.append(LineageSerializationHelper.deserialize_class(dep))
        return instance