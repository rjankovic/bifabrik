from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.LineageSerializationHelper import LineageSerializationHelper
from bifabrik.utils.lineage.DataFrameLineageOutputColumn import DataFrameLineageOutputColumn
from bifabrik.utils.lineage.LineageDependency import LineageDependency
from bifabrik.utils.lineage.LineageContext import LineageContext
import json

class DataFrameLineage:
    
    def __init__(self, columns: list[DataFrameLineageOutputColumn] = [], dependencies: list[LineageDependency] = [], expressions: list[LineageExpressionId] = [], 
        logicalPlan: str = None, targetLakehouseName: str = None, targetTableName: str = None, error = None):
        self.__columns = columns
        self.__dependencies = dependencies
        self.__expressions = expressions
        self.__logicalPlan = logicalPlan
        self.__error = error
        self.__context = LineageContext(targetLakehouseName = targetLakehouseName, targetTableName = targetTableName)
        

    # Getter and Setter for columns
    @property
    def columns(self) -> list["DataFrameLineageOutputColumn"]:
        return self.__columns

    @columns.setter
    def columns(self, value: list["DataFrameLineageOutputColumn"]):
        self.__columns = value

    # Getter and Setter for dependencies
    @property
    def dependencies(self) -> list["LineageDependency"]:
        return self.__dependencies

    @dependencies.setter
    def dependencies(self, value: list["LineageDependency"]):
        self.__dependencies = value

    # Getter and Setter for expressions
    @property
    def expressions(self) -> list["LineageExpressionId"]:
        return self.__expressions

    @expressions.setter
    def expressions(self, value: list["LineageExpressionId"]):
        self.__expressions = value

    # Getter and Setter for logicalPlan
    @property
    def logicalPlan(self) -> str:
        return self.__logicalPlan

    @logicalPlan.setter
    def logicalPlan(self, value: str):
        self.__logicalPlan = value

    # Getter and Setter for context
    @property
    def context(self) -> LineageContext:
        return self.__context

    @context.setter
    def context(self, value: LineageContext):
        self.__context = value

    @property
    def error(self) -> str:
        return self.__error

    @error.setter
    def error(self, value: str):
        self.__error = value

    def __str__(self):
        res = '\n'.join([str(item) for item in self.columns])
        return res
    
    def to_json_object(self):
        s_columns = []
        s_dependencies = []
        s_expressions = []

        for col in self.columns:
            s_columns.append(col.to_json_object())
        for dep in self.dependencies:
            s_dependencies.append(dep.to_json_object())
        for exp in self.expressions:
            s_expressions.append(exp.to_json_object())
        
        
        return {
            "columns": s_columns,
            "dependencies": s_dependencies,
            "expressions": s_expressions,
            "context": self.context.to_json_object(),
            "logicalPlan": self.logicalPlan,
            "error": self.error
        }

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        """Read a JSON-like object and deserialize a DataFrameLineage object."""
        #data = json.loads(json_str)
        # columns: list[DataFrameLineageOutputColumn], dependencies: list[LineageDependency], expressions: list[LineageExpressionId]
        instance = cls()
        instance.columns = [DataFrameLineageOutputColumn.from_json_object(item) for item in data['columns']]
        instance.dependencies = [LineageDependency.from_json_object(item) for item in data['dependencies']]
        instance.expressions = [LineageSerializationHelper.deserialize_class(item) for item in data['expressions']]
        instance.context = LineageContext.from_json_object(data['context'])
        instance.logicalPlan = data['logicalPlan']
        instance.error = data['error']
        
        return instance
    
    def to_json(self):
        """Serialize the DataFrameLineage object to a JSON string."""
        return json.dumps(self.to_json_object())

    def toJson(self):
        """Serialize the DataFrameLineage object to a JSON string."""
        return json.dumps(self.to_json_object())
    
    @classmethod
    def from_json(cls, json_string):
        """Read a JSON string and deserialize a DataFrameLineage object."""
        return cls.from_json_object(json.loads(json_string))
    
    @classmethod
    def fromJson(cls, json_string):
        """Read a JSON string and deserialize a DataFrameLineage object. This is the recommended method to use."""
        return cls.from_json_object(json.loads(json_string))