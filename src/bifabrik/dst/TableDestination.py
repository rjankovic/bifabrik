from bifabrik.dst.DataDestination import DataDestination
from bifabrik.dst.base import Pipeline
from pyspark.sql.dataframe import DataFrame

class TableDestination(DataDestination):
    """Saves data to a lakehouse table.
    """
    def __init__(self, pipeline: Pipeline, targetTableName: str):
        super().__init__(pipeline)
        self._targetTableName = targetTableName
    
    def execute(self, input: DataFrame) -> None:
        input.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + self._targetTableName)