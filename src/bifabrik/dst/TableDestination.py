from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from pyspark.sql.dataframe import DataFrame

class TableDestination(DataDestination):
    """Saves data to a lakehouse table.

    Examples
    --------
    > from bifabrik import bifabrik
    > bif = bifabrik(spark)
    > bif.fromSql.query('SELECT * FROM SomeTable').toTable('DestinationTable1').run()
    """
    def __init__(self, pipeline: Pipeline, targetTableName: str):
        super().__init__(pipeline)
        self._targetTableName = targetTableName

    def __str__(self):
        return f'Table destination: {self._targetTableName}'
    
    def execute(self, input: DataFrame) -> None:
        input.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + self._targetTableName)