from bifabrik.dst import DataDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class TableDestination(DataDestination):
    def __init__(self, spark: SparkSession, sourceDf: DataFrame, targetTableName: str):
        super().__init__(spark, sourceDf)
        self._targetTableName = targetTableName
    
    def save(self):
        self._sourceDf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + targetTableName)