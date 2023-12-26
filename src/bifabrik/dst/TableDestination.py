from bifabrik.dst import DataDestination
import spark.sql.SparkSession
import pyspark.sql

class TableDestination(DataDestination):
    def __init__(self, spark: SparkSession, sourceDf: DataFrame, targetTableName: str):
        super().__init__(spark, sourceDf)
        self._targetTableName = targetTableName
    
    def save(self):
        self._sourceDf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + targetTableName)