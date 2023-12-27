from bifabrik.dst.DataDestination import DataDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.DataLoader import DataLoader

class TableDestination(DataDestination):
    def __init__(self, dataLoader: DataLoader, sourceDf: DataFrame, targetTableName: str):
        super().__init__(dataLoader, sourceDf)
        self._targetTableName = targetTableName
    
    def save(self):
        self._sourceDf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + self._targetTableName)