from bifabrik.dst.DataDestination import DataDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class TableDestination(DataDestination):
    """Saves data to a lakehouse table.
    """
    def __init__(self, dataLoader, sourceDf: DataFrame, targetTableName: str):
        super().__init__(dataLoader, sourceDf)
        self._targetTableName = targetTableName
    
    def save(self):
        """Save the data to a lakehouse table. This is the "commit" method at the end of the chain.
        """
        self._sourceDf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/" + self._targetTableName)