from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.dst.TableDestination import TableDestination

class DataSource:
    """Generic data source. 
    Contains methods to initiate a DataDestination after you are done setting the source.
    bif.sourceInit.sourceOption1(A).srcOpt2(B).toTable("Tab").destinationOption(X)....save()
    """
    def __init__(self, dataLoader):
        self._loader = dataLoader
        dataLoader.source = self
        self._spark = self._loader.spark

    def toDf(self) -> DataFrame:
        """Converts the data to a dataframe
        """
        pass

    def toTable(self, targetTableName: str) -> TableDestination:
        """Sets the destination table name (table in the current lakehouse)
        """
        dst = TableDestination(self._loader, self.toDf(), targetTableName)
        return dst
