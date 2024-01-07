#from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.dst.TableDestination import TableDestination
from bifabrik.base.Task import Task

class DataSource(Task):
    """Generic data source. 
    Contains methods to initiate a DataDestination after you are done setting the source.
    bif.sourceInit.sourceOption1(A).srcOpt2(B).toTable("Tab").destinationOption(X)....save()
    """
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        

    def toDf(self) -> DataFrame:
        """Converts the data to a dataframe
        """
        pass

    def toTable(self, targetTableName: str) -> TableDestination:
        """Sets the destination table name (table in the current lakehouse)
        """
        dst = TableDestination(self._pipeline, targetTableName)
        return dst
