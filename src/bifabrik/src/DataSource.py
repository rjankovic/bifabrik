#from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.dst.LakehouseTableDestination import LakehouseTableDestination
from bifabrik.dst.WarehouseTableDestination import WarehouseTableDestination
from bifabrik.dst.SparkDfDestination import SparkDfDestination
from bifabrik.dst.PandasDfDestination import PandasDfDestination
from bifabrik.tsf.DataTransformation import DataTransformation
from bifabrik.tsf import SparkDfTransformation
from bifabrik.tsf import PandasDfTransformation
from bifabrik.base.Task import Task

class DataSource(Task):
    """Generic data source. 
    Contains methods to initiate a DataDestination after you are done setting the source.
    bif.sourceInit.sourceOption1(A).srcOpt2(B).toTable("Tab").destinationOption(X)....save()
    """
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        

    # def toDf(self) -> DataFrame:
    #     """Converts the data to a spark dataframe
    #     """
    #     pass

    def toTable(self, targetTableName: str) -> LakehouseTableDestination:
        """Sets the lakehouse destination table name
        """
        return self.toLakehouseTable(targetTableName)
    
    def toLakehouseTable(self, targetTableName: str) -> LakehouseTableDestination:
        """Sets the lakehouse destination table name
        """
        dst = LakehouseTableDestination(self._pipeline, targetTableName)
        return dst
    
    def toWarehouseTable(self, targetTableName: str, targetSchemaName: str = 'dbo') -> WarehouseTableDestination:
        """Sets the warehouse destination table name
        """
        dst = WarehouseTableDestination(self._pipeline, targetTableName, targetSchemaName)
        return dst
    
    def toSparkDf(self) -> SparkDfDestination:
        """Sets the destination as a Spark DF (for further processing)
        """
        dst = SparkDfDestination(self._pipeline)
        return dst
    
    def toPandasDf(self) -> PandasDfDestination:
        """Sets the destination as a Pandas DF (for further processing)
        """
        dst = PandasDfDestination(self._pipeline)
        return dst
    
    def transformSparkDf(self, func) -> SparkDfTransformation.SparkDfTransformation:
        """Applies the givet function to transform the data as a Spark DF
        """
        tsf = SparkDfTransformation.SparkDfTransformation(self._pipeline, func)
        return tsf
    
    def transformPandasDf(self, func) -> PandasDfTransformation.PandasDfTransformation:
        """Applies the givet function to transform the data as a Pandas DF
        """
        tsf = PandasDfTransformation.PandasDfTransformation(self._pipeline, func)
        return tsf