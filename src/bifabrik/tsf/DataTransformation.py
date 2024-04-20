#from pyspark.sql.session import SparkSession
from bifabrik.dst.LakehouseTableDestination import LakehouseTableDestination
from bifabrik.dst.WarehouseTableDestination import WarehouseTableDestination
from bifabrik.dst.SparkDfDestination import SparkDfDestination
from bifabrik.dst.PandasDfDestination import PandasDfDestination
from bifabrik.base.Task import Task
#from typing import Any, Self

class DataTransformation(Task):
    """Generic data source. 
    Contains methods to initiate a DataDestination after you are done setting the transformation.
    bif.sourceInit.sourceOption1(A).srcOpt2(B).transformation1(X).transformation2(Y).toTable("Tab").destinationOption(X)....run()
    """
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        
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
    
    def transformSparkDf(self, func):
        """Applies the givet function to transform the data as a Spark DF
        """
        from .SparkDfTransformation import SparkDfTransformation
        tsf = SparkDfTransformation(self._pipeline, func)
        return tsf
    
    def transformPandasDf(self, func):
        """Applies the givet function to transform the data as a Pandas DF
        """
        from .PandasDfTransformation import PandasDfTransformation
        tsf = PandasDfTransformation(self._pipeline, func)
        return tsf
