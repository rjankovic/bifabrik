from bifabrik.src.DataSource import DataSource
from bifabrik.dst.TableDestination import TableDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
#from dst import
        
class DataLoader:
    def __init__(self, spark: SparkSession, source: DataSource):
        self._spark = spark
        self._source = source
        self._stage = "src"
    
    def toTable(targetTableName: str) -> TableDestination:
        self._stage = "dst"
        srcDf = self._source.load()
        dst = TableDestination(self._spar, srcDf, targetTableName)
        return dst
    

    
