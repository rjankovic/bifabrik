from src import DataSource
from dst import TableDestination
import spark.sql.SparkSession
#from dst import
        
class DataLoader:
    def __init__(self, spark: SparkSession, source: DataSource):
        self._spark = spark
        self._source = source
        self._stage = "src"
    
    def toTable(targetTableName: str) -> DataDestination:
        TableDestination
        self._stage = "dst"
        srcDf = self._source.load()
        dst = TableDestination(self._spar, srcDf, targetTableName)
        return dst
    

    
