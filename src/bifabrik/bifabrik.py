import spark.sql.SparkSession
from src import CsvSource
import DataLoader

class bifabrik:
    def __init__(self, spark: SparkSession):
        self._spark = spark
    
    def csv(self) -> DataLoader:
        ds = CsvSource(self._spark)
        dl = DataLoader(self._spark, ds)
        return dl
    
    
