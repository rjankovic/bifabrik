from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.src.CsvSource import CsvSource
from bifabrik.DataLoader import DataLoader

# remove later
import glob2

class bifabrik:
    def __init__(self, spark: SparkSession):
        self._spark = spark
    
    def prepLoader(self):
        return DataLoader(self._spark)
    
    @property
    def fromCsv(self) -> DataLoader:
        ds = CsvSource(self.prepLoader())
        return ds
    
    
    
    
