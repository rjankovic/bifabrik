from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.src.CsvSource import CsvSource
from bifabrik.src.JsonSource import JsonSource
from bifabrik.src.SqlSource import SqlSource
from bifabrik.DataLoader import DataLoader

class bifabrik:
    def __init__(self, spark: SparkSession):
        self._spark = spark
    
    def _prepLoader(self):
        return DataLoader(self._spark)
    
    @property
    def fromCsv(self) -> DataLoader:
        ds = CsvSource(self._prepLoader())
        return ds
    
    @property
    def fromJson(self) -> DataLoader:
        ds = JsonSource(self._prepLoader())
        return ds
    
    @property
    def fromSql(self) -> DataLoader:
        ds = SqlSource(self._prepLoader())
        return ds
