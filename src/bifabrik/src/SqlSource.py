from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class SqlSource(DataSource):
    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._query = ""
    
    def query(self, query: str):
        self._query = query
        return self
    
    def toDf(self) -> DataFrame:
        df = self._spark.sql(self._query)
        return df