from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class SqlSource(DataSource):
    """SQL (SparkSQL) data source
    """

    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._query = ""
    
    def query(self, query: str):
        """The source SQL query (SparkSQL) to be executed against the current lakehouse
        """
        self._query = query
        return self
    
    def toDf(self) -> DataFrame:
        df = self._spark.sql(self._query)
        return df