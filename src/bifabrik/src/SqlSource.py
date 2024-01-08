from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame

class SqlSource(DataSource):
    """SQL (SparkSQL) data source
    """

    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        self._query = ""
    
    def query(self, query: str):
        """The source SQL query (SparkSQL) to be executed against the current lakehouse
        """
        self._query = query
        return self
    
    def execute(self, input) -> DataFrame:
        df = self._spark.sql(self._query)
        self._result = df
        self._completed = True