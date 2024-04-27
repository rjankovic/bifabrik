from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame

class SparkSqlSource(DataSource):
    """SQL (SparkSQL) data source - executes a SparkSQL query

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromSql.query('SELECT OrderId, CustomerId  FROM LH1.FactOrderLine WHERE ProductId = 791057').toTable('TransformedOrders1').run()
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        self._query = ""
    
    def __str__(self):
        return f'SQL source: {self._query}'
    
    def query(self, query: str):
        """The source SQL query (SparkSQL) to be executed against the current lakehouse
        """
        self._query = query
        return self
    
    def execute(self, input) -> DataFrame:
        df = self._spark.sql(self._query)
        self._result = df
        self._completed = True