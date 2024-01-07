from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.src.CsvSource import CsvSource
from bifabrik.src.JsonSource import JsonSource
from bifabrik.src.SqlSource import SqlSource
from bifabrik.src.bifabrik.base.Task import DataLoader

class bifabrik:
    """The starting point for using the library.

    To create an instance, pass the :class:`SparkSession` ("spark") from your Fabric notebook.
    You can then use various methods to load data to tables to the current lakehouse.
    The notebook has to have a default lakehouse selected - it references files from the Files/
    path of the lakehouse and tables from Tables/

    Examples
    --------
    Load a CSV file from Files/Folder/orders*.csv (all files matching the pattern) to a table called OrdersTable.

    >>> from bifabrik import bifabrik
    >>> bif = bifabrik(spark)
    >>>
    >>> bif.fromCsv.path('Folder/orders*.csv').toTable('OrdersTable').save()
    
    Load the results of a SQL query to a table

    >>> bif.fromSql.query("SELECT * FROM OrdersTable LIMIT 10").toTable('TenOrders').save()
    """
    def __init__(self, spark: SparkSession):
        self._spark = spark
    
    def _prepLoader(self):
        return DataLoader(self._spark)
    
    @property
    def fromCsv(self) -> CsvSource:
        """Load data from CSV
        
        Examples
        --------
        
        >>> from bifabrik import bifabrik
        >>> bif = bifabrik(spark)
        >>>
        >>> bif.fromCsv.path('orders*.csv').toTable('Orders').save()
        """
        ds = CsvSource(self._prepLoader())
        return ds
    
    @property
    def fromJson(self) -> JsonSource:
        """Load data from JSON
        
        Examples
        --------
        
        >>> from bifabrik import bifabrik
        >>> bif = bifabrik(spark)
        >>>
        >>> bif.fromJson.path('invoices.json').toTable('Invoices').save()
        """
        ds = JsonSource(self._prepLoader())
        return ds
    
    @property
    def fromSql(self) -> SqlSource:
        """Load the result of a SQL query to a table
        
        Examples
        --------
        
        >>> from bifabrik import bifabrik
        >>> bif = bifabrik(spark)
        >>>
        >>> bif.fromSql.query('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').save()
        """
        ds = SqlSource(self._prepLoader())
        return ds
