"""The starting point for using the library.

    For more info see https://rjankovic.github.io/bifabrik/
    
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

from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.src.CsvSource import CsvSource
from bifabrik.src.JsonSource import JsonSource
from bifabrik.src.SqlSource import SqlSource
from bifabrik.cfg.CompleteConfiguration import CompleteConfiguration
import bifabrik.utils.log as log
import pyspark.sql.session as pss


from bifabrik.base.Pipeline import Pipeline
#from bifabrik.base.Task import Task

__spark = pss.SparkSession.builder.getOrCreate()
__configuration = CompleteConfiguration()
        #self.__configuration.log.loggingEnabled = False
    
def __prepPipeline() -> Pipeline:
    global __configuration
    global __spark
    log.configureLogger(__configuration.log)
    return Pipeline(__spark, __configuration)


def fromCsv(path: str = None) -> CsvSource:
    """Load data from CSV
    
    Examples
    --------
    
    >>> from bifabrik import bifabrik
    >>> bif = bifabrik(spark)
    >>>
    >>> bif.fromCsv('orders*.csv').toTable('Orders').run()
    """
    ds = CsvSource(__prepPipeline())
    ds.path(path)
    return ds


def fromJson(path: str = None) -> JsonSource:
    """Load data from JSON
    
    Examples
    --------
    
    >>> from bifabrik import bifabrik
    >>> bif = bifabrik(spark)
    >>>
    >>> bif.fromJson('invoices.json').toTable('Invoices').run()
    """
    ds = JsonSource(__prepPipeline())
    ds.path(path)
    return ds


def fromSql(query: str = None) -> SqlSource:
    """Load the result of a SQL query to a table
    
    Examples
    --------
    
    >>> from bifabrik import bifabrik
    >>> bif = bifabrik(spark)
    >>>
    >>> bif.fromSql('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').run()
    """
    ds = SqlSource(__prepPipeline())
    ds.query(query)
    return ds

def __getattr__(name):
    if name == 'cfg':
        return __configuration
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# @property
# def cfg(self) -> CompleteConfiguration:
#     return self.__configuration

# def loadConfigFromFile(self, path: str):
#     self.cfg.loadFromFile(path)
#     if self.cfg.log.loggingEnabled:
#         log.configureLogger(self.cfg.log)


