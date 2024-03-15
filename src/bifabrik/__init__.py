"""The starting point for using the library.

    For more info see https://rjankovic.github.io/bifabrik/
    
    To create an instance, pass the :class:`SparkSession` ("spark") from your Fabric notebook.
    You can then use various methods to load data to tables to the current lakehouse.
    The notebook has to have a default lakehouse selected - it references files from the Files/
    path of the lakehouse and tables from Tables/

    Examples
    --------
    Load a CSV file from Files/Folder/orders*.csv (all files matching the pattern) to a table called OrdersTable.

    >>> import bifabrik as bif
    >>>
    >>> bif.fromCsv.path('Folder/orders*.csv').toTable('OrdersTable').run()
    
    Load the results of a SQL query to a table

    >>> bif.fromSql("SELECT * FROM OrdersTable LIMIT 10").toTable('TenOrders').save()
"""

from bifabrik.src.CsvSource import CsvSource
from bifabrik.src.JsonSource import JsonSource
from bifabrik.src.SqlSource import SqlSource
from bifabrik.src.SparkDfSource import SparkDfSource
from bifabrik.src.PandasDfSource import PandasDfSource
from bifabrik.cfg.CompleteConfiguration import CompleteConfiguration
import bifabrik.utils.log as log
from bifabrik.base.Pipeline import Pipeline
import pyspark.sql.session as pss
from pyspark.sql.dataframe import DataFrame as SparkDf
from pandas.core.frame import DataFrame as PandasDf

# read version from installed package
from importlib.metadata import version
__version__ = version("bifabrik")
__loggername__ = "bifabrik_logger"

#import bifabrik
from bifabrik.utils.fsUtils import getDefaultLakehouseAbfsPath

if getDefaultLakehouseAbfsPath() is None:
    print('bifabrik warning: the notebook is not attached to a lakehouse - some features of bifabrik will not work correctly.')


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
    
    >>> import bifabrik as bif
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
    
    >>> import bifabrik as bif
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
    
    >>> import bifabrik as bif
    >>>
    >>> bif.fromSql('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').run()
    """
    ds = SqlSource(__prepPipeline())
    ds.query(query)
    return ds

def __getattr__(name):
    if name == 'config':
        return __configuration
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def fromSparkDf(df: SparkDf) -> SparkDfSource:
    """Use spark dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> df = spark.read.format("csv").option("header","true").load("Files/CsvFiles/annual-enterprise-survey-2021.csv")
    >>> bif.fromSparkDf(df).toTable('Table1').run()
    """
    ds = SparkDfSource(__prepPipeline(), df)
    return ds

def fromPandasDf(df: PandasDf) -> PandasDfSource:
    """Use pandas dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> df = pd.read_csv(...)
    >>> bif.fromPandasDf(df).toTable('Table1').run()
    """
    ds = PandasDfSource(__prepPipeline(), df)
    return ds