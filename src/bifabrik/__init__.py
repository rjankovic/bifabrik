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
from bifabrik.src.ExcelSource import ExcelSource
from bifabrik.src.SharePointListSource import SharePointListSource
from bifabrik.src.JsonSource import JsonSource
from bifabrik.src.SparkSqlSource import SparkSqlSource
from bifabrik.src.WarehouseSqlSource import WarehouseSqlSource
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
    print('bifabrik warning: the notebook is not attached to a lakehouse - some features will not work correctly.')


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

def fromExcel(path: str = None) -> ExcelSource:
    """Load data from Excel
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> bif.fromExcel.path('ExcelData/factOrderLine.xlsx').sheetName('Sheet1').toTable('FactOrderLine').run()
    """
    ds = ExcelSource(__prepPipeline())
    ds.path(path)
    return ds


def fromSql(query: str = None) -> SparkSqlSource:
    """Load the result of a SparkSQL query to a table
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> bif.fromSql('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').run()
    """
    return fromSparkSql(query)

def fromSparkSql(query: str = None) -> SparkSqlSource:
    """Load the result of a SparkSQL query to a table
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> bif.fromSql('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').run()
    """
    ds = SparkSqlSource(__prepPipeline())
    ds.query(query)
    return ds

def fromWarehouseSql(query: str) -> WarehouseSqlSource:
    """Load the result of a TSQL query from a Fabric warehouse to a table
    Service principal authentication is used to connect to the warehouse. This needs to be configured as below
    
    Examples
    --------
    >>> import bifabrik as bif
    >>>
    >>> bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
    >>> bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
    >>> bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'
    >>> bif.config.sourceStorage.sourceWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'
    >>>
    >>> bif.fromWarehouseSql('''
    >>> SELECT CountryOrRegion, Year, PublicHolidayCount FROM [DW1].[dbo].[HolidayCountsYearly]
    >>> ''').toTable('HolidayCountsYearlyFromDW').run()
    
    """
    ds = WarehouseSqlSource(__prepPipeline(), query)
    return ds

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

def fromSharePointList(siteUrl: str, listName: str) -> SharePointListSource:
    """Loads data from an Office365 SharePoint list. You will need to configure the credentials for SharePoint.
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>> 
    >>> bif.config.security.keyVaultUrl = 'https://kv-fabrik.vault.azure.net/'
    >>> bif.config.security.loginKVSecretName = 'SharePointLogin'
    >>> bif.config.security.passwordKVSecretName = 'SharePointPwd'
    >>> 
    >>> bif.fromSharePointList('https://fabrik.sharepoint.com/sites/BusinessIntelligence', 'CustomerList') \
    >>>     .toTable('Customers').run()
    """
    ds = SharePointListSource(__prepPipeline(), siteUrl, listName)
    return ds

def __getattr__(name):
    if name == 'config':
        return __configuration
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
