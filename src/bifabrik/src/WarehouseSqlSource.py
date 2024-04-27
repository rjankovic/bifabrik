from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame
import bifabrik.utils.log as lg
import pyodbc
import pandas as pd
import notebookutils.mssparkutils.fs
import notebookutils.mssparkutils.credentials

class WarehouseSqlSource(DataSource):
    """SQL (SparkSQL) data source - executes a SparkSQL query

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromSql.query('SELECT OrderId, CustomerId  FROM LH1.FactOrderLine WHERE ProductId = 791057').toTable('TransformedOrders1').run()
    """

    def __init__(self, parentPipeline, query):
        super().__init__(parentPipeline)
        self.__query = query
        self.__config = None
        
    
    def __str__(self):
        return f'Warehouse SQL source: {self.__query}'
    
    def execute(self, input) -> DataFrame:
        lgr = lg.getLogger()
        self.__logger = lgr
        self._error = None
        
        config = self._pipeline.configuration
        self.__config = config
        
        # connect ODBC
        odbcServer = config.sourceStorage.sourceWarehouseConnectionString
        odbcDatabase = config.sourceStorage.sourceWarehouseName
        odbcTimeout = config.sourceStorage.sourceWarehouseConnectionTimeout
        principalClientId = config.security.servicePrincipalClientId
        principalClientSecret = notebookutils.mssparkutils.credentials.getSecret(config.security.keyVaultUrl, config.security.servicePrincipalClientSecretKVSecretName)
        constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};database={odbcDatabase};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        if odbcDatabase is None:
            constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        self.__odbcConnection = pyodbc.connect(constr)

        pd_df = pd.read_sql(self.__query, self.__odbcConnection)
        df = self._spark.createDataFrame(pd_df)
        
        self._result = df
        self._completed = True