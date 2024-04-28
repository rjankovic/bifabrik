from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame
import bifabrik.utils.log as lg
import pyodbc
import pandas as pd
import notebookutils.mssparkutils.fs
import notebookutils.mssparkutils.credentials

class WarehouseSqlSource(DataSource):
    """TSQL data source loading data from a Fabric data warehouse
    Service principal authentication is used to connect to the warehouse. This needs to be configured as below

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
    > bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
    > bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'
    > bif.config.sourceStorage.sourceWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'
    >
    > bif.fromWarehouseSql('''
    > SELECT CountryOrRegion, Year, PublicHolidayCount FROM [DW1].[dbo].[HolidayCountsYearly]
    > ''').toTable('HolidayCountsYearlyFromDW').run()
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

        if odbcServer is None:
            raise Exception('Cannot connect to warehouse - sourceStorage.sourceWarehouseConnectionString not configured (copy this from the properties of your warehouse)')
        if odbcDatabase is None:
            raise Exception('Cannot connect to warehouse - sourceStorage.sourceWarehouseName not configured')
        if principalClientId is None:
            raise Exception('Cannot connect to warehouse - security.servicePrincipalClientId not configured (service principal authentication is used)')
        if config.security.keyVaultUrl is None:
            raise Exception('Cannot connect to warehouse - security.keyVaultUrl not configured (service principal authentication is used and the client secret needs to be stored in key vault)')
        if config.security.servicePrincipalClientSecretKVSecretName is None:
            raise Exception('Cannot connect to warehouse - security.servicePrincipalClientSecretKVSecretName not configured (service principal authentication is used and the client secret needs to be stored in key vault)')
        
        principalClientSecret = notebookutils.mssparkutils.credentials.getSecret(config.security.keyVaultUrl, config.security.servicePrincipalClientSecretKVSecretName)
        constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};database={odbcDatabase};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        if odbcDatabase is None:
            constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        self.__odbcConnection = pyodbc.connect(constr)

        pd_df = pd.read_sql(self.__query, self.__odbcConnection)
        df = self._spark.createDataFrame(pd_df)
        
        self._result = df

        self.__odbcConnection.close()
        self._completed = True