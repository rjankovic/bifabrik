from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.JdbcSourceConfiguration import JdbcSourceConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
from bifabrik.utils import log
import notebookutils.mssparkutils.credentials

class JdbcSource(DataSource, JdbcSourceConfiguration):
    """Jdbc data source
    supports configuration options from https://spark.apache.org/docs/3.5.2/sql-data-sources-jdbc.html
    
    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromJdbc('SELECT * FROM Orders').url(';').user(',').passwordKVSecretName('passwordSecret').toTable('FactOrderLine').run()
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        JdbcSourceConfiguration.__init__(self)
        #self.__processed_abfs_paths = []
        self.__mergedConfig = None

    def __str__(self):
        url = None
        if self.__mergedConfig is not None:
            if 'jdbcUrl' in self.__mergedConfig.jdbc._explicitProps:
                url = self.__mergedConfig.jdbc._explicitProps['jdbcUrl']
        return f'JDBC source: {url}'
    
    def execute(self, input):
        lgr = log.getLogger()
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__mergedConfig = mergedConfig

        # set spark options for all the configuration switches specified in the task's config
        readerBase = self._spark.read.format("jdbc")

        # get login and password from KV if provided
        loginKVSecretName = mergedConfig.security.loginKVSecretName
        passwordKVSecretName = mergedConfig.security.passwordKVSecretName
        if not (loginKVSecretName is None):
            username = notebookutils.mssparkutils.credentials.getSecret(mergedConfig.security.keyVaultUrl, loginKVSecretName)
            readerBase.option('user', username)
        if not (passwordKVSecretName is None):
            password = notebookutils.mssparkutils.credentials.getSecret(mergedConfig.security.keyVaultUrl, passwordKVSecretName)
            readerBase.option('password', password)

        # get JDBC-speific options in a loop
        # (if the jdbcUser option is specified, it overrides the user from loginKVSecretName)
        for key in mergedConfig.jdbc._explicitProps:
            sparkKey = key[4:]
            sparkKey = sparkKey[0].lower() + sparkKey[1:]
            
            lgr.info(f'Setting {sparkKey} to {mergedConfig.jdbc._explicitProps[key]}')
            #print(f'Setting {sparkKey} to {mergedConfig.jdbc._explicitProps[key]}')
            readerBase.option(sparkKey, mergedConfig.jdbc._explicitProps[key])

        df = readerBase.load()
        
        self._result = df
        self._completed = True

    def cleanup(self):
        pass