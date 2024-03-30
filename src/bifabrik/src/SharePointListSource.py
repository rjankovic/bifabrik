from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.SharePointListSourceConfiguration import SharePointListSourceConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import log
from shareplum import Site, folder
from shareplum import Office365
from shareplum.site import Version
import notebookutils.mssparkutils.credentials

class SharePointListSource(DataSource, SharePointListSourceConfiguration):
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
    
    def __init__(self, parentPipeline, siteUrl: str, listName: str):
        super().__init__(parentPipeline)
        SharePointListSourceConfiguration.__init__(self)
        self.__siteUrl = siteUrl
        self.__listName = listName
        self.__mergedConfig = None

    def __str__(self):
        return f'SharePointListSource source: {self.__siteUrl}/Lists/{self.__listName}'
    
    def execute(self, input):
        lgr = log.getLogger()
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__mergedConfig = mergedConfig

        securityConfig = mergedConfig.security

# try:
#                 if tsk.completed == True:
#                     if tsk.error != None:
#                         raise Exception(tsk.error)
#                     prevResult = tsk.result
#                     continue
                
#                 if tsk.error != None:
#                     raise Exception(tsk.error)
                
#                 lgr.info(f'Executing {tsk}')
#                 tsk.execute(prevResult)
#                 prevResult = tsk.result
#             except Exception as e:
#                 lgr.exception(e)
#                 raise e

        kvLogin = None
        kvPwd = None
        try:
            kvLogin = notebookutils.mssparkutils.credentials.getSecret(securityConfig.keyVaultUrl, securityConfig.loginKVSecretName)
        except Exception as e:
            lgr.exception(e)
            raise Exception(f'Failed to fetch login secret \'{securityConfig.loginKVSecretName}\' from Key Vault \'{securityConfig.keyVaultUrl}\', see error log for details')
        try:
            kvPwd = notebookutils.mssparkutils.credentials.getSecret(securityConfig.keyVaultUrl, securityConfig.passwordKVSecretName)
        except Exception as e:
            lgr.exception(e)
            raise Exception(f'Failed to fetch password secret \'{securityConfig.passwordKVSecretName}\' from Key Vault \'{securityConfig.keyVaultUrl}\', see error log for details')
        
        ixSites = self.__siteUrl.index('/sites/')
        sharePointServerUrl = self.__siteUrl[:ixSites]

        authcookie = Office365(sharePointServerUrl, username=kvLogin, password=kvPwd).GetCookies()
        site = Site(self.__siteUrl, version=Version.v365, authcookie=authcookie)
        sp_list = site.List(self.__listName)
        sp_data = sp_list.GetListItems()
        sp_df = self._spark.createDataFrame(sp_data)
        
        self._result = sp_df
        self._completed = True