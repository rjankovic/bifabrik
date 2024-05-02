from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class SecurityConfiguration(Configuration):
    """Credentials configuration, currently used for connecting to Fabric warehouses and the SharePointListSource. (Passwords aren't actually stored here, this configuration only refers to KeyVault secrets.)
    """
    def __init__(self):
        self._explicitProps = {}
        self.__keyVaultUrl = None
        self.__loginKVSecretName = None
        self.__passwordKVSecretName = None
        self.__servicePrincipalClientId = None
        self.__servicePrincipalClientSecretKVSecretName = None

    @CfgProperty
    def keyVaultUrl(self) -> str:
        """Key Vault URL - e.g. 'https://kv-fabrik.vault.azure.net/'
        """
        return self.__keyVaultUrl
    @keyVaultUrl.setter(key='keyVaultUrl')
    def keyVaultUrl(self, val):
        self.__keyVaultUrl = val

    @CfgProperty
    def loginKVSecretName(self) -> str:
        """Secret in the Key Vault that contains the username
        """
        return self.__loginKVSecretName
    @loginKVSecretName.setter(key='loginKVSecretName')
    def loginKVSecretName(self, val):
        self.__loginKVSecretName = val
    
    @CfgProperty
    def passwordKVSecretName(self) -> str:
        """Secret in the Key Vault that contains the password
        """
        return self.__passwordKVSecretName
    @passwordKVSecretName.setter(key='passwordKVSecretName')
    def passwordKVSecretName(self, val):
        self.__passwordKVSecretName = val

    @CfgProperty
    def servicePrincipalClientId(self) -> str:
        """Service principal used for connecting to Fabric warehouses
        """
        return self.__servicePrincipalClientId
    @servicePrincipalClientId.setter(key='servicePrincipalClientId')
    def servicePrincipalClientId(self, val):
        self.__servicePrincipalClientId = val
    
    @CfgProperty
    def servicePrincipalClientSecretKVSecretName(self) -> str:
        """Secret in the Key Vault that contains the service principal's client secret
        """
        return self.__servicePrincipalClientSecretKVSecretName
    @servicePrincipalClientSecretKVSecretName.setter(key='servicePrincipalClientSecretKVSecretName')
    def servicePrincipalClientSecretKVSecretName(self, val):
        self.__servicePrincipalClientSecretKVSecretName = val

    