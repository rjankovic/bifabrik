from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class SecurityConfiguration(Configuration):
    """Credentials configuration, currently used in the SharePointListSource.
    (Credentials aren't actually stored here, this configuration only refers to KeyVault secrets.)
    """
    def __init__(self):
        self._explicitProps = {}
        self.__keyVaultUrl = None
        self.__loginKVSecretName = None
        self.__passwordKVSecretName = None

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

    