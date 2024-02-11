from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class MetadataStorageConfiguration(Configuration):
    """Defines the data metadata lakehouse. By default refers to the default lakehouse of the notebook.
    This is used for logs and metadata tables
    """
    def __init__(self):
        self._explicitProps = {}
        self.__metadataWorkspaceName = None
        self.__metadataLakehouseName = None

    @CfgProperty
    def metadataWorkspaceName(self) -> str:
        """The workspace where bifabrik stores metadata
        """
        return self.__metadataWorkspaceName
    @metadataWorkspaceName.setter(key='metadataWorkspaceName')
    def metadataWorkspaceName(self, val):
        self.__metadataWorkspaceName = val

    @CfgProperty
    def metadataLakehouseName(self) -> str:
        """The lakehouse where bifabrik stores metadata
        """
        return self.__metadataLakehouseName
    @metadataLakehouseName.setter(key='metadataLakehouseName')
    def metadataLakehouseName(self, val):
        self.__metadataLakehouseName = val