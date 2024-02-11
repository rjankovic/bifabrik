from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class SourceStorageConfiguration(Configuration):
    """Defines the data source lakehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__sourceWorkspaceName = None
        self.__sourceLakehouseName = None

    @CfgProperty
    def sourceWorkspaceName(self) -> str:
        """The workspace from which pipeline sources load data
        """
        return self.__sourceWorkspaceName
    @sourceWorkspaceName.setter(key='sourceWorkspaceName')
    def sourceWorkspaceName(self, val):
        self.__sourceWorkspaceName = val

    @CfgProperty
    def sourceLakehouseName(self) -> str:
        """The lakehouse from which pipeline sources load data
        """
        return self.__sourceLakehouseName
    @sourceLakehouseName.setter(key='sourceLakehouseName')
    def sourceLakehouseName(self, val):
        self.__sourceLakehouseName = val