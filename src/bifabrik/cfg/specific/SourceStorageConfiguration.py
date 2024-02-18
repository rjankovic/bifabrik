from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class SourceStorageConfiguration(Configuration):
    """Defines the data source lakehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__sourceWorkspace = None
        self.__sourceLakehouse = None

    @CfgProperty
    def sourceWorkspace(self) -> str:
        """The workspace from which pipeline sources load data - ID or name
        """
        return self.__sourceWorkspace
    @sourceWorkspace.setter(key='sourceWorkspace')
    def sourceWorkspace(self, val):
        self.__sourceWorkspace = val

    @CfgProperty
    def sourceLakehouse(self) -> str:
        """The lakehouse from which pipeline sources load data - ID or name
        """
        return self.__sourceLakehouse
    @sourceLakehouse.setter(key='sourceLakehouse')
    def sourceLakehouse(self, val):
        self.__sourceLakehouse = val