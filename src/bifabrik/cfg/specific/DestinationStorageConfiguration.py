from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class DestinationStorageConfiguration(Configuration):
    """Defines the data destination lakehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__destinationWorkspaceName = None
        self.__destinationLakehouseName = None

    @CfgProperty
    def destinationWorkspaceName(self) -> str:
        """The workspace from which pipeline destinations load data
        """
        return self.__destinationWorkspaceName
    @destinationWorkspaceName.setter(key='destinationWorkspaceName')
    def destinationWorkspaceName(self, val):
        self.__destinationWorkspaceName = val

    @CfgProperty
    def destinationLakehouseName(self) -> str:
        """The lakehouse from which pipeline destinations load data
        """
        return self.__destinationLakehouseName
    @destinationLakehouseName.setter(key='destinationLakehouseName')
    def destinationLakehouseName(self, val):
        self.__destinationLakehouseName = val