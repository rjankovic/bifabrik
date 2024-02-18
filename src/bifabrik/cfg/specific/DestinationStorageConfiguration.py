from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class DestinationStorageConfiguration(Configuration):
    """Defines the data destination lakehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__destinationWorkspace = None
        self.__destinationLakehouse = None

    @CfgProperty
    def destinationWorkspace(self) -> str:
        """The workspace to which pipeline destinations writes data - ID or name
        """
        return self.__destinationWorkspace
    @destinationWorkspace.setter(key='destinationWorkspace')
    def destinationWorkspace(self, val):
        self.__destinationWorkspace = val

    @CfgProperty
    def destinationLakehouse(self) -> str:
        """The lakehouse to which pipeline destinations writes data - ID or name
        """
        return self.__destinationLakehouse
    @destinationLakehouse.setter(key='destinationLakehouse')
    def destinationLakehouse(self, val):
        self.__destinationLakehouse = val