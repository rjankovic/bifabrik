from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class DestinationStorageConfiguration(Configuration):
    """Defines the data destination lakehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__destinationWorkspace = None
        self.__destinationLakehouse = None
        self.__destinationWarehouse = None

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

    @CfgProperty
    def destinationWarehouseConnectionString(self) -> str:
        """Connection string to the Fabric warehouse (like dxt....datawarehouse.fabric.microsoft.com)
        """
        return self.__destinationWarehouseConnectionString
    @destinationWarehouseConnectionString.setter(key='destinationWarehouseConnectionString')
    def destinationWarehouseConnectionString(self, val):
        self.__destinationWarehouseConnectionString = val

    @CfgProperty
    def destinationWarehouse(self) -> str:
        """Name of the warehouse to which pipeline destinations writes data
        """
        return self.__destinationWarehouse
    @destinationWarehouse.setter(key='destinationWarehouse')
    def destinationWarehouse(self, val):
        self.__destinationWarehouse = val