from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class SourceStorageConfiguration(Configuration):
    """Defines the data source lakehouse or warehouse. By default refers to the default lakehouse of the notebook.
    """
    def __init__(self):
        self._explicitProps = {}
        self.__sourceWorkspace = None
        self.__sourceLakehouse = None
        self.__sourceWarehouseConnectionString = None
        self.__sourceWarehouseConnectionTimeout = 600
        self.__sourceWarehouseName = None

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

    @CfgProperty
    def sourceWarehouseConnectionString(self) -> str:
        """Connection string to the source Fabric warehouse (server name like dxt....datawarehouse.fabric.microsoft.com)
        """
        return self.__sourceWarehouseConnectionString
    @sourceWarehouseConnectionString.setter(key='sourceWarehouseConnectionString')
    def sourceWarehouseConnectionString(self, val):
        self.__sourceWarehouseConnectionString = val

    @CfgProperty
    def sourceWarehouseConnectionTimeout(self) -> str:
        """Warehouse ODBC connection timetout in seconds
        default 600
        """
        return self.__sourceWarehouseConnectionTimeout
    @sourceWarehouseConnectionTimeout.setter(key='sourceWarehouseConnectionTimeout')
    def sourceWarehouseConnectionTimeout(self, val):
        self.__sourceWarehouseConnectionTimeout = val

    @CfgProperty
    def sourceWarehouseName(self) -> str:
        """Name of the warehouse from which the pipelines read data
        """
        return self.__sourceWarehouseName
    @sourceWarehouseName.setter(key='sourceWarehouseName')
    def sourceWarehouseName(self, val):
        self.__sourceWarehouseName = val