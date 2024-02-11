from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.SourceStorageConfiguration import SourceStorageConfiguration

class DataSourceConfigurationBase(ConfigContainer):

    def __init__(self):
        self.__sourceStorage = SourceStorageConfiguration()
        super().__init__()
        

    @property
    def sourceStorage(self) -> SourceStorageConfiguration:
        return self.__sourceStorage