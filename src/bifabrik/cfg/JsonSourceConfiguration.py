from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.JsonConfiguration import JsonConfiguration
from bifabrik.cfg.specific.FileSourceConfiguration import FileSourceConfiguration
from bifabrik.cfg.DataSourceConfigurationBase import DataSourceConfigurationBase

class JsonSourceConfiguration(DataSourceConfigurationBase):

    def __init__(self):
        self.__json = JsonConfiguration()
        self.__fileSource = JsonConfiguration()
        super().__init__()
        

    @property
    def json(self) -> JsonConfiguration:
        return self.__json
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        return self.__fileSource