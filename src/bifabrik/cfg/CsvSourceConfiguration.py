from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.CsvConfiguration import CsvConfiguration
from bifabrik.cfg.specific.FileSourceConfiguration import FileSourceConfiguration

class CsvSourceConfiguration(ConfigContainer):

    def __init__(self):
        self.__csv = CsvConfiguration()
        self.__fileSource = CsvConfiguration()
        super().__init__()
        

    @property
    def csv(self) -> CsvConfiguration:
        return self.__csv
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        return self.__fileSource