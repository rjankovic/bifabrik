from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.CsvConfiguration import CsvConfiguration
from bifabrik.cfg.specific.FileSourceConfiguration import FileSourceConfiguration

class CompleteConfiguration(ConfigContainer):
    """
    Fluent API for configuration
    
    You can set configuration directly through setter methods like .delimiter(';')[.further options...]
    or
    .option(name, value = None) gets / sets a named config property as in
    .option('delimiter', ';') #setter
    .option('delimiter') #getter
    """
    def __init__(self):
        self.__csv = CsvConfiguration()
        self.__fileSource = FileSourceConfiguration()
        super().__init__()
        

    @property
    def csv(self) -> CsvConfiguration:
        return self.__csv
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        return self.__fileSource