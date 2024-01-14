from bifabrik.cfg.engine import Configuration
from bifabrik.cfg.engine import ConfigContainer
from bifabrik.cfg.specific import CsvConfiguration

class CsvConfiguration(ConfigContainer):
    def __init__(self):
        self.__csv = CsvConfiguration()

    @property
    def csv(self) -> CsvConfiguration:
        return self.__csv
    