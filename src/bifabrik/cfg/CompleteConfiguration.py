from bifabrik.cfg.engine import ConfigContainer
from bifabrik.cfg.specific import CsvConfiguration

class CompleteConfiguration(ConfigContainer):

    def __init__(self):
        self.__csv = CsvConfiguration()
        super().__init__()
        

    @property
    def csv(self) -> CsvConfiguration:
        return self.__csv