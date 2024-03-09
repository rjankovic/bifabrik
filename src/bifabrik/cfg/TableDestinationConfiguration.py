from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.DataDestinationConfigurationBase import DataDestinationConfigurationBase
from bifabrik.cfg.specific.TableConfiguration import TableConfiguration

class TableDestinationConfiguration(DataDestinationConfigurationBase):

    def __init__(self):
        self.__destinationTable = TableConfiguration()
        super().__init__()

    @property
    def destinationTable(self) -> TableConfiguration:
        "Destination table settings (increment method, identity column, etc.)"
        return self.__destinationTable