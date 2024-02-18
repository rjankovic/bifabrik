from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.DestinationStorageConfiguration import DestinationStorageConfiguration

class DataDestinationConfigurationBase(ConfigContainer):

    def __init__(self):
        self.__destinationStorage = DestinationStorageConfiguration()
        super().__init__()
        

    @property
    def destinationStorage(self) -> DestinationStorageConfiguration:
        return self.__destinationStorage