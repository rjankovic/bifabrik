from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.LineageConfiguration import LineageConfiguration

class LineageConfigurationContainer(ConfigContainer):

    def __init__(self):
        self.__lineage = LineageConfiguration()
        super().__init__()
        

    @property
    def lineage(self) -> LineageConfiguration:
        """Data lineage configuration"""
        return self.__lineage