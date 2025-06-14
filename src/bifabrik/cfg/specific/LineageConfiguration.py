from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class LineageConfiguration(Configuration):
    """Configuration related to saving data lineage information about dataframes saved to the lakehouse as tables
    """
    def __init__(self):
        super().__init__()
        self.__lineageEnabled = False
        self.__lineageFolder = 'Files/bifabrik_lineage'
        
    @CfgProperty
    def lineageEnabled(self) -> bool:
        """Whether to save lineage information about tables saved to the lakehouse
        default False
        """
        return self.__lineageEnabled
    @lineageEnabled.setter(key='lineageEnabled')
    def lineageEnabled(self, val):
        self.__lineageEnabled = val

    @CfgProperty
    def lineageFolder(self) -> bool:
        """Folder where lineage information about tables saved to the lakehouse is stored
        default Files/bifabrik_lineage
        """
        return self.__lineageFolder
    @lineageFolder.setter(key='lineageFolder')
    def lineageFolder(self, val):
        self.__lineageFolder = val
