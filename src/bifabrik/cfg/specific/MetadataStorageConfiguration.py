from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class MetadataStorageConfiguration(Configuration):
    """Defines the data metadata lakehouse. By default refers to the default lakehouse of the notebook. This is used for logs and metadata tables
    """
    def __init__(self):
        self._explicitProps = {}
        self.__metadataWorkspaceName = None
        self.__metadataLakehouseName = None
        self.__tmslBackupPathPattern = 'Files/tmsl_backup/{workspacename}/{datasetname}/{datasetname}_{timestamp}.json'

    @CfgProperty
    def metadataWorkspaceName(self) -> str:
        """The workspace where bifabrik stores metadata
        """
        return self.__metadataWorkspaceName
    @metadataWorkspaceName.setter(key='metadataWorkspaceName')
    def metadataWorkspaceName(self, val):
        self.__metadataWorkspaceName = val

    @CfgProperty
    def metadataLakehouseName(self) -> str:
        """The lakehouse where bifabrik stores metadata
        """
        return self.__metadataLakehouseName
    @metadataLakehouseName.setter(key='metadataLakehouseName')
    def metadataLakehouseName(self, val):
        self.__metadataLakehouseName = val
    
    @CfgProperty
    def tmslBackupPathPattern(self) -> str:
        """The path pattern where to save backups of semantic model definitions (in TMSL format). 
        
        Supported placeholders:
            {workspacename} : the source workspace name
            {datasetname}   : the source dataset name
            {timestamp}     : backup timestamp as %Y_%m_%d_%H_%M_%S_%f
        
        default: 'Files/tmsl_backup/{workspacename}/{datasetname}/{datasetname}_{timestamp}.json'

        The metadataLakehouseName / metadataWorkspaceName should also be configured. If not, the file will be saved to the default lakehouse attached to the notebook.

        """
        return self.__tmslBackupPathPattern
    @tmslBackupPathPattern.setter(key='tmslBackupPathPattern')
    def tmslBackupPathPattern(self, val):
        self.__tmslBackupPathPattern = val
    
