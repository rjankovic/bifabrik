from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class FileSourceConfiguration(Configuration):
    """Configuration related to loading data from files (aspects common for all file formats)
    """
    def __init__(self):
        self._explicitProps = {}
        self.__encoding = None
        self.__locale = None
        self.__moveFilesToArchive: bool = False
        self.__archiveFolder: str = None
        self.__archiveFilePattern: str = '{filename}_{timestamp}{extension}'


    @CfgProperty
    def encoding(self) -> str:
        """From pandas:
        How encoding errors are treated. https://docs.python.org/3/library/codecs.html#standard-encodings
        """
        return self.__encoding
    @encoding.setter(key='encoding')
    def encoding(self, val):
        self.__encoding = val

    @CfgProperty
    def locale(self) -> str:
        """From spark:
        sets a locale as language tag in IETF BCP 47 format. If None is set, it uses the default value, en-US. For instance, locale is used while parsing dates and timestamps.
        """
        return self.__locale
    @locale.setter(key='locale')
    def jsonLocale(self, val):
        self.__locale = val

    @CfgProperty
    def moveFilesToArchive(self) -> bool:
        """True / False whether to move loaded source files to archive after the pipeline finishes succesfully.
            The archiveFolder and archiveFilePattern need to be configured for this to work.
            Default False
        """
        return self.__moveFilesToArchive
    @moveFilesToArchive.setter(key='moveFilesToArchive')
    def moveFilesToArchive(self, val):
        self.__moveFilesToArchive = val
    
    @CfgProperty
    def archiveFolder(self) -> str:
        """The folder where to archive processed source files (in the source lakehouse).
        """
        return self.__archiveFolder
    @archiveFolder.setter(key='archiveFolder')
    def archiveFolder(self, val):
        self.__archiveFolder = val
    
    @CfgProperty
    def archiveFilePattern(self) -> str:
        """The file pattern for archiving processed source files. 
        Supported placeholders:
        {filename} : source file name without suffix
        {extension} : source file name extension, including the '.'
        {timestamp} : current timestamp as '%Y_%m_%d_%H_%M_%S_%f'

        Default '{filename}_{timestamp}{extension}'
        """
        return self.__archiveFilePattern
    @archiveFilePattern.setter(key='archiveFilePattern')
    def archiveFilePattern(self, val):
        self.__archiveFilePattern = val
