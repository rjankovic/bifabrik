from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class LogConfiguration(Configuration):
    """Configuration related to loading CSV files
    """
    def __init__(self):
        self._explicitProps = {}
        self.__loggingEnabled = True
        self.__logLakehouse = None
        self.__loggingLevel = 'INFO'
        self.__errorLogPath = 'Files/BifabrikErrorLog.log'
        self.__logPath = 'Files/BifabrikLog.log'
        self.__modified = False

    @CfgProperty
    def loggingEnabled(self) -> bool:
        """True / False whether logging is enabled
        """
        return self.__loggingEnabled
    @loggingEnabled.setter(key='loggingEnabled')
    def loggingEnabled(self, val):
        if self.__loggingEnabled != val:
            self.__modified = True
        self.__loggingEnabled = val
        

    # @CfgProperty
    # def logLakehouse(self) -> str:
    #     """The lakehouse to which logs will be saved
    #     """
    #     return self.__logLakehouse
    # @logLakehouse.setter(key='logLakehouse')
    # def logLakehouse(self, val):
    #     if self.__logLakehouse != val:
    #         self.__modified = True
    #     self.__logLakehouse = val

    @CfgProperty
    def loggingLevel(self) -> str:
        """DEBUG / INFO / WARNING / ERROR / CRITICAL
        """
        return self.__loggingLevel
    @loggingLevel.setter(key='loggingLevel')
    def loggingLevel(self, val):
        if self.__loggingLevel != val:
            self.__modified = True
        self.__loggingLevel = val

    @CfgProperty
    def errorLogPath(self) -> str:
        """The file to which to save error logs
        """
        return self.__errorLogPath
    @errorLogPath.setter(key='errorLogPath')
    def errorLogPath(self, val):
        if self.__errorLogPath != val:
            self.__modified = True
        self.__errorLogPath = val
        
    @CfgProperty
    def logPath(self) -> str:
        """The file to which to save logs
        """
        return self.__logPath
    @logPath.setter(key='logPath')
    def logPath(self, val):
        if self.__logPath != val:
            self.__modified = True
        self.__logPath = val

    @property
    def modified(self) -> bool:
        return self.__modified
    @modified.setter
    def modified(self, val) -> bool:
        self.__modified = val