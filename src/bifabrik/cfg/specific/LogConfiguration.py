from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class LogConfiguration(Configuration):
    """Configuration related to loading CSV files
    """
    def __init__(self):
        self._explicitProps = {}
        self.__loggingEnabled = True
        self.__logLakehouse = None
        self.__loggingLevel = 'info'
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
        self.__loggingEnabled = val
        self.__modified = True

    @CfgProperty
    def logLakehouse(self) -> str:
        """The lakehouse to which logs will be saved
        """
        return self.__logLakehouse
    @logLakehouse.setter(key='logLakehouse')
    def logLakehouse(self, val):
        self.__logLakehouse = val
        self.__modified = True

    @CfgProperty
    def loggingLevel(self) -> str:
        """debug / info / warning / error / critical
        """
        return self.__loggingLevel
    @loggingLevel.setter(key='loggingLevel')
    def loggingLevel(self, val):
        self.__loggingLevel = val
        self.__modified = True

    @CfgProperty
    def errorLogPath(self) -> str:
        """The file to which to save error logs
        """
        return self.__errorLogPath
    @errorLogPath.setter(key='errorLogPath')
    def errorLogPath(self, val):
        self.__errorLogPath = val
        self.__modified = True

    @CfgProperty
    def logPath(self) -> str:
        """The file to which to save logs
        """
        return self.__logPath
    @logPath.setter(key='logPath')
    def quotechar(self, val):
        self.__logPath = val
        self.__modified = True