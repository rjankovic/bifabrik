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

    @CfgProperty
    def loggingEnabled(self) -> bool:
        """True / False whether logging is enabled
        """
        return self.__loggingEnabled
    @loggingEnabled.setter(key='loggingEnabled')
    def loggingEnabled(self, val):
        self.__loggingEnabled = val

    # @CfgProperty
    # def header(self) -> str:
    #     """From pandas:
    #     Row number(s) containing column labels and marking the start of the data (zero-indexed). Default behavior is to infer the column names: if no names are passed the behavior is identical to header=0 and column names are inferred from the first line of the file, if column names are passed explicitly to names then the behavior is identical to header=None. Explicitly pass header=0 to be able to replace existing names. The header can be a list of integers that specify row locations for a MultiIndex on the columns e.g. [0, 1, 3]. Intervening rows that are not specified will be skipped (e.g. 2 in this example is skipped). Note that this parameter ignores commented lines and empty lines if skip_blank_lines=True, so header=0 denotes the first line of data rather than the first line of the file.
    #     """
    #     return self.__header
    # @header.setter(key='header')
    # def header(self, val):
    #     self.__header = val

    # @CfgProperty
    # def thousands(self) -> str:
    #     """From pandas:
    #     Character acting as the thousands separator in numerical values.
    #     """
    #     return self.__thousands
    # @thousands.setter(key='thousands')
    # def thousands(self, val):
    #     self.__thousands = val

    # @CfgProperty
    # def decimal(self) -> str:
    #     """From pandas:
    #     Character to recognize as decimal point (e.g., use ‘,’ for European data).
    #     """
    #     return self.__decimal
    # @decimal.setter(key='decimal')
    # def decimal(self, val):
    #     self.__decimal = val

    # @CfgProperty
    # def quotechar(self) -> str:
    #     """From pandas:
    #     Character used to denote the start and end of a quoted item. Quoted items can include the delimiter and it will be ignored.
    #     """
    #     return self.__quotechar
    # @quotechar.setter(key='quotechar')
    # def quotechar(self, val):
    #     self.__quotechar = val

    # @CfgProperty
    # def quoting(self) -> int:
    #     """From pandas:
    #     {0 or csv.QUOTE_MINIMAL, 1 or csv.QUOTE_ALL, 2 or csv.QUOTE_NONNUMERIC, 3 or csv.QUOTE_NONE}, default csv.QUOTE_MINIMAL
    #     Control field quoting behavior per csv.QUOTE_* constants. Default is csv.QUOTE_MINIMAL (i.e., 0) which implies that only fields containing special characters are quoted (e.g., characters defined in quotechar, delimiter, or lineterminator.
    #     """
    #     return self.__quoting
    # @quoting.setter(key='quoting')
    # def quoting(self, val):
    #     self.__quoting = val

    # @CfgProperty
    # def escapechar(self) -> int:
    #     """From pandas:
    #     Character used to escape other characters.
    #     """
    #     return self.__escapechar
    # @escapechar.setter(key='escapechar')
    # def escapechar(self, val):
    #     self.__escapechar = val