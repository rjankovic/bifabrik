from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class ExcelConfiguration(Configuration):
    """Configuration related to loading Excel files
    """
    def __init__(self):
        self._explicitProps = {}
        self.__sheetName = 0
        self.__header = 0
        self.__names = None
        self.__usecols = None
        self.__skiprows = None
        self.__nrows = None
        self.__thousands = None
        self.__decimal = '.'

    @CfgProperty
    def sheetName(self):
        """From pandas:
        Strings are used for sheet names. Integers are used in zero-indexed sheet positions (chart sheets do not count as a sheet position). Lists of strings/integers are used to request multiple sheets. Specify None to get all worksheets.
        Available cases:
        Defaults to 0: 1st sheet as a DataFrame
        1: 2nd sheet as a DataFrame
        "Sheet1": Load sheet with name “Sheet1”
        [0, 1, "Sheet5"]: Load first, second and sheet named “Sheet5” as a dict of DataFrame
        None: All worksheets

        default: 0
        """
        return self.__sheetName
    @sheetName.setter(key='sheetName')
    def sheetName(self, val):
        self.__sheetName = val

    @CfgProperty
    def header(self):
        """From pandas:
        Row (0-indexed) to use for the column labels of the parsed DataFrame. If a list of integers is passed those row positions will be combined into a MultiIndex. Use None if there is no header.
        
        default: 0
        """
        return self.__header
    @header.setter(key='header')
    def header(self, val):
        self.__header = val

    @CfgProperty
    def names(self):
        """From pandas:
        List of column names to use. If file contains no header row, then you should explicitly pass header=None.
        
        default: None
        """
        return self.__names
    @names.setter(key='names')
    def names(self, val):
        self.__names = val

    @CfgProperty
    def usecols(self):
        """From pandas:
        If None, then parse all columns.
        If str, then indicates comma separated list of Excel column letters and column ranges (e.g. “A:E” or “A,C,E:F”). Ranges are inclusive of both sides.
        If list of int, then indicates list of column numbers to be parsed (0-indexed).
        If list of string, then indicates list of column names to be parsed.
        If callable, then evaluate each column name against it and parse the column if the callable returns True.
        Returns a subset of the columns according to behavior above.

        default: None
        """
        return self.__usecols
    @usecols.setter(key='usecols')
    def usecols(self, val):
        self.__usecols = val

    @CfgProperty
    def skiprows(self):
        """From pandas:
        Line numbers to skip (0-indexed) or number of lines to skip (int) at the start of the file. If callable, the callable function will be evaluated against the row indices, returning True if the row should be skipped and False otherwise. An example of a valid callable argument would be lambda x: x in [0, 2].
        
        default: None
        """
        return self.__skiprows
    @skiprows.setter(key='skiprows')
    def skiprows(self, val):
        self.__skiprows = val

    @CfgProperty
    def nrows(self):
        """From pandas:
        Number of rows to parse.
        
        default: None
        """
        return self.__nrows
    @nrows.setter(key='nrows')
    def nrows(self, val):
        self.__nrows = val

    @CfgProperty
    def thousands(self):
        """From pandas:
        Thousands separator for parsing string columns to numeric. Note that this parameter is only necessary for columns stored as TEXT in Excel, any numeric columns will automatically be parsed, regardless of display format.
        
        default: None
        """
        return self.__thousands
    @thousands.setter(key='thousands')
    def thousands(self, val):
        self.__thousands = val

    @CfgProperty
    def decimal(self):
        """From pandas:
        Character to recognize as decimal point for parsing string columns to numeric. Note that this parameter is only necessary for columns stored as TEXT in Excel, any numeric columns will automatically be parsed, regardless of display format.(e.g. use ‘,’ for European data).
        
        default: '.'
        """
        return self.__decimal
    @decimal.setter(key='decimal')
    def decimal(self, val):
        self.__decimal = val