from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class JsonConfiguration(Configuration):
    """Configuration related to loading CSV files
    """
    def __init__(self):
        self._explicitProps = {}
        self.__primitivesAsString = None
        self.__prefersDecimal = None
        self.__allowComments = None
        self.__allowUnquotedFieldNames = None
        self.__allowSingleQuotes = None
        self.__allowNumericLeadingZero = None
        self.__allowBackslashEscapingAnyCharacter = None
        self.__jsonCorruptRecordsMode = None
        self.__jsonDateFormat = None
        self.__timestampFormat = None
        self.__multiLine = None
        self.__allowUnquotedControlChars = None
        self.__lineSep = None
        self.__samplingRatio = None

    @CfgProperty
    def primitivesAsString(self) -> str:
        """From spark:
        infers all primitive values as a string type. If None is set, it uses the default value, false.
        """
        return self.__primitivesAsString
    @primitivesAsString.setter(key='primitivesAsString')
    def primitivesAsString(self, val):
        self.__primitivesAsString = val

    @CfgProperty
    def prefersDecimal(self) -> str:
        """From spark:
        infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles. If None is set, it uses the default value, false.
        """
        return self.__prefersDecimal
    @prefersDecimal.setter(key='prefersDecimal')
    def prefersDecimal(self, val):
        self.__prefersDecimal = val

    @CfgProperty
    def allowComments(self) -> str:
        """From spark:
        ignores Java/C++ style comment in JSON records. If None is set, it uses the default value, false.
        """
        return self.__allowComments
    @allowComments.setter(key='allowComments')
    def allowComments(self, val):
        self.__allowComments = val

    @CfgProperty
    def allowUnquotedFieldNames(self) -> str:
        """From spark:
        allows unquoted JSON field names. If None is set, it uses the default value, false.
        """
        return self.__allowUnquotedFieldNames
    @allowUnquotedFieldNames.setter(key='allowUnquotedFieldNames')
    def allowUnquotedFieldNames(self, val):
        self.__allowUnquotedFieldNames = val

    @CfgProperty
    def allowSingleQuotes(self) -> str:
        """From spark:
        allows single quotes in addition to double quotes. If None is set, it uses the default value, true.
        """
        return self.__allowSingleQuotes
    @allowSingleQuotes.setter(key='allowSingleQuotes')
    def allowSingleQuotes(self, val):
        self.__allowSingleQuotes = val

    @CfgProperty
    def allowNumericLeadingZero(self) -> str:
        """From spark:
        allows leading zeros in numbers (e.g. 00012). If None is set, it uses the default value, false.
        """
        return self.__allowNumericLeadingZero
    @allowNumericLeadingZero.setter(key='allowNumericLeadingZero')
    def allowNumericLeadingZero(self, val):
        self.__allowNumericLeadingZero = val

    @CfgProperty
    def allowBackslashEscapingAnyCharacter(self) -> str:
        """From spark:
        allows accepting quoting of all character using backslash quoting mechanism. If None is set, it uses the default value, false.
        """
        return self.__allowBackslashEscapingAnyCharacter
    @allowBackslashEscapingAnyCharacter.setter(key='allowBackslashEscapingAnyCharacter')
    def allowBackslashEscapingAnyCharacter(self, val):
        self.__allowBackslashEscapingAnyCharacter = val

    @CfgProperty
    def jsonCorruptRecordsMode(self) -> str:
        """From spark:
        allows a mode for dealing with corrupt records during parsing. If None is
set, it uses the default value, PERMISSIVE.

PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.

DROPMALFORMED: ignores the whole corrupted records.

FAILFAST: throws an exception when it meets corrupted records.
        """
        return self.__jsonCorruptRecordsMode
    @jsonCorruptRecordsMode.setter(key='jsonCorruptRecordsMode')
    def jsonCorruptRecordsMode(self, val):
        self.__jsonCorruptRecordsMode = val

    @CfgProperty
    def jsonDateFormat(self) -> str:
        """From spark:
        sets the string that indicates a date format. Custom date formats follow the formats at datetime pattern. # noqa This applies to date type. If None is set, it uses the default value, yyyy-MM-dd.
        """
        return self.__jsonDateFormat
    @jsonDateFormat.setter(key='jsonDateFormat')
    def jsonDateFormat(self, val):
        self.__jsonDateFormat = val

    @CfgProperty
    def timestampFormat(self) -> str:
        """From spark:
        sets the string that indicates a timestamp format. Custom date formats follow the formats at datetime pattern. # noqa This applies to timestamp type. If None is set, it uses the default value, yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX].
        """
        return self.__timestampFormat
    @timestampFormat.setter(key='timestampFormat')
    def timestampFormat(self, val):
        self.__timestampFormat = val

    @CfgProperty
    def multiLine(self) -> str:
        """From spark:
        parse one record, which may span multiple lines, per file. If None is set, it uses the default value, false.
        """
        return self.__multiLine
    @multiLine.setter(key='multiLine')
    def multiLine(self, val):
        self.__multiLine = val

    @CfgProperty
    def allowUnquotedControlChars(self) -> str:
        """From spark:
        allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.
        """
        return self.__allowUnquotedControlChars
    @allowUnquotedControlChars.setter(key='allowUnquotedControlChars')
    def allowUnquotedControlChars(self, val):
        self.__allowUnquotedControlChars = val

    @CfgProperty
    def lineSep(self) -> str:
        """From spark:
        defines the line separator that should be used for parsing. If None is set, it covers all \r, \r\n and \n.
        """
        return self.__lineSep
    @lineSep.setter(key='lineSep')
    def lineSep(self, val):
        self.__lineSep = val

    @CfgProperty
    def samplingRatio(self) -> str:
        """From spark:
        defines fraction of input JSON objects used for schema inferring. If None is set, it uses the default value, 1.0.
        """
        return self.__samplingRatio
    @samplingRatio.setter(key='samplingRatio')
    def samplingRatio(self, val):
        self.__samplingRatio = val