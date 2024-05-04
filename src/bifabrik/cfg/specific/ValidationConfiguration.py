from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class ValidationConfiguration(Configuration):
    """Configuration related to saving data to delta tables in a Fabric lakehouse or warehouse
    """
    def __init__(self):
        self._explicitProps = {}
        self.__resultColumnName = 'ValidationResult'
        self.__messageColumnName = 'ValidationMessage'
        self.__errorResultValue = 'Error'
        self.__warningResultValue = 'Warning'
        # fail / log / None
        self.__onError = 'fail'
        self.__onWarning = 'log'
        self.__testName = None
        self.__maxErrors = 10
        self.__maxWarnings = 10
        
    @CfgProperty
    def resultColumnName(self) -> str:
        """Column of the input dataframe that contains the validation result. The recognized values of the column are set in the configuration - see
        errorResultValue, warningResultValue, okResultValue

        default 'ValidationResult'
        """
        return self.__resultColumnName
    @resultColumnName.setter(key='resultColumnName')
    def resultColumnName(self, val):
        self.__resultColumnName = val

    @CfgProperty
    def messageColumnName(self) -> str:
        """Column of the input dataframe that contains the validation message. This can explain the reason for validation errors. 
        It will be included in the log / exception thrown from errors / warnings.

        default 'ValidationMessage'
        """
        return self.__messageColumnName
    @messageColumnName.setter(key='messageColumnName')
    def messageColumnName(self, val):
        self.__messageColumnName = val

    @CfgProperty
    def errorResultValue(self) -> str:
        """If the configured {resultColumnName} in the input data has the {errorResultValue}, the row will be considered a validation error.

        default 'Error'
        """
        return self.__errorResultValue
    @errorResultValue.setter(key='errorResultValue')
    def errorResultValue(self, val):
        self.__errorResultValue = val

    @CfgProperty
    def warningResultValue(self) -> str:
        """If the configured {resultColumnName} in the input data has the {warningResultValue}, the row will be considered a validation warning.

        default 'Warning'
        """
        return self.__warningResultValue
    @warningResultValue.setter(key='warningResultValue')
    def warningResultValue(self, val):
        self.__warningResultValue = val

    @CfgProperty
    def onError(self) -> str:
        """What to do when a validation error is encountered.
        Fail = throw exception and stop execution
        Log = log the warning to console and log file
        Nothing / None = ignore

        default 'Fail'
        """
        return self.__onError
    @onError.setter(key='onError')
    def onError(self, val):
        self.__onError = val

    @CfgProperty
    def onWarning(self) -> str:
        """What to do when a validation warning is encountered
        Fail = throw exception and stop execution
        Log = log the warning to console and log file
        Nothing / None = ignore

        default 'Log'
        """
        return self.__onWarning
    @onWarning.setter(key='onWarning')
    def onWarning(self, val):
        self.__onWarning = val

    @CfgProperty
    def testName(self) -> str:
        """Name of the current validation test - will be included in logs / exceptions
        """
        return self.__testName
    @testName.setter(key='testName')
    def testName(self, val):
        self.__testName = val

    @CfgProperty
    def maxErrors(self) -> int:
        """Maximum number of errors to be reported
        
        default 10
        """
        return self.__maxErrors
    @maxErrors.setter(key='maxErrors')
    def maxErrors(self, val):
        self.__maxErrors = val

    @CfgProperty
    def maxWarnings(self) -> int:
        """Maximum number of warnings to be reported

        default 10
        """
        return self.__maxWarnings
    @maxWarnings.setter(key='maxWarnings')
    def maxWarnings(self, val):
        self.__maxWarnings = val
