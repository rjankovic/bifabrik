from bifabrik.tsf.DataTransformation import DataTransformation
from bifabrik.cfg.ValidationTransformationConfiguration import ValidationTransformationConfiguration
import bifabrik.utils.log as lg
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame as SparkDf

class ValidationTransformation(DataTransformation, ValidationTransformationConfiguration):
    """Test the input data by checking the ValidationResult and ValidationMessage columns to determine if each row failed / passed the validation.
    The column names / values checked can be changed in configuration.
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> bif.fromSql('''
    >>> WITH values AS(
    >>> SELECT AnnualSurveyID
    >>> ,Variable_code, Value
    >>> ,CAST(REPLACE(Value, ',', '') AS DOUBLE) ValueDouble 
    >>> FROM AnnualSurveyFromDW1
    >>> )
    >>> SELECT v.*
    >>> ,IF(v.ValueDouble IS NULL, 'Error', 'OK') AS ValidationResult
    >>> ,IF(v.ValueDouble IS NULL, CONCAT('"', Value, '" cannot be converted to double'), 'OK') AS ValidationMessage
    >>> FROM values v
    >>> ''').validate('NumberParsingTest') \
    >>> .toTable('SurveyValuesParsed').run()
    """

    def __init__(self, parentPipeline, testName = 'Unnamed'):
        super().__init__(parentPipeline)
        ValidationTransformationConfiguration.__init__(self)
        self.validation.testName = testName
    
    def __str__(self):
        return f'Data validation \'{self.validation.testName}\''
    
    def execute(self, input) -> SparkDf:
        lgr = lg.getLogger()
        self.__logger = lgr
        self._error = None
        
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        config = mergedConfig.validation

        resultColumnName = config.resultColumnName
        messageColumnName = config.messageColumnName
        errorResultValue = config.errorResultValue
        warningResultValue = config.warningResultValue
        # okResultValue = config.okResultValue
        # fail / log / None
        onError = config.onError.lower()
        onWarning = config.onWarning.lower()
        self.__resultColumnName = resultColumnName
        self.__messageColumnName = messageColumnName

        maxErrors = config.maxErrors
        maxWarnings = config.maxWarnings

        if maxErrors < 1:
            raise Exception('Invalid configuration - maxErrors has to be a positive integer')
        if maxWarnings < 1:
            raise Exception('Invalid configuration - maxWarnings has to be a positive integer')

        if self.validation.testName == None:
            self.validation.testName = ''
        self.__testName = self.validation.testName

        # resultColumnName = 'ValidationResult'
        # messageColumnName = 'ValidationMessage'
        # errorResultValue = 'Error'
        # warningResultValue = 'Warning'
        # okResultValue = 'OK'
        # onError = 'fail'
        # onWarning = 'log'
        # testName = None
        
        if not (resultColumnName in input.columns):
            raise Exception(f'The input dataframe does not contain the result column `{resultColumnName}`')
        if not (messageColumnName in input.columns):
            raise Exception(f'The input dataframe does not contain the result column `{messageColumnName}`')
        
        errors = input.filter(col(resultColumnName) == errorResultValue).collect()
        warnings = input.filter(col(resultColumnName) == warningResultValue).collect()
        oks = input.filter(col(resultColumnName) != errorResultValue).filter(col(resultColumnName) != warningResultValue)

        if onError is None:
            onError = 'none'
        if onWarning is None:
            onWarning = 'none'
        
        # log
        if onError in ['fail', 'log']:
            remaining = maxErrors
            for error in errors:
                if remaining < 1:
                    break
                err = f'Test {self.__testName}: {error[self.__messageColumnName]}; {self.rowToStr(error)}'
                lgr.error(err)
                print(f'Error: {err}')
                remaining = remaining - 1
        
        if onWarning in ['fail', 'log']:
            remaining = maxWarnings
            for warning in warnings:
                if remaining < 1:
                    break
                wrn = f'Test {self.__testName}: {warning[self.__messageColumnName]}; {self.rowToStr(warning)}'
                lgr.warning(wrn)
                print(f'Warning: {wrn}')
                remaining = remaining - 1

        # fail if need be
        if onError == 'fail' and len(errors) > 0:
            error = errors[0]
            raise Exception(f'Test {self.__testName} failed: {error[self.__messageColumnName]}; {self.rowToStr(error)}')
        
        if onWarning == 'fail' and len(warnings) > 0:
            warning = warnings[0]
            raise Exception(f'Test {self.__testName} failed: {warning[self.__messageColumnName]}; {self.rowToStr(warning)}')
        
        self._result = input
        self._completed = True
        return self._result
    
    def rowToStr(self, row) -> str:
        d = row.asDict()
        r = ''
        for k in d:
            if k == self.__resultColumnName:
                continue
            if k == self.__messageColumnName:
                continue
            r += f'`{k}`: {d[k]}\t'
        return r