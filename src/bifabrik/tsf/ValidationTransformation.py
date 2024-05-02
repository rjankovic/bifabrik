from bifabrik.tsf.DataTransformation import DataTransformation
from bifabrik.cfg.ValidationTransformationConfiguration import ValidationTransformationConfiguration
import bifabrik.utils.log as lg
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame as SparkDf

class ValidationTransformation(DataTransformation, ValidationTransformationConfiguration):
    """Use spark dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> (
    >>> bif
    >>>     .fromCsv("Files/CsvFiles/annual-enterprise-survey-2021.csv")
    >>>     .transformSparkDf(lambda df: df.withColumn('NewColumn', lit('NewValue')))
    >>>     .toTable("Survey2")
    >>>     .run()
    >>> )
    >>> 
    """

    def __init__(self, parentPipeline, testName = 'Unnamed'):
        super().__init__(parentPipeline)
        self.testName(testName)
    
    def __str__(self):
        return f'Data validation \`{self.__testName}\''
    
    def execute(self, input) -> SparkDf:
        lgr = lg.getLogger()
        self.__logger = lgr
        self._error = None
        
        config = self._pipeline.configuration.validation

        resultColumnName = config.resultColumnName
        messageColumnName = config.messageColumnName
        errorResultValue = config.errorResultValue
        warningResultValue = config.warningResultValue
        # okResultValue = config.okResultValue
        # fail / log / None
        onError = config.onError.lower()
        onWarning = config.onWarning.lower()
        self.__testName = config.testName
        self.__resultColumnName = resultColumnName
        self.__messageColumnName = messageColumnName

        if self.__testName == None:
            self.__testName = ''

        # resultColumnName = 'ValidationResult'
        # messageColumnName = 'ValidationMessage'
        # errorResultValue = 'Error'
        # warningResultValue = 'Warning'
        # okResultValue = 'OK'
        # onError = 'fail'
        # onWarning = 'log'
        # testName = None
        
        if not input.columns.contains(resultColumnName):
            raise Exception(f'The input dataframe does not contain the result column `{resultColumnName}`')
        if not input.columns.contains(messageColumnName):
            raise Exception(f'The input dataframe does not contain the result column `{messageColumnName}`')
        
        errors = input.filter(col(resultColumnName) == errorResultValue).collect()
        warnings = input.filter(col(resultColumnName) == warningResultValue).collect()
        oks = input.filter(col(resultColumnName) != errorResultValue).filter(col(resultColumnName) != warningResultValue)

        # log
        if onError in ['fail', 'log']:
            for error in errors:
                lgr.error(f'Test {self.__testName}: {error[self.__messageColumnName]}; {self.rowToStr(error)}')
        if onWarning in ['fail', 'log']:
            for warning in warnings:
                lgr.warning(f'Test {self.__testName}: {warning[self.__messageColumnName]}; {self.rowToStr(warning)}')

        # fail if need be
        if onError == 'fail' and len(errors) > 0:
            error = errors[0]
            raise Exception(f'Test {self.__testName} failed: {error[self.__messageColumnName]}; {self.rowToStr(error)}')
        
        if onWarning == 'fail' and len(warnings) > 0:
            warning = warnings[0]
            raise Exception(f'Test {self.__testName} failed: {warning[self.__messageColumnName]}; {self.rowToStr(warning)}')

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