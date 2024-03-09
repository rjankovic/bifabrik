from bifabrik.tsf.DataTransformation import DataTransformation
from pyspark.sql.dataframe import DataFrame as SparkDf

class SparkDfTransformation(DataTransformation):
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

    def __init__(self, parentPipeline, func):
        super().__init__(parentPipeline)
        self.__func = func
    
    def __str__(self):
        return f'Spark DF transformation: {self.__func}'
    
    def execute(self, input) -> SparkDf:
        self._result = self.__func(input)
        self._completed = True
        return self._result