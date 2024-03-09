from bifabrik.tsf.DataTransformation import DataTransformation
from pyspark.sql.dataframe import DataFrame as SparkDf
from pandas.core.frame import DataFrame as PandasDf

class PandasDfTransformation(DataTransformation):
    """Use spark dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> df = spark.read.format("csv").option("header","true").load("Files/CsvFiles/annual-enterprise-survey-2021.csv")
    >>> bif.fromSparkDf(df).toTable('Table1').run()
    """

    def __init__(self, parentPipeline, func):
        super().__init__(parentPipeline)
        self.__func = func
    
    def __str__(self):
        return f'Pandas DF transformation: {self.__func}'
    
    def execute(self, input) -> SparkDf:
        pandasInput =  input.toPandas()
        pandasResult = self.__func(pandasInput)
        self._result = self._spark.createDataFrame(pandasResult)
        self._completed = True
        return self._result