from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame as SparkDf

class SparkDfSource(DataSource):
    """Use spark dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> df = spark.read.format("csv").option("header","true").load("Files/CsvFiles/annual-enterprise-survey-2021.csv")
    >>> bif.fromSparkDf(df).toTable('Table1').run()
    """

    def __init__(self, parentPipeline, df: SparkDf):
        if not isinstance(df, SparkDf):
            raise Exception('The source is not a spark dataframe')
        
        super().__init__(parentPipeline)
        self.result = df
        self._completed = True
        
    
    def __str__(self):
        return f'Spark DF source: {self._result}'
    
    def execute(self, input) -> SparkDf:
        return self._result