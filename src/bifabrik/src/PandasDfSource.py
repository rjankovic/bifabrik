from bifabrik.src.DataSource import DataSource
from pandas.core.frame import DataFrame as PandasDf
from pyspark.sql.dataframe import DataFrame as SparkDf

class PandasDfSource(DataSource):
    """Use pandas dataframe as source
    
    Examples
    --------
    
    >>> import bifabrik as bif
    >>>
    >>> df = pd.read_csv(...)
    >>> bif.fromPandasDf(df).toTable('Table1').run()
    """

    def __init__(self, parentPipeline, df: PandasDf):
        if not isinstance(df, PandasDf):
            raise Exception('The source is not a pandas dataframe')
        
        super().__init__(parentPipeline)
        self.__pandasDf = df
    
    def __str__(self):
        return f'Pandas DF source: {self.__pandasDf}'
    
    def execute(self, input) -> SparkDf:
        self._result = self._spark.createDataFrame(self.__pandasDf)
        self._completed = True
        return self._result