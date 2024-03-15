from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pandas.core.frame import DataFrame as PandasDf
from pyspark.sql.dataframe import DataFrame as SparkDf
from bifabrik.utils import fsUtils
from bifabrik.utils import log

class PandasDfDestination(DataDestination):
    """Saves data to a pandas dataframe.

    Examples
    --------
    > import bifabrik as bif
    >
    > pandas_df = bif.fromSql('SELECT * FROM SomeTable').toPandasDf().run()
    """
    def __init__(self, pipeline: Pipeline):
        super().__init__(pipeline)

    def __str__(self):
        return f'Pandas DF destination'
    
    def execute(self, input: SparkDf) -> None:
        self._result = input.toPandas()
        self._error = None
        self._completed = True