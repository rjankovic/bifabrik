from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pyspark.sql.dataframe import DataFrame as SparkDf
from bifabrik.utils import fsUtils
from bifabrik.utils import log

class SparkDfDestination(DataDestination):
    """Saves data to a spark dataframe.

    Examples
    --------
    > import bifabrik as bif
    >
    > df = bif.fromSql('SELECT * FROM SomeTable').toSparkDf().run()
    """
    def __init__(self, pipeline: Pipeline):
        super().__init__(pipeline)

    def __str__(self):
        return f'Spark DF destination'
    
    def execute(self, input: SparkDf) -> None:
        self._result = input
        self._error = None
        self._completed = True