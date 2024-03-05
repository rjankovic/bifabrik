# from pyspark.sql.session import SparkSession
# from pyspark.sql.dataframe import DataFrame
from bifabrik.base.Task import Task

class DataDestination(Task):
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)

    # def __init__(self, dataLoader, sourceDf: DataFrame):
    #     self._sourceDf = sourceDf
    #     self._loader = dataLoader
    #     dataLoader.destination = self
    #     self._spark = self._loader.spark

    def save(self) -> None:
        """Save the data to the destination. This is the "run" method at the end of the chain. 
        Returns the result of the last task, if any.
        """
        self._pipeline.execute()

    def run(self) -> any:
        """Save the data to the destination. This is the "run" method at the end of the chain. 
        Returns the result of the last task, if any.
        """
        return self._pipeline.execute()
