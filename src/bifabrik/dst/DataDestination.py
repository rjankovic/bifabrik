# from pyspark.sql.session import SparkSession
# from pyspark.sql.dataframe import DataFrame

class DataDestination:
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)

    # def __init__(self, dataLoader, sourceDf: DataFrame):
    #     self._sourceDf = sourceDf
    #     self._loader = dataLoader
    #     dataLoader.destination = self
    #     self._spark = self._loader.spark

    def save(self) -> None:
        """Save the data to the destination. This is the "run" method at the end of the chain.
        """
        self._pipeline.execute()
