from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class DataDestination:
    def __init__(self, dataLoader, sourceDf: DataFrame):
        self._sourceDf = sourceDf
        self._loader = dataLoader
        dataLoader.destination = self
        self._spark = self._loader.spark

    def save(self) -> None:
        pass
