from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.DataLoader import DataLoader

class DataDestination:
    def __init__(self, dataLoader: DataLoader, sourceDf: DataFrame):
        self._sourceDf = sourceDf
        self._loader = dataLoader
        dataLoader.destination = self

    def save(self) -> None:
        pass
