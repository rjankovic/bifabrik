from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class DataDestination:
    def __init__(self, spark: SparkSession, sourceDf: DataFrame):
        self._spark = spark
        self._sourceDf = sourceDf

    def save(self) -> None:
        pass
