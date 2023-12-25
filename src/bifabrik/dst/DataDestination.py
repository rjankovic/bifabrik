import pyspark.sql
import spark.sql.SparkSession

class DataDestination:
    __init__(self, spark: SparkSession, sourceDf: DataFrame):
        self._spark = spark
        self._sourceDf = sourceDf

    def save(self) -> None:
        pass
