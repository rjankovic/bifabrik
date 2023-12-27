from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

class DataSource:
    def load(self, spark: SparkSession) -> DataFrame:
        self._spark = spark
