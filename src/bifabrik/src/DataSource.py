import spark.sql.SparkSession

class DataSource:
    def load(self, spark: SparkSession) -> pyspark.sql.DataFrame:
        self._spark = spark
