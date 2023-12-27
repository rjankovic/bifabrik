from bifabrik.DataLoader import DataLoader
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

def fromPath(spark: SparkSession, filePath: str) -> DataLoader:
    pass

def fromPattern(spark: SparkSession, pattern: str) -> DataLoader:
    pass
