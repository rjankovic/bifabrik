from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
import pandas as pd

class CsvSource(DataSource):
    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._path = ""
        self._pattern = ""
        self._pathType = ""
    
    def path(self, path: str):
        self._path = path
        self._pathType = 'PATH'
        return self
    
    def pattern(self, pattern: str):
        self._pattern = pattern
        self._pathType = 'PATTERN'
        return self
    
    def toDf(self) -> DataFrame:
        csv_path = f'/lakehouse/default/Files/{self._path}'
        pd_df = pd.read_csv(csv_path)
        df = self._spark.createDataFrame(pd_df)
        return df