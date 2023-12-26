from bifabrik.src import DataSource
import spark.sql.SparkSession
import pandas as pd

class CsvSource(DataSource):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._path = ""
        self._pattern = ""
        self._pathType = ""
    
    def setPath(self, path: str):
        self._path = path
        self._pathType = 'PATH'
    
    def setPattern(self, pattern: str):
        self._pattern = pattern
        self._pathType = 'PATTERN'
    
    def load(self) -> pyspark.sql.DataFrame:
        csv_path = f'/lakehouse/default/Files/{self._path}'
        pd_df = pd.read_csv(csv_path)
        df = self.spark.createDataFrame(pd_df)
        return df