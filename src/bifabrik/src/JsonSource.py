from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils

class JsonSource(DataSource):
    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._path = ""
    
    def path(self, path: str):
        self._path = path
        return self
    
    def toDf(self) -> DataFrame:
        source_files = fsUtils.filePatternSearch(self._path)
        if len(source_files) == 0:
            return None
        df = self._spark.read.option("multiline", "true").json(source_files)
        return df