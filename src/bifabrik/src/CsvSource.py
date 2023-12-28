from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
import pandas as pd
from bifabrik.utils import fsUtils

class CsvSource(DataSource):
    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._path = ""
    
    def path(self, path: str):
        self._path = path
        return self
    
    def toDf(self) -> DataFrame:
        source_files = fsUtils.filePatternSearch(self._path)
        fileDfs = []
        for src_file in source_files:
            csv_path = f'/lakehouse/default/{src_file}'
            pd_df = pd.read_csv(csv_path)
            df = self._spark.createDataFrame(pd_df)
            fileDfs.append(df)
        if len(fileDfs) == 0:
            return None
        df = fileDfs[0]
        if len(fileDfs) > 1:
            for i in range(1, len(fileDfs)):
                df = df.union(fileDfs[i])
        return df