from bifabrik.src.DataSource import DataSource
from pyspark.sql.dataframe import DataFrame
import pandas as pd
from bifabrik.utils import fsUtils

class CsvSource(DataSource):
    """CSV data source
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        self._path = ""
    
    def path(self, path: str):
        """Set the path (or pattern) to the source file.
        It searches the Files/ folder in the current lakehouse, so 
        e.g. path("datasource/*.csv") will match "Files/datasource/file1.csv", "Files/datasource/file1.csv", ...
        """
        self._path = path
        return self
    
    def execute(self, input):
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
        
        self._result = df
        self._completed = True