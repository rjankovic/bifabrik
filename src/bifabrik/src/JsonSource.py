from bifabrik.src.DataSource import DataSource
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils

class JsonSource(DataSource):
    """CSV data source
    """
    
    def __init__(self, dataLoader):
        super().__init__(dataLoader)
        self._path = ""
    
    def path(self, path: str):
        """Set the path (or pattern) to the source file.
        It searches the Files/ folder in the current lakehouse, so 
        e.g. path("datasource/*.json") will match "Files/datasource/file1.json", "Files/datasource/file1.json", ...
        """
        self._path = path
        return self
    
    def toDf(self) -> DataFrame:
        source_files = fsUtils.filePatternSearch(self._path)
        if len(source_files) == 0:
            return None
        df = self._spark.read.option("multiline", "true").json(source_files)
        return df