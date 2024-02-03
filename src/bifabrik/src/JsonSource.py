from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.JsonSourceConfiguration import JsonSourceConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils

class JsonSource(DataSource, JsonSourceConfiguration):
    """CSV data source
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        JsonSourceConfiguration.__init__(self)
        self._path = ""
    
    def path(self, path: str):
        """Set the path (or pattern) to the source file.
        It searches the Files/ folder in the current lakehouse, so 
        e.g. path("datasource/*.json") will match "Files/datasource/file1.json", "Files/datasource/file1.json", ...
        """
        self._path = path
        return self
    
    # if key in other.propDict[key]._explicitProps:
    # df = spark.read.option("multiLine", "true").option('mode', 'PERMISSIVE').json("Files/JSON/ITA_TabZakazka.json")

    def execute(self, input) -> DataFrame:
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)

        # find source files
        source_files = fsUtils.filePatternSearch(self._path)
        if len(source_files) == 0:
            return None
        
        # set spark options for all the configuration switches specified in the task's config
        readerBase = self._spark.read
        for key in mergedConfig.fileSource._explicitProps:
            readerBase.option(key, mergedConfig.fileSource['key'])
        for key in mergedConfig.json._explicitProps:
            readerBase.option(key, mergedConfig.json['key'])

        df = readerBase.json(source_files)
        
        self._result = df
        self._completed = True