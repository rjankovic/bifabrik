from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.JsonSourceConfiguration import JsonSourceConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
import bifabrik.utils.log as lg

class JsonSource(DataSource, JsonSourceConfiguration):
    """CSV data source. Uses the spark loader in the backend.
    Supports (most of) the options from https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameReader.json.html

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromJson('Files/JSON/ITA_TabOrder.json').multiLine(True).toTable('TabOrder1').run()
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        JsonSourceConfiguration.__init__(self)
        self._path = ""
        self.__processed_abfs_paths = []
        self.__mergedConfig = None

    def __str__(self):
        return f'CSV source: {self._path}'
    
    def path(self, path: str):
        """Set the path (or pattern) to the source file.
        It searches the Files/ folder in the current lakehouse
        """
        self._path = path
        return self
    
    # if key in other.propDict[key]._explicitProps:
    # df = spark.read.option("multiLine", "true").option('mode', 'PERMISSIVE').json("Files/JSON/ITA_TabZakazka.json")

    def execute(self, input) -> DataFrame:
        lgr = lg.getLogger()
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__mergedConfig = mergedConfig

        # find source files
        srcLh = mergedConfig.sourceStorage.sourceLakehouse
        srcWs = mergedConfig.sourceStorage.sourceWorkspace

        source_files = fsUtils.filePatternSearch(self._path, srcLh, srcWs)
        self.__processed_abfs_paths = source_files
        if len(source_files) == 0:
            return None
        
        separator = ', '
        lgr.info(f'Loading JSON files: [{separator.join(source_files)}]')
        
        # set spark options for all the configuration switches specified in the task's config
        readerBase = self._spark.read
        for key in mergedConfig.fileSource._explicitProps:
            #lgr.info(f'Setting {key} to {mergedConfig.fileSource._explicitProps[key]}')
            #print(f'Setting {key} to {mergedConfig.fileSource._explicitProps[key]}')
            readerBase.option(key, mergedConfig.fileSource._explicitProps[key])
        for key in mergedConfig.json._explicitProps:
            #lgr.info(f'Setting {key} to {mergedConfig.json._explicitProps[key]}')
            #print(f'Setting {key} to {mergedConfig.json._explicitProps[key]}')
            readerBase.option(key, mergedConfig.json._explicitProps[key])

        df = readerBase.json(source_files)
        
        self._result = df
        self._completed = True
    
    def cleanup(self):
        if self.__mergedConfig.fileSource.moveFilesToArchive:
            fsUtils.archiveFiles(files = self.__processed_abfs_paths, 
                                 archiveFolder = self.__mergedConfig.fileSource.archiveFolder, 
                                 filePattern = self.__mergedConfig.fileSource.archiveFilePattern, 
                                 lakehouse = self.__mergedConfig.sourceStorage.sourceLakehouse, 
                                 workspace = self.__mergedConfig.sourceStorage.sourceWorkspace)