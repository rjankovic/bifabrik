from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.CsvSourceConfiguration import CsvSourceConfiguration
from pyspark.sql.dataframe import DataFrame
import pandas as pd
from bifabrik.utils import fsUtils
from bifabrik.utils import log

class CsvSource(DataSource, CsvSourceConfiguration):
    """CSV data source
    uses the pandas loaders
    supports configuration options from https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    
    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromCsv('DATA/factOrderLine*.csv').delimiter(';').decimal(',').toTable('FactOrderLine').run()
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        CsvSourceConfiguration.__init__(self)
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
    
    def execute(self, input):
        lgr = log.getLogger()
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__mergedConfig = mergedConfig

        srcLh = mergedConfig.sourceStorage.sourceLakehouse
        srcWs = mergedConfig.sourceStorage.sourceWorkspace

        # pandas seems to have a problem with absolute ABFS paths, so use /lakehouse/default/ if refering to the default lakehouse
        source_files = fsUtils.filePatternSearch(self._path, srcLh, srcWs, useImplicitDefaultLakehousePath = True)
        implicitLhPath = fsUtils.getLakehousePath(lakehouse = srcLh, workspace = srcWs, useImplicitDefaultLakehousePath = True)
        abfsLhPath = fsUtils.getLakehousePath(lakehouse = srcLh, workspace = srcWs, useImplicitDefaultLakehousePath = False)
        self.__processed_abfs_paths = [x.replace(implicitLhPath, abfsLhPath) for x in source_files]

        if len(source_files) == 0:
            return None
        
        separator = ', '
        lgr.info(f'Loading CSV files: [{separator.join(source_files)}]')
        
        #source_files = fsUtils.filePatternSearch(self._path)
        fileDfs = []
        for src_file in source_files:
            #csv_path = f'/lakehouse/default/{src_file}'
            pd_df = pd.read_csv(
                filepath_or_buffer = src_file, 
                encoding = mergedConfig.fileSource.encoding,
                delimiter = mergedConfig.csv.delimiter,
                header = mergedConfig.csv.header,
                thousands = mergedConfig.csv.thousands,
                decimal = mergedConfig.csv.decimal,
                quotechar = mergedConfig.csv.quotechar,
                quoting = mergedConfig.csv.quoting,
                escapechar = mergedConfig.csv.escapechar
                )
            if pd_df.empty:
                w = f'File {src_file} has no data - skipping'
                lgr.warn(w)
                print(w)
                continue
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

    def cleanup(self):
        if self.__mergedConfig.fileSource.moveFilesToArchive:
            fsUtils.archiveFiles(files = self.__processed_abfs_paths, 
                                 archiveFolder = self.__mergedConfig.fileSource.archiveFolder, 
                                 filePattern = self.__mergedConfig.fileSource.archiveFilePattern, 
                                 lakehouse = self.__mergedConfig.sourceStorage.sourceLakehouse, 
                                 workspace = self.__mergedConfig.sourceStorage.sourceWorkspace)