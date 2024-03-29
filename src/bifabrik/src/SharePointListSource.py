from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.SharePointListSourceConfiguration import SharePointListSourceConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import log
from shareplum import Site, folder
from shareplum import Office365
from shareplum.site import Version
import notebookutils.mssparkutils.credentials

class SharePointListSource(DataSource, SharePointListSourceConfiguration):
    """Excel data source
    uses the pandas loaders
    supports configuration options from https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
    
    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromExcel.path('ExcelData/factOrderLine.xlsx').sheetName('Sheet1').toTable('FactOrderLine').run()
    """
    
    def __init__(self, parentPipeline, siteUrl, listName):
        super().__init__(parentPipeline)
        SharePointListSourceConfiguration.__init__(self)
        self.__siteUrl = siteUrl
        self.__listName = listName
        self.__mergedConfig = None

    def __str__(self):
        return f'SharePointListSource source: {self.__siteUrl}/Lists/{self.__listName}'
    
    def execute(self, input):

        # kvLogin = mssparkutils.credentials.getSecret(self._keyVaultUrl, loginKvSecretName)
        # kvPwd = mssparkutils.credentials.getSecret(self._keyVaultUrl, passwordKvSecretName)

        # authcookie = Office365(self._sharePointServerUrl, username=kvLogin, password=kvPwd).GetCookies()
        # siteUrl = self._sharePointServerUrl + '/sites/' + siteName
        # site = Site(siteUrl, version=Version.v365, authcookie=authcookie)

        # lastSlash = filePath.rfind('/')
        # folderPath = filePath[0:lastSlash]
        # fileName = filePath[lastSlash + 1:]

        # folder = site.Folder(folderPath)
        # xlsx_content = folder.get_file(fileName)

        # pd_df = pd.read_excel(BytesIO(xlsx_content), sheet_name=sheetName)
        # df = self.spark.createDataFrame(pd_df)
        # dfts = self.df_add_timestamp(df)
        # dfts.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self._tablesPrefix + targetTableName)


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
        lgr.info(f'Loading Excel files: [{separator.join(source_files)}]')
        
        #source_files = fsUtils.filePatternSearch(self._path)
        fileDfs = []
        for src_file in source_files:
            #csv_path = f'/lakehouse/default/{src_file}'
            pd_df = pd.read_excel(
                io = src_file, 
                sheet_name = mergedConfig.excel.sheetName,
                header = mergedConfig.excel.header,
                names = mergedConfig.excel.names,
                usecols = mergedConfig.excel.usecols,
                skiprows = mergedConfig.excel.skiprows,
                nrows = mergedConfig.excel.nrows,
                thousands = mergedConfig.excel.thousands,
                decimal = mergedConfig.excel.decimal
                )
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