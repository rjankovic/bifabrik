from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
from bifabrik.utils import log
from bifabrik.utils import fsUtils
from pyspark.sql.functions import col

class TableDestination(DataDestination, TableDestinationConfiguration):
    """Saves data to a lakehouse table.

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromSql.query('SELECT * FROM SomeTable').toTable('DestinationTable1').run()
    """
    def __init__(self, pipeline: Pipeline, targetTableName: str):
        super().__init__(pipeline)
        TableDestinationConfiguration.__init__(self)
        self.__targetTableName = targetTableName
        self.__data = None
        self.__config = None
        self.__lhBasePath = None
        self.__tableExists = False

    def __str__(self):
        return f'Table destination: {self.__targetTableName}'
    
    def execute(self, input: DataFrame) -> None:
        lgr = log.getLogger()
        self._error = None
        
        if input is None:
            msg = f'The table destination `{self.__targetTableName}` has no input; terminating'
            print(msg)
            lgr.warning(msg)
            return
        
        self.__data = input

        #todo transformations

        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__config = mergedConfig
        self.__tableConfig = mergedConfig.destinationTable

        dstLh = mergedConfig.destinationStorage.destinationLakehouse
        dstWs = mergedConfig.destinationStorage.destinationWorkspace
        self.__lhBasePath = fsUtils.getLakehousePath(dstLh, dstWs)
        self.__tableExists = self.tableExists()

        self.replaceInvalidCharactersInColumnNames()
        self.insertIdentityColumn()
        self.insertInsertDateColumn()

        incrementMethod = mergedConfig.destinationTable.increment
        
        # if the target target table does not exist yet, just handle it as an overwrite
        if not(self.__tableExists):
            self.overwriteTarget()
        elif incrementMethod is None:
            incrementMethod = 'overwrite'
        elif incrementMethod == 'overwrite':
            self.overwriteTarget()
        elif incrementMethod == 'append':
            self.appendTarget()
        elif incrementMethod == 'merge':
            self.mergeTarget()
        elif incrementMethod == 'snapshot':
            self.snapshotTarget()

        self._completed = True
        
        #if not self.tableExists():
        #    self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__lhBasePath + "/Tables/" + self.__targetTableName)

        
    def replaceInvalidCharactersInColumnNames(self):
        replacement = self.__tableConfig.invalidCharactersInColumnNamesReplacement
        if replacement is None:
            return
        
        self.__data = (
            self.
            __data
            .select(
                [col(c).alias(
                    self.__sanitizeColumnName(c, replacement)) 
                    for c in self.__data.columns
                ]))

    def __sanitizeColumnName(self, colName, replacement):
        invalids = " ,;{}()\n\t="
        name = colName        
        for i in invalids:
            name = name.replace(i, replacement)
        return name

    def insertIdentityColumn(self):
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is None:
            return
        
        pass

    def insertInsertDateColumn(self):
        pass

    def overwriteTarget(self):
        pass

    def appendTarget(self):
        pass

    def mergeTarget(self):
        pass

    def snapshotTarget(self):
        pass

    def tableExists(self):
         """Checks if a table in the lakehouse exists.
         """
         dstLh = self.__config.destinationStorage.destinationLakehouse
         dstWs = self.__config.destinationStorage.destinationWorkspace
         fileExists = fsUtils.fileExists(dstLh, dstWs, f'Tables/{self.__targetTableName}')
         return fileExists
