from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
from bifabrik.utils import log
from bifabrik.utils import fsUtils
#from pyspark.sql.functions import col
from pyspark.sql.functions import *
import time
import datetime

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
        self.__identityColumn = None
        self.__insertDateColumn = None
        self.__tableExists = False
        self.__logger = None

    def __str__(self):
        return f'Table destination: {self.__targetTableName}'
    
    def execute(self, input: DataFrame) -> None:
        lgr = log.getLogger()
        self.__logger = lgr
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
        self.__lhMeta = fsUtils.getLakehouseMeta(dstLh, dstWs)
        self.__tableExists = self.__tableExists()

        self.__replaceInvalidCharactersInColumnNames()
        self.__insertIdentityColumn()
        self.__insertInsertDateColumn()

        incrementMethod = mergedConfig.destinationTable.increment
        
        # if the target target table does not exist yet, just handle it as an overwrite
        if not(self.__tableExists):
            self.__overwriteTarget()
        elif incrementMethod is None:
            incrementMethod = 'overwrite'
        elif incrementMethod == 'overwrite':
            self.__overwriteTarget()
        elif incrementMethod == 'append':
            self.__appendTarget()
        elif incrementMethod == 'merge':
            self.__mergeTarget()
        elif incrementMethod == 'snapshot':
            self.__snapshotTarget()

        self._completed = True
        
        #if not self.tableExists():
        #    self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__lhBasePath + "/Tables/" + self.__targetTableName)

        
    def __replaceInvalidCharactersInColumnNames(self):
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

    def __insertIdentityColumn(self):
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is None:
            return
        
        lhName = self.__lhMeta.lakehouseName
        initID = 0
        identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, lakehousename = lhName)
        self.__identityColumn = identityColumn

        if self.__tableExists:
            query = f"SELECT MAX({identityColumn}) FROM {lhName}.{self.__targetTableName}"
            initID = self.spark.sql(query).collect()[0][0]
            # if the table exists, but is empty
            if initID is None:
                initID = 0
        
        # place the identity column at the beginning of the table
        schema = [obj[0]  for obj in self.__data.dtypes]
        schema.insert(0, identityColumn)
        df_ids  = self.__data.withColumn(identityColumn, row_number().over(Window.orderBy(schema[1])).cast('bigint')).select(schema)
        df_res = df_ids.withColumn(identityColumn,col(identityColumn) + initID)
        
        self.__data = df_res

    def __insertInsertDateColumn(self):
        insertDateColumn = self.__tableConfig.insertDateColumn
        if insertDateColumn is None:
            return
        
        self.__insertDateColumn = insertDateColumn
        ts = time.time()
        r = self.__data.withColumn(insertDateColumn, lit(ts).cast("timestamp"))
        self.__data = r

    def __overwriteTarget(self):
        self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__lhBasePath + "/Tables/" + self.__targetTableName)

    def __appendTarget(self):
        self.__data.write.mode("append").format("delta").save(self.__lhBasePath + "/Tables/" + self.__targetTableName)

    def __mergeTarget(self):
        all_columns = self.__data.columns
        key_columns = self.__tableConfig.mergeKeyColumns
        non_key_columns = all_columns - key_columns - [self.__identityColumn, self.__insertDateColumn]
        
        join_condition = " AND ".join([f"src.{item} = tgt.{item}" for item in key_columns])
        update_list = ", ".join([f"{item} = src.{item}" for item in non_key_columns])
        insert_list = ", ".join([f"{item}" for item in all_columns])
        insert_values = ", ".join([f"src.{item}" for item in all_columns])
        
        scd1_update = f"WHEN MATCHED THEN UPDATE SET \
            {update_list} "
        # table consisisting only of key columns - no need for the update clause
        if len(non_key_columns) == 0:
            scd1_update = ""

        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        self.__data.createOrReplaceTempView(src_view_name)
        mergeDbRef = f'{self.__lhMeta.lakehouseName}.' 
        merge_sql = f"MERGE INTO {mergeDbRef}{self.__targetTableName} AS tgt \
            USING {src_view_name}  AS src \
            ON {join_condition} \
            {scd1_update}\
            WHEN NOT MATCHED THEN INSERT ( \
            {insert_list} \
            ) VALUES ( \
            {insert_values} \
            )\
            "
        self.__logger.info("----SCD1 MERGE SQL")
        self.__logger.info(merge_sql)
        self._spark.sql(merge_sql)

    def __snapshotTarget(self):
        pass

    def __tableExists(self):
         """Checks if a table in the lakehouse exists.
         """
         dstLh = self.__config.destinationStorage.destinationLakehouse
         dstWs = self.__config.destinationStorage.destinationWorkspace
         fileExists = fsUtils.fileExists(dstLh, dstWs, f'Tables/{self.__targetTableName}')
         return fileExists
