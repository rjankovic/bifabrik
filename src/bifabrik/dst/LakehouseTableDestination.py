from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
import bifabrik.utils.log as lg
from bifabrik.utils import fsUtils
#from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import time
from datetime import datetime
import notebookutils.mssparkutils.fs
from bifabrik.utils import tableUtils as tu
import bifabrik.dst.CommonDestinationUtils as commons
from unidecode import unidecode

class LakehouseTableDestination(DataDestination, TableDestinationConfiguration):
    """Saves data to a lakehouse table.

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromSql.query('SELECT * FROM SomeTable').toTable('DestinationTable1').run()
    """
    def __init__(self, pipeline: Pipeline, targetTableName: str, targetSchemaName: str = 'dbo'):
        super().__init__(pipeline)
        TableDestinationConfiguration.__init__(self)
        self.__targetTableName = targetTableName
        self.__targetSchemaName = targetSchemaName
        if '.' in targetTableName:
            split_name = targetTableName.split('.')
            self.__targetSchemaName = split_name[0]
            self.__targetTableName = split_name[1]

        self.__data = None
        self.__config = None
        self.__lhBasePath = None
        self.__identityColumn = None
        self.__insertDateColumn = None
        self.__updateDateColumn = None
        self.__tableExists = False
        self.__logger = None
        self.__tableLocation = None
        self.__addNARecord = False
        self.__addBadValueRecord = False
        self.__scd2RowStartTimestamp = None
        self.__scd2ExcludeColumns = []
        self.__partitionByColumns = []
        self.__partitionsDefined = False

    def __str__(self):
        return f'Table destination: {self.__targetTableName}'
    
    def execute(self, input: DataFrame) -> None:
        lgr = lg.getLogger()
        self.__logger = lgr
        self._error = None
        
        if input is None:
            msg = f'The table destination `{self.__targetTableName}` has no input; terminating'
            print(msg)
            lgr.warning(msg)
            return
        
        self.__data = input

        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        self.__config = mergedConfig
        self.__tableConfig = mergedConfig.destinationTable
        
        # print(f'dst tbl cfg')
        # print(f'increment {self.__tableConfig.increment}')
        # print(f'mergeKeyColumns {self.__tableConfig.mergeKeyColumns}')
        # print(f'snapshotKeyColumns {self.__tableConfig.snapshotKeyColumns}')
        # print(f'identityColumnPattern {self.__tableConfig.identityColumnPattern}')
        # print(f'insertDateColumn {self.__tableConfig.insertDateColumn}')
        # print(f'invalidCharactersInColumnNamesReplacement {self.__tableConfig.invalidCharactersInColumnNamesReplacement}')

        dstLh = mergedConfig.destinationStorage.destinationLakehouse
        dstWs = mergedConfig.destinationStorage.destinationWorkspace
        self.__lhBasePath = fsUtils.getLakehousePath(dstLh, dstWs)
        self.__lhMeta = fsUtils.getLakehouseMeta(dstLh, dstWs)
        self.__useSchema = self.__lhMeta.schemasEnabled
        self.__sechemaRefPrefix = ''
        self.__tableLocation = self.__lhBasePath + "/Tables/" + self.__targetTableName
        if self.__useSchema:
            self.__sechemaRefPrefix = f'{self.__targetSchemaName}.'
            self.__tableLocation = self.__lhBasePath + "/Tables/" + self.__targetSchemaName + '/' + self.__targetTableName
        self.__addNARecord = self.__tableConfig.addNARecord
        self.__addBadValueRecord = self.__tableConfig.addBadValueRecord
        self.__scd2ExcludeColumns = self.__tableConfig.scd2ExcludeColumns
        self.__partitionByColumns = self.__tableConfig.partitionByColumns
        self.__partitionsDefined = len(self.__partitionByColumns) > 0
        self.__tableExists = self.__tableExistsF()

        incrementMethod = mergedConfig.destinationTable.increment
        if incrementMethod is None:
            incrementMethod = 'overwrite'
        incrementMethod = incrementMethod.lower()
        self.__incrementMethod = incrementMethod

        self.__replaceInvalidCharactersInColumnNames()
        
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is not None:
            identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, lakehousename = self.__lhMeta.lakehouseName)
            self.__identityColumn = identityColumn

        # merge handles identity insert itself - separating the new records from the updates
        if (incrementMethod != 'merge' and incrementMethod != 'scd2') or self.__identityColumn is None or not(self.__tableExists):
            self.__insertIdentityColumn()

        #print('insert inserted date')
        self.__insertInsertDateColumn()
        self.__insertUpdateDateColumn()
        #print('insert N/A')
        self.__insertNARecord()
        #print('insert BadValue')
        self.__insertBadValueRecord()
        #print('insert scd2 tracking')
        self.__insertScd2TrackingColumns()

        #print('resolve schema differences')
        self.__resolveSchemaDifferences()
        
        self.__filterByWatermark()

        # if the target target table does not exist yet, just handle it as an overwrite
        if not(self.__tableExists):
            if incrementMethod == 'merge':
                # even if the table does not exist, check for duplicate merge keys
                self.__checkDuplicatesInMerge()
            self.__overwriteTarget()
        elif incrementMethod == 'overwrite':
            self.__overwriteTarget()
        elif incrementMethod == 'append':
            self.__appendTarget()
        elif incrementMethod == 'merge':
            self.__mergeTarget()
        elif incrementMethod == 'snapshot':
            self.__snapshotTarget()
        elif incrementMethod == 'scd2':
            self.__scd2Target()
        else:
            raise Exception(f'Unrecognized increment type: {incrementMethod}')

        if self.__tableConfig.ensureTableIsReadyAfterSaving:
            if not self.__data.rdd.isEmpty():
                table_name_full_check_existence = f"{self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}`{self.__targetTableName}`"
                self.__ensureTableIsReady(table_name_full_check_existence)

        self._result = self.__targetTableName
        self._completed = True
        
    def __list_diff(self, first_list, second_list):
        diff = [item for item in first_list if item not in second_list]
        return diff
        
    def __replaceInvalidCharactersInColumnNames(self):
        replacement = self.__tableConfig.invalidCharactersInColumnNamesReplacement
        if replacement is None:
            return
        
        self.__data = (
            self.
            __data
            .select(
                [col(f'`{c}`').alias(
                    self.__sanitizeColumnName(c)) 
                    for c in self.__data.columns
                ]))

    def __sanitizeColumnName(self, colName):
        replacement = self.__tableConfig.invalidCharactersInColumnNamesReplacement
        if replacement is None:
            return colName
        
        invalids = " ,;{}()\n\t="
        name = colName        
        for i in invalids:
            name = name.replace(i, replacement)
        name_without_accents = unidecode(name)
        return name_without_accents

    def __insertIdentityColumn(self):
        df_res = self.__insertIdentityColumnToDf(self.__data)
        self.__data = df_res

    def __insertIdentityColumnToDf(self, df):
        lgr = lg.getLogger()
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        lgr.info(f'adding identity to df using pattern {identityColumnPattern}')
        if identityColumnPattern is None:
            return df
        
        lhName = self.__lhMeta.lakehouseName
        identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, lakehousename = lhName)
        self.__identityColumn = identityColumn

        initID = 0
        if self.__tableExists:
            query = f"SELECT MAX({identityColumn}) FROM {lhName}.{self.__sechemaRefPrefix}{self.__targetTableName}"
            initID = self._spark.sql(query).collect()[0][0]
            # if the table exists, but is empty
            if initID is None:
                initID = 0
        
        # place the identity column at the beginning of the table
        schema = [obj[0]  for obj in df.dtypes]
        schema.insert(0, identityColumn)
        df_ids  = df.withColumn(identityColumn, row_number().over(Window.orderBy(schema[1])).cast('bigint')).select(schema)
        df_res = df_ids.withColumn(identityColumn,col(identityColumn) + initID)
        
        return df_res

    def __insertInsertDateColumn(self):
        insertDateColumn = self.__tableConfig.insertDateColumn
        if insertDateColumn is None:
            return
        
        self.__insertDateColumn = insertDateColumn
        ts = time.time()
        r = self.__data.withColumn(insertDateColumn, lit(ts).cast("timestamp"))
        self.__data = r
    
    def __insertUpdateDateColumn(self):
        updateDateColumn = self.__tableConfig.updateDateColumn
        if updateDateColumn is None:
            return
        
        self.__updateDateColumn = updateDateColumn
        ts = time.time()
        r = self.__data.withColumn(updateDateColumn, lit(ts).cast("timestamp"))
        self.__data = r
    
    def __insertScd2TrackingColumns(self):        
        # self.__rowStartColumn = self.__tableConfig.rowStartColumn
        # row_end_column = self.__tableConfig.rowEndColumn
        # current_row_column = self.__tableConfig.currentRowColumn

        # src_filtered_sql = f'SELECT src.*, CURRENT_TIMESTAMP() AS {row_start_column}, CAST(9999-12-31 AS TIMESTMP) AS {row_end_column}, TRUE AS {current_row_column} \
        # FROM {src_view_name} src INNER JOIN {left_join_temp_name} AS lj ON {join_condition_src_left_join}'
        
        if self.__incrementMethod != 'scd2':
            return
        
        row_start_ts = ts = time.time()
        row_end = time.strptime("9999-12-31", '%Y-%m-%d')
        row_end_ts = time.mktime(row_end)
        self.__scd2RowStartTimestamp = row_start_ts
        r = self.__data \
            .withColumn(self.__tableConfig.rowStartColumn, lit(row_start_ts).cast("timestamp")) \
            .withColumn(self.__tableConfig.rowEndColumn, lit(row_end_ts).cast("timestamp")) \
            .withColumn(self.__tableConfig.currentRowColumn, lit(True))
        self.__data = r


    def __insertNARecord(self):
        if not self.__addNARecord:
            return
        # bad value and N/A records could cause duplicates in the merge columns
        # so insert these only when creating the table
        if self.__tableExists and self.__incrementMethod in ['merge', 'scd2']:
            return
        if self.__identityColumn is None:
            raise Exception('Configuration error - when addNARecord is enabled, identityColumnPattern needs to be configured as well.')
        if (not self.__tableExists) or (self.__incrementMethod in ['overwrite', 'snapshot']):
            self.__data = commons.addNARecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
            return
        na_exists_sql = f'SELECT COUNT(*) FROM {self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}`{self.__targetTableName}` WHERE `{self.__identityColumn}` = -1'
        na_exists = self._spark.sql(na_exists_sql).collect()[0][0]
        if na_exists == 0:
            self.__data = commons.addNARecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
    
    def __insertBadValueRecord(self):
        if not self.__addBadValueRecord:
            return
        # bad value and N/A records could cause duplicates in the merge columns
        # so insert these only when creating the table
        if self.__tableExists and self.__incrementMethod in ['merge', 'scd2']:
            return
        if self.__identityColumn is None:
            raise Exception('Configuration error - when addBadValueRecord is enabled, identityColumnPattern needs to be configured as well.')
        if (not self.__tableExists) or (self.__incrementMethod in ['overwrite', 'snapshot']):
            self.__data = commons.addBadValueRecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
            return
        bv_exists_sql = f'SELECT COUNT(*) FROM {self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}`{self.__targetTableName}` WHERE `{self.__identityColumn}` = 0'
        bv_exists = self._spark.sql(bv_exists_sql).collect()[0][0]
        if bv_exists == 0:
            self.__data = commons.addBadValueRecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)

    def __overwriteTarget(self):
        writer = self.__data.write
        if self.__partitionsDefined:
            writer = writer.partitionBy(self.__partitionByColumns)

        writer.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__tableLocation)

    def __appendTarget(self):
        writer = self.__data.write
        if self.__partitionsDefined:
            writer = writer.partitionBy(self.__partitionByColumns)

        writer.mode("append").format("delta").save(self.__tableLocation)

    def __resolveSchemaDifferences(self):
        lgr = lg.getLogger()

        lgr.info('resolve schema differences...')

        if not(self.__tableExists):
            return
        if self.__incrementMethod == 'overwrite':
            return
        
        target_table = f'{self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}{self.__targetTableName}'
        df_old = self._spark.sql(f"SELECT * FROM {target_table} LIMIT 0")
        cols_new = self._spark.createDataFrame(self.__data.dtypes, ["new_name", "new_type"])
        cols_old = self._spark.createDataFrame(df_old.dtypes, ["old_name", "old_type"])
        
        if self.__identityColumn is not None:
            cols_new = cols_new.filter(cols_new.new_name != self.__identityColumn)
            cols_old = cols_old.filter(cols_old.old_name != self.__identityColumn)

        compare = (
            cols_new.join(cols_old, cols_new.new_name == cols_old.old_name, how="outer")
            .fillna("")
        )
        # TODO: catch unsolvable type differences
        difference = compare.filter(compare.new_type != compare.old_type) \
            .filter((col('old_name') == '') | (col('new_name') == ''))

        canAddColumns = self.__tableConfig.canAddNewColumns
        allowMissingColumnsInSource = self.__tableConfig.allowMissingColumnsInSource

        
        # lgr.info('old table columns')
        # lgr.info(self.__getShowString(cols_old))
        # lgr.info('new table columns')
        # lgr.info(self.__getShowString(cols_new))

        # lgr.info('schema compare')
        # lgr.info(self.__getShowString(compare))
        lgr.info('schema difference')
        lgr.info(self.__getShowString(difference))
        #print('schema compare')
        #compare.show()
        #print('schema difference')
        #difference.show()


        # filter helper
        # dff = df.where( ~(~( ((df.LayerName == 'Gold') & (df.TableName.like('DM%'))) |  ((df.LayerName == 'Silver') & (df.TableName == 'Branches')) )) )
        # display(dff)

        # if canAddColumns:
        
        solvable = difference.where(((difference.old_name == '') & (canAddColumns == lit(True))) | ((difference.new_name == '') & (allowMissingColumnsInSource == lit(True))))
        insolvable = difference.where(~(((difference.old_name == '') & (canAddColumns == lit(True))) | ((difference.new_name == '') & (allowMissingColumnsInSource == lit(True)))))
        
            #insolvable = difference.where(difference.old_name != '')
        # else:
        #     solvable = difference.where(difference.old_type == '____none____')
        #     insolvable = difference

        if insolvable.count() > 0:
            err = f'Schema difference detected in table {target_table} that cannot be merged:'
            for r in insolvable.collect():
                ons = "N/A" if r.old_name == '' else r.old_name
                ots = "N/A" if r.old_type == '' else r.old_type
                nns = "N/A" if r.new_name == '' else r.new_name
                nts = "N/A" if r.new_type == '' else r.new_type
                err = err + f'\n> old_name: {ons}, old_type: {ots}'
                err = err + f', new_name: {nns}, new_type: {nts}'
            lgr.error(err)
            insolvable.show()
            raise Exception(err)

        for r in solvable.collect():
            
            # add new column to destination table
            if r.old_name == '':
                lgr.info(f'Adding new column {r.new_name} (r.new_type) to {self.__targetTableName}')
                tu.addTableColumnFromType(self.__lhMeta.lakehouseName, self.__targetTableName, r.new_name, r.new_type)
            # add missing column to source df
            elif r.new_name == '':
                lgr.info(f'Adding empty column {r.old_name} to source table')
                self.__data = self.__data.withColumn(r.old_name, lit(None))
            # this shouldnot really happen in the solvable set
            else:
                raise Exception(f'unexpected schema difference: {r}')



    def __mergeTarget(self):
        
        # print(str(datetime.now()))
        # print('running merge')

        lgr = lg.getLogger()
        all_columns = self.__data.columns
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.mergeKeyColumns))
        non_key_columns = self.__list_diff(self.__list_diff(all_columns, key_columns), [self.__identityColumn, self.__insertDateColumn])
        
        # print('key columns')
        # print(key_columns)
        # print('non-key columns')
        # print(non_key_columns)
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for merge increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')
        

        # TODO: instead of SQL, use pyspark for cross-workspace ETL
        join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])
        update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
        insert_list = ", ".join([f"`{item}`" for item in all_columns])
        insert_values = ", ".join([f"src.`{item}`" for item in all_columns])
        key_col_list = ", ".join([f"`{item}`" for item in key_columns])
        
        scd1_update = f"WHEN MATCHED THEN UPDATE SET \
            {update_list} "
        # table consisisting only of key columns - no need for the update clause
        if len(non_key_columns) == 0:
            scd1_update = ""

        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        # temp table name in case we needed to use the large table approach
        src_temp_table_name_withid = f"temp_src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}_withid"
        

        merge_materialization_threshold = self.__tableConfig.mergeSourceMaterializationThresholdRowcount

        # if there are partitions, we're dealing with a big table
        # in that case, save the source to a temp table immediately to help with JOINs, instead of a view
        
        input_row_count = self.__data.count()
        if(input_row_count > merge_materialization_threshold):
                writer = self.__data.write
                if self.__partitionsDefined:
                    writer = writer.partitionBy(self.__partitionByColumns)
            
                # print(str(datetime.now()))
                # print(f'writing to initial src temp table {src_view_name}')

                writer.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_view_name}')
                
                self.__ensureTableIsReady(src_view_name)
                #time.sleep(10)
        else:
            self.__data.createOrReplaceTempView(src_view_name)
        
        mergeDbRef = f'{self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}' 

        # split data into news and updates
        
        # 1. separate new records dataframe

        src_news_df_sql = f'''
        SELECT /*+ BROADCAST(src) */ src.* 
        FROM {src_view_name} AS src
        LEFT JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
        WHERE tgt.`{key_columns[0]}` IS NULL
        '''
        # print('news df SQL')
        # print(src_news_df_sql)
        src_news_df = self._spark.sql(src_news_df_sql)
        
        # 2. generate IDs for the news DF independently - we don't want to mix these with the updates
        news_df = self.__insertIdentityColumnToDf(src_news_df)

        # 3. separate updated records dataframe 
        # and if identity column is configured, include in in the DF

        identityColumnPattern = self.__tableConfig.identityColumnPattern
        
        # print(str(datetime.now()))
        # print('handling identity')
        
        if identityColumnPattern is None:
            # SELECT /*+ BROADCAST(src) */ src.*
            src_updates_df = self._spark.sql(f'''
            SELECT src.* 
            FROM {src_view_name} AS src
            INNER JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
            ''')    
        else:
            insert_list = f'`{self.__identityColumn}`, {insert_list}'
            insert_values = f'src.`{self.__identityColumn}`, {insert_values}'
            src_updates_df_sql = f'''
            SELECT tgt.{self.__identityColumn}, src.* 
            FROM {src_view_name} AS src
            INNER JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
            '''

            # print('update df SQL')
            # print(src_updates_df_sql)
            src_updates_df = self._spark.sql(src_updates_df_sql)
        
        # 4. create an unified dataframe
        
        # print('news_df')
        # news_df.printSchema()
        # print('src_updates_df')
        # src_updates_df.printSchema()

        uni_df = news_df.union(src_updates_df)

        # get the target table size

        # print(str(datetime.now()))
        # print('getting the size of the target table')

        tableGB = 0
        inputGB = 0


        largeTableModeEnabled =  self.__tableConfig.largeTableMergeMethodEnabled
        sourceSizeThreshold = self.__tableConfig.largeTableMergeMethodSourceThresholdGB
        targetSizeThreshold = self.__tableConfig.largeTableMergeMethodDestinationThresholdGB

        targetTableGB = 0
        if largeTableModeEnabled:
            targetTableBytes = self._spark.sql(f'describe detail {mergeDbRef}{self.__targetTableName}').select("sizeInBytes").collect()[0][0]
            targetTableGB = targetTableBytes / 1024 / 1024 / 1024
            # empty placeholder value
            lgr.info(f'Target GB: {targetTableGB}')

        # print(str(datetime.now()))
        # print('size getting complete')

        # print(f'Large table mode enabled: {largeTableModeEnabled}')

        # to find out the source data sie, we need to save the source dataframe to a table
        # only do this if the target table size threshold is crossed
        # and the large table method is enabled

        if targetTableGB >= targetSizeThreshold and largeTableModeEnabled:

            lgr.info(f'writing to {src_temp_table_name_withid}')
            
            # print(str(datetime.now()))
            # print(f'writing to {src_temp_table_name_withid} - large table mode')

            #uni_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_temp_table_name_withid}')

            lgr.info(f'Partitioning source by {self.__partitionByColumns}')
            #print(f'Partitioning source by {self.__partitionByColumns}')
            writer = uni_df.write
            if self.__partitionsDefined:
                writer = writer.partitionBy(self.__partitionByColumns)
            writer.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_temp_table_name_withid}')
            
            # waiting for the new table to "come online"
            
            self.__ensureTableIsReady(src_temp_table_name_withid)
            #time.sleep(10)

            #uni_df.createOrReplaceTempView(src_temp_table_name_withid)
            #input_row_count = uni_df.count()
            lgr.info(f'input count {input_row_count}')
            try:
                inputBytes = self._spark.sql(f'describe detail {src_temp_table_name_withid}').select("sizeInBytes").collect()[0][0]
            except Exception as e:
                self.__logger.warning(f'failed to get the size of {src_temp_table_name_withid}')
                self.__logger.warning(e)
                time.sleep(20)
                inputBytes = self._spark.sql(f'describe detail {src_temp_table_name_withid}').select("sizeInBytes").collect()[0][0]
            inputGB = inputBytes / 1024 / 1024 / 1024
            lgr.info(f'Source GB: {inputGB}')
            # delete_src_temp_table_name_withid = True
        else:
            inputBytes = 0
            #uni_df.createOrReplaceTempView(src_temp_table_name_withid)

            # print(str(datetime.now()))
            # print('getting source count')
            # input_row_count = uni_df.count()

            # print(str(datetime.now()))
            # print(f'source count: {input_row_count}')

            if(input_row_count <= merge_materialization_threshold):
                uni_df.createOrReplaceTempView(src_temp_table_name_withid)
            else:
                
                # print(str(datetime.now()))
                # print(f'Partitioning source by {self.__partitionByColumns}')

                #print(f'Partitioning source by {self.__partitionByColumns}')
                writer = uni_df.write
                if self.__partitionsDefined:
                    lgr.info(f'Partitioning merge source by {self.__partitionByColumns}')
                    writer = writer.partitionBy(self.__partitionByColumns)
                writer.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_temp_table_name_withid}')
                

                # print(str(datetime.now()))
                # print(f'created {src_temp_table_name_withid}')
                
                # waiting for the new table to "come online"
                
                self.__ensureTableIsReady(src_temp_table_name_withid)
                #time.sleep(10)
                
        lgr.info('checking for duplicates')
        # print(str(datetime.now()))
        # print('checking for duplicates')
        duplicates_check = self._spark.sql(f'''
        SELECT {key_col_list}, COUNT(*) FROM {src_temp_table_name_withid}
        GROUP BY {key_col_list}
        HAVING COUNT(*) > 1
        ''')
        if duplicates_check.count() > 0:
            duplicates_check.show(10)
            raise Exception(f'There are duplicate records about to be merged into {self.__targetTableName}')

        #mergeDbRef = f'{self.__lhMeta.lakehouseName}.' 
        
        update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
        scd1_update = f"WHEN MATCHED THEN UPDATE SET \
            {update_list} "

        if tableGB < targetSizeThreshold or inputGB < sourceSizeThreshold or (not largeTableModeEnabled):
            # small table - direct update / insert
            merge_sql_update_basic = f"MERGE INTO {mergeDbRef}{self.__targetTableName} AS tgt \
            USING {src_temp_table_name_withid}  AS src \
            ON {join_condition} \
            {scd1_update}\
            "

            merge_sql_insert_basic = f"MERGE INTO {mergeDbRef}{self.__targetTableName} AS tgt \
            USING {src_temp_table_name_withid}  AS src \
            ON {join_condition} \
            WHEN NOT MATCHED THEN INSERT ( \
            {insert_list} \
            ) VALUES ( \
            {insert_values} \
            )\
            "

            lgr.info('merge update')
            lgr.info(merge_sql_update_basic)
            
            # print(str(datetime.now()))
            # print('merge update')
            # print(merge_sql_update_basic)
            
            self._spark.sql(merge_sql_update_basic)

            lgr.info('merge insert')
            lgr.info(merge_sql_insert_basic)
            
            # print(str(datetime.now()))
            # print('merge insert')
            # print(merge_sql_insert_basic)
            
            
            self._spark.sql(merge_sql_insert_basic)
            
            self._spark.sql(f'DROP TABLE IF EXISTS {src_view_name}')
            self._spark.sql(f'DROP TABLE IF EXISTS {src_temp_table_name_withid}')

            return
        
        msg = 'large table mrege strategy - rebuilding the target table'
        lgr.info(msg)
        print(msg)
        
        tgt_temp_table_name = f'temp_scd_tgt_{self.__targetTableName}'
        lgr.info(f'pooling rows for update to {tgt_temp_table_name}')
        tgt_df_query = f'''
        SELECT tgt.* 
        FROM {src_temp_table_name_withid} AS src
        INNER JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
        '''

        lgr.info(tgt_df_query)
        # print(tgt_df_query)
        tgt_df = self._spark.sql(tgt_df_query)

        tgt_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{tgt_temp_table_name}')

        # waiting for the new tables to "come online"
        
        self.__ensureTableIsReady(tgt_temp_table_name)
        #time.sleep(10)

        
        df_tgt_temp = self._spark.read.format('delta').load(f'Tables/{tgt_temp_table_name}')
        lgr.info(f'tgt temp count {df_tgt_temp.count()}')
        

        # 5. merge update and insert to the temp destination from source view
        
        merge_sql_update_temp = f"MERGE INTO {tgt_temp_table_name} AS tgt \
                    USING {src_temp_table_name_withid}  AS src \
                    ON {join_condition} \
                    {scd1_update}\
                    "
        lgr.info('merge update temp')
        lgr.info(merge_sql_update_temp)
        self._spark.sql(merge_sql_update_temp)

        merge_sql_insert_temp = f"MERGE INTO {tgt_temp_table_name} AS tgt \
            USING {src_temp_table_name_withid}  AS src \
            ON {join_condition} \
            WHEN NOT MATCHED THEN INSERT ( \
            {insert_list} \
            ) VALUES ( \
            {insert_values} \
            )\
            "
        lgr.info('merge insert temp')
        lgr.info(merge_sql_insert_temp)
        self._spark.sql(merge_sql_insert_temp)

        df_merged = self._spark.read.format('delta').load(f'Tables/{tgt_temp_table_name}')
        lgr.info(df_merged.count())

        # 5. add unmodified (unaffected, unmatched) rows from target to the temp table
        # (Xmerge delete from target table using temp destination (on merge keys))

        lgr.info('adding unchanged rows to temp')

        #tgt_temp_table_name = f'temp_scd_tgt_{self.__target_table_name}'
        tgt_unchanged_df_query = f'''
        SELECT tgt.* 
        FROM {mergeDbRef}{self.__targetTableName} AS tgt
        LEFT JOIN {src_temp_table_name_withid} AS src ON {join_condition}
        WHERE src.`{key_columns[0]}` IS NULL
        '''
        lgr.info(tgt_unchanged_df_query)
        tgt_unchanged_df = self._spark.sql(tgt_unchanged_df_query)

        lgr.info(tgt_unchanged_df.count())
        tgt_unchanged_df.write.mode("append").format("delta").save(f'Tables/{tgt_temp_table_name}')

        lgr.info('writing back to target table')

        #print('appending new and updated rows')
        #df_full = spark.sql(f'SELECT * FROM `{tgt_temp_table_name}`')
        df_full = self._spark.read.format('delta').load(f'Tables/{tgt_temp_table_name}')
        lgr.info(f'writing {df_full.count()} rows')
        
        writer = df_full.write
        if self.__partitionsDefined:
            writer = writer.partitionBy(self.__partitionByColumns)

        writer.write.mode("overwrite").format("delta").save(self.__tableLocation)

        self._spark.sql(f'DROP TABLE IF EXISTS {src_temp_table_name_withid}')
        self._spark.sql(f'DROP TABLE IF EXISTS {tgt_temp_table_name}')
        self._spark.sql(f'DROP TABLE IF EXISTS {src_view_name}')

    def __checkDuplicatesInMerge(self):
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.mergeKeyColumns))
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for merge increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')

        duplicates = self.__data.groupBy(*key_columns).count().where(col('count') > 1)
        
        if duplicates.count() > 0:
            duplicates.show(10)
            raise Exception(f'There are duplicate records about to be merged into {self.__targetTableName}')


    def __scd2Target(self):
        lgr = lg.getLogger()
        row_start_column = self.__tableConfig.rowStartColumn
        row_end_column = self.__tableConfig.rowEndColumn
        current_row_column = self.__tableConfig.currentRowColumn
        all_columns = self.__data.columns
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.mergeKeyColumns))
        non_key_columns = self.__list_diff(self.__list_diff(all_columns, key_columns), [self.__identityColumn, self.__insertDateColumn, row_start_column, row_end_column, current_row_column])
        non_key_historized_columns = self.__list_diff(non_key_columns, self.__scd2ExcludeColumns)
        soft_deletes = self.__tableConfig.scd2SoftDelete
        
        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        # temp table name in case we needed to use the large table approach
        src_temp_table_name_withid = f"temp_src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}_withid"
        self.__data.createOrReplaceTempView(src_view_name)
        db_reference = f'{self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}' 
        
        # print('key columns')
        # print(key_columns)
        # print('non-key columns')
        # print(non_key_columns)
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for SCD 2 increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')
        
        # TODO: instead of SQL, use pyspark for cross-workspace ETL
        join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])
        # update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
        # insert_list = ", ".join([f"`{item}`" for item in all_columns])
        # insert_values = ", ".join([f"src.`{item}`" for item in all_columns])
        key_col_list = ", ".join([f"`{item}`" for item in key_columns])

        # 1. check for duplicates
        # 2. if soft deletes are enabled, end-date deleted rows (source left join target...)
        # 3. target left join source to find matches
        # 4. for matched rows, find rows where all non-key attributes are the same as before and remove them from source
        # 5. end-date remaining matched current rows in target
        # 6. add RowStart, RowEnd and CurrentRow to source rows
        # 7. generate identity column for the source data if configured
        # 8. append all source rows to target


        # 1. check for duplicates

        deduplicated = self.__data.dropDuplicates(key_columns)
        orig_count = self.__data.count()
        deduplicated_count = deduplicated.count()
        if orig_count > deduplicated_count:
            #duplicates_df = self.__data.exceptAll(deduplicated)
            raise Exception(f'There are duplicate records about to be merged into {self.__targetTableName}')
        
        # 2. if soft deletes are enabled, end-date deleted rows (target left join source...)

        # will be used throughout the method
        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        self.__data.createOrReplaceTempView(src_view_name)
        end_date_merge_update = f'SET tgt.{row_end_column} = CAST({self.__scd2RowStartTimestamp} AS TIMESTAMP), tgt.{current_row_column} = FALSE'
        #end_date_merge_update = f'UPDATE SET tgt.{row_end_column} = CURRENT_TIMESTAMP(), tgt.{current_row_column} = FALSE'
        
        if soft_deletes:
            soft_deletes_sql = f"""
            MERGE INTO {db_reference}{self.__targetTableName} AS tgt
            USING(
                SELECT {key_col_list}
                FROM {src_view_name}
                WHERE {current_row_column} = TRUE
            ) AS src
            ON {join_condition}
            WHEN NOT MATCHED BY SOURCE AND tgt.`{current_row_column}` = TRUE THEN UPDATE {end_date_merge_update}
            """
            
            lgr.info('running SQL merge to handle soft deletes')
            lgr.info('soft delete sql:')
            lgr.info(soft_deletes_sql)
            self._spark.sql(soft_deletes_sql)
        
        # 3. target left join source to find matches
        
# baded on https://medium.com/yipitdata-engineering/how-to-use-broadcasting-for-more-efficient-joins-in-spark-2d53a336b02b
# SELECT
# /*+ BROADCAST(small_df_a) */
# /*+ BROADCAST(small_df_b) */
# /*+ BROADCAST(small_df_c) */
# *
# FROM large_df
# LEFT JOIN small_df_a
# USING (id)
# LEFT JOIN small_df_b
# USING (id)
# LEFT JOIN small_df_c
# USING (id)

        #select_non_key_list = ", " + ", ".join([f"src.{item} AS src_{item}, tgt.{item} AS tgt_{item}" for item in non_key_columns])
        # non-key match condition


        key_match = " AND ".join([f"src.{item} <=> tgt.{item}" for item in key_columns])
        if len(non_key_historized_columns) == 0:
            combined_match = f"({key_match}) AS __scd2_combined_match"
        else:
            combined_match = f"({key_match} AND " + " AND ".join([f"src.{item} <=> tgt.{item}" for item in non_key_historized_columns]) + ") AS __scd2_combined_match"
        select_key_list = ", ".join([f"src.{item} AS src_{item}, tgt.{item} AS tgt_{item}" for item in key_columns])
        target_left_join_sql = f"""
SELECT
/*+ BROADCAST(src) */ 
{select_key_list}, {combined_match} 
FROM {src_view_name} AS src
LEFT JOIN {db_reference}{self.__targetTableName} AS tgt ON {join_condition} AND tgt.`{current_row_column}` = TRUE
"""
        lgr.info('running SQL left join to find matching rows')
        lgr.info('left join sql')
        lgr.info(target_left_join_sql)
        df_left_join = self._spark.sql(target_left_join_sql)
        left_join_temp_name = f"temp_match_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        
        # 4. for matched rows, find rows where all non-key attributes are the same as before and remove them from source

        #df_left_join.printSchema()
        df_left_join = df_left_join.filter('`__scd2_combined_match` = FALSE')
        df_left_join.createOrReplaceTempView(left_join_temp_name)
        self.__data.createOrReplaceTempView(src_view_name)
        
        #end_date_merge_update = f'SET tgt.{row_end_column} = CAST({self.__scd2RowStartTimestamp} AS TIMESTAMP), tgt.{current_row_column} = FALSE'
        #end_date_merge_update = f'SET tgt.{row_end_column} = CURRENT_TIMESTAMP(), tgt.{current_row_column} = FALSE'

        #src_view_name_filtered_with_id = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}_filtered_with_id"
        join_condition_src_left_join = " AND ".join([f"src.`{item}` = lj.`src_{item}`" for item in key_columns])
        
        # 6. add RowStart, RowEnd and CurrentRow to source rows
        
        src_filtered_sql = f'SELECT src.* \
        FROM {src_view_name} src INNER JOIN {left_join_temp_name} AS lj ON {join_condition_src_left_join}'

        #src_filtered_sql = f'SELECT src.*, CURRENT_TIMESTAMP() AS {row_start_column}, CAST(9999-12-31 AS TIMESTMP) AS {row_end_column}, TRUE AS {current_row_column} \
        #FROM {src_view_name} src INNER JOIN {left_join_temp_name} AS lj ON {join_condition_src_left_join}'

        lgr.info('filtered source query')
        lgr.info(src_filtered_sql)
        df_src_filtered = self._spark.sql(src_filtered_sql)
        
        # 7. generate identity column for the source data if configured

        df_src_filtered_with_id = self.__insertIdentityColumnToDf(df_src_filtered)
        
        #df_src_filtered_with_id.createOrReplaceTempView(src_view_name_filtered_with_id)
        
        # 5. end-date remaining matched current rows in target

        join_condition_tgt = " AND ".join([f"src.`tgt_{item}` = tgt.`{item}`" for item in key_columns])
        enddate_old_rows_sql = f"""
            MERGE INTO {db_reference}{self.__targetTableName} AS tgt
            USING {left_join_temp_name} AS src
            ON {join_condition_tgt}
            WHEN MATCHED AND tgt.`{current_row_column}` = TRUE THEN UPDATE {end_date_merge_update}
            """
        lgr.info('end-dating old rows')
        lgr.info(enddate_old_rows_sql)
        self._spark.sql(enddate_old_rows_sql)

        # 8. append all source rows to target
        
        lgr.info('appending new / updated data')

        writer = df_src_filtered_with_id.write
        if self.__partitionsDefined:
            writer = writer.partitionBy(self.__partitionByColumns)

        writer.mode("append").format("delta").save(self.__tableLocation)
        #df_src_filtered_with_id.write.mode("append").format("delta").save(self.__tableLocation)
        

    def __snapshotTarget(self):
        # first delete the snapshot to be replaced
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.snapshotKeyColumns))
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for snapshot increment. Please set the snapshotKeyColumns property in destinationTable configuration to the list of column names.')
        
        join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])

        join_column_list = ", ".join([f"`{item}`" for item in key_columns])
        
        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        self.__data.createOrReplaceTempView(src_view_name)
        mergeDbRef = f'{self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}'

        merge_delete_sql = f"MERGE INTO {mergeDbRef}{self.__targetTableName} AS tgt \
            USING (SELECT DISTINCT {join_column_list} FROM {src_view_name})  AS src \
            ON {join_condition} \
            WHEN MATCHED THEN DELETE \
            "
        
        self.__logger.info("----SNAPSHOT DELETE SQL")
        self.__logger.info(merge_delete_sql)
        #print(merge_delete_sql)
        self._spark.sql(merge_delete_sql)
    
        # append the new rows
        self.__appendTarget()

    def __tableExistsF(self):
        """Checks if a table in the lakehouse exists.
        """
        
        default_location = self.__lhBasePath + "/Tables/" + self.__targetTableName
        lowercase_location = self.__lhBasePath + "/Tables/" + self.__targetTableName.lower()
        if self.__useSchema:
            default_location = self.__lhBasePath + "/Tables/" + self.__targetSchemaName + '/' + self.__targetTableName
            lowercase_location = self.__lhBasePath + "/Tables/" + self.__targetSchemaName + '/' + self.__targetTableName.lower()
            
        self.__logger.info('checking the existence of ' + lowercase_location + ':')
        fileExists = notebookutils.mssparkutils.fs.exists(default_location) or notebookutils.mssparkutils.fs.exists(lowercase_location)
        self.__logger.info(fileExists)
        return fileExists
        
        # df_tables = self._spark.sql('SHOW TABLES')
        # df_f = df_tables.filter(lower('tableName') == self.__targetTableName.lower())
        # if df_f.count() > 0:
        #     return True
        # return False

    def __ensureTableIsReady(self, tableName: str):
        """After a delta table is written to the Tables/ folder in a lakehouse, 
        it can take a few seconds before it is available in SparkSQL.
        This function waits for up to 28 seconds (4 + 8 + 16) checking if the table is SQL-ready.
        When it's ready, the function returns. If it isn't, an exception is thrown.

        table_name is either a simple name or [lakehouse.table_name]
        """

        table_name_wrapped = tu.wrapInBackticks(tableName)
        wait_time = 2
        total_wait = 0
        while wait_time <= 16:
            try:
                df = self._spark.sql(f'SELECT * FROM {table_name_wrapped} LIMIT 0')
                return
            except Exception as e:
                if wait_time == 16:
                    print(f'The table {table_name_wrapped} still not found after {total_wait} seconds')
                    raise e
            wait_time = wait_time * 2
            time.sleep(wait_time)
            total_wait = total_wait + wait_time

    def __filterByWatermark(self):
        if not(self.__tableExists):
            return
        elif self.__incrementMethod is None:
            return
        elif self.__incrementMethod == 'overwrite':
            return
        
        watermarkColumn = self.__tableConfig.watermarkColumn
        if watermarkColumn is None:
            return
        
        max_watermark_sql = f'SELECT MAX(`{watermarkColumn}`) FROM {self.__lhMeta.lakehouseName}.{self.__sechemaRefPrefix}{self.__targetTableName}'
        # print(max_watermark_sql)
        max_watermark = self._spark.sql(max_watermark_sql).collect()[0][0]
        # print(f'max watermark: {max_watermark}')

        # if the table is empty or watermark is 0, take all the data
        if max_watermark is None:
            return
        if not max_watermark:
            return
        
        filter = f'{watermarkColumn} > "{max_watermark}"'
        self.__data = self.__data.filter(filter)

    def __getShowString(self, df, n=20, truncate=True, vertical=False):
        if isinstance(truncate, bool) and truncate:
            return(df._jdf.showString(n, 20, vertical))
        else:
            return(df._jdf.showString(n, int(truncate), vertical))
        # print(filter)
        # print(self.__data.count())