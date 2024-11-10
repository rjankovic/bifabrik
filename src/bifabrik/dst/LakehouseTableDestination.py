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
import datetime
import notebookutils.mssparkutils.fs
from bifabrik.utils import tableUtils as tu
import bifabrik.dst.CommonDestinationUtils as commons

class LakehouseTableDestination(DataDestination, TableDestinationConfiguration):
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
        self.__tableLocation = None
        self.__addNARecord = False
        self.__addBadValueRecord = False

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
        self.__addNARecord = self.__tableConfig.addNARecord
        self.__addBadValueRecord = self.__tableConfig.addBadValueRecord
        self.__tableLocation = self.__lhBasePath + "/Tables/" + self.__targetTableName
        self.__tableExists = self.__tableExistsF()

        incrementMethod = mergedConfig.destinationTable.increment
        if incrementMethod is None:
            incrementMethod = 'overwrite'
        self.__incrementMethod = incrementMethod

        self.__replaceInvalidCharactersInColumnNames()
        
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is not None:
            identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, lakehousename = self.__lhMeta.lakehouseName)
            self.__identityColumn = identityColumn

        # merge handles identity insert itself - separating the new records from the updates
        if incrementMethod != 'merge' or self.__identityColumn is None:
            self.__insertIdentityColumn()

        self.__insertInsertDateColumn()
        self.__insertNARecord()
        self.__insertBadValueRecord()

        self.__resolveSchemaDifferences()
        
        self.__filterByWatermark()

        # if the target target table does not exist yet, just handle it as an overwrite
        if not(self.__tableExists):
            self.__overwriteTarget()
        elif incrementMethod == 'overwrite':
            self.__overwriteTarget()
        elif incrementMethod == 'append':
            self.__appendTarget()
        elif incrementMethod == 'merge':
            self.__mergeTarget()
        elif incrementMethod == 'snapshot':
            self.__snapshotTarget()
        else:
            raise Exception(f'Unrecognized increment type: {incrementMethod}')

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
        return name

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
            query = f"SELECT MAX({identityColumn}) FROM {lhName}.{self.__targetTableName}"
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

    def __insertNARecord(self):
        if not self.__addNARecord:
            return
        if self.__identityColumn is None:
            raise Exception('Configuration error - when addNARecord is enabled, identityColumnPattern needs to be configured as well.')
        if (not self.__tableExists) or (self.__incrementMethod in ['overwrite', 'overwrite', 'snapshot']):
            self.__data = commons.addNARecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
            return
        na_exists_sql = f'SELECT COUNT(*) FROM {self.__lhMeta.lakehouseName}.`{self.__targetTableName}` WHERE `{self.__identityColumn}` = -1'
        na_exists = self._spark.sql(na_exists_sql).collect()[0][0]
        if na_exists == 0:
            self.__data = commons.addNARecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
    
    def __insertBadValueRecord(self):
        if not self.__addBadValueRecord:
            return
        if self.__identityColumn is None:
            raise Exception('Configuration error - when addBadValueRecord is enabled, identityColumnPattern needs to be configured as well.')
        if (not self.__tableExists) or (self.__incrementMethod in ['overwrite', 'overwrite', 'snapshot']):
            self.__data = commons.addBadValueRecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)
            return
        bv_exists_sql = f'SELECT COUNT(*) FROM {self.__lhMeta.lakehouseName}.`{self.__targetTableName}` WHERE `{self.__identityColumn}` = 0'
        bv_exists = self._spark.sql(bv_exists_sql).collect()[0][0]
        if bv_exists == 0:
            self.__data = commons.addBadValueRecord(self.__data, self._spark, self.__targetTableName, self.__identityColumn)

    def __overwriteTarget(self):
        self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__tableLocation)

    def __appendTarget(self):
        self.__data.write.mode("append").format("delta").save(self.__tableLocation)

    def __resolveSchemaDifferences(self):
        lgr = lg.getLogger()

        if not(self.__tableExists):
            return
        if self.__incrementMethod == 'overwrite':
            return
        
        target_table = f'{self.__lhMeta.lakehouseName}.{self.__targetTableName}'
        df_old = self._spark.sql(f"SELECT * FROM {target_table} LIMIT 0")
        cols_new = self._spark.createDataFrame(self.__data.dtypes, ["new_name", "new_type"])
        cols_old = self._spark.createDataFrame(df_old.dtypes, ["old_name", "old_type"])
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

        if canAddColumns:
            solvable = difference.where(difference.old_name == '')
            insolvable = difference.where(difference.old_name != '')
        else:
            solvable = difference.where(difference.old_type == '____none____')
            insolvable = difference

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
            tu.addTableColumnFromType(self.__lhMeta.lakehouseName, self.__targetTableName, r.new_name, r.new_type)


    def __mergeTarget(self):
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
        self.__data.createOrReplaceTempView(src_view_name)
        mergeDbRef = f'{self.__lhMeta.lakehouseName}.' 

        # split data into news and updates
        
        # 1. separate new records dataframe

        src_news_df = self._spark.sql(f'''
        SELECT src.* 
        FROM {src_view_name} AS src
        LEFT JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
        WHERE tgt.`{key_columns[0]}` IS NULL
        ''')
        
        # 2. generate IDs for the news DF independently - we don't want to mix these with the updates
        news_df = self.__insertIdentityColumnToDf(src_news_df)

        # 3. separate updated records dataframe 
        # and if identity column is configured, include in in the DF

        identityColumnPattern = self.__tableConfig.identityColumnPattern

        if identityColumnPattern is None:
            src_updates_df = self._spark.sql(f'''
            SELECT src.* 
            FROM {src_view_name} AS src
            INNER JOIN {mergeDbRef}{self.__targetTableName} AS tgt ON {join_condition}
            ''')    
        else:
            insert_list = f'`{self.__identityColumn}`, {insert_list}'
            insert_values = f'src.`{self.__identityColumn}`, {insert_values}'
            src_updates_df = self._spark.sql(f'''
            SELECT tgt.{self.__identityColumn}, src.* 
            FROM {src_view_name} AS src
            INNER JOIN {self.__targetTableName} AS tgt ON {join_condition}
            ''')
        
        # 4. create an unified dataframe
        
        # print('news_df')
        # news_df.printSchema()
        # print('src_updates_df')
        # src_updates_df.printSchema()

        uni_df = news_df.union(src_updates_df)

        # get the target table size

        tableBytes = self._spark.sql(f'describe detail {mergeDbRef}{self.__targetTableName}').select("sizeInBytes").collect()[0][0]
        tableGB = tableBytes / 1024 / 1024 / 1024
        lgr.info(f'Target GB: {tableGB}')

        largetTableModeEnabled =  self.__tableConfig.largeTableMethodEnabled
        sourceSizeThreshold = self.__tableConfig.largeTableMethodSourceThresholdGB
        targetSizeThreshold = self.__tableConfig.largeTableMethodDestinationThresholdGB

        # to find out the source data sie, we need to save the source dataframe to a table
        # only do this if the target table size threshold is crossed
        # and the large table method is enabled

        if tableGB >= targetSizeThreshold and largetTableModeEnabled:

            lgr.info(f'writing to {src_temp_table_name_withid}')
            
            uni_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_temp_table_name_withid}')
            
            # waiting for the new table to "come online"
            time.sleep(10)

            #uni_df.createOrReplaceTempView(src_temp_table_name_withid)
            input_row_count = uni_df.count()
            lgr.info(f'input count {input_row_count}')

            inputBytes = self._spark.sql(f'describe detail {src_temp_table_name_withid}').select("sizeInBytes").collect()[0][0]
            inputGB = inputBytes / 1024 / 1024 / 1024
            lgr.info(f'Source GB: {inputGB}')
            # delete_src_temp_table_name_withid = True
        else:
            inputBytes = 0
            uni_df.createOrReplaceTempView(src_temp_table_name_withid)

        lgr.info('checking for duplicates')
        duplicates_check = self._spark.sql(f'''
        SELECT {key_col_list}, COUNT(*) FROM {src_temp_table_name_withid}
        GROUP BY {key_col_list}
        HAVING COUNT(*) > 1
        ''')
        if duplicates_check.count() > 0:
            display(duplicates_check)
            raise Exception(f'There are duplicate records about to be merged into {self.__targetTableName}')

        #mergeDbRef = f'{self.__lhMeta.lakehouseName}.' 
        
        update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
        scd1_update = f"WHEN MATCHED THEN UPDATE SET \
            {update_list} "

        if tableGB < targetSizeThreshold or inputGB < sourceSizeThreshold:
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
            self._spark.sql(merge_sql_update_basic)

            lgr.info('merge insert')
            lgr.info(merge_sql_insert_basic)
            self._spark.sql(merge_sql_insert_basic)
            
            self._spark.sql(f'DROP TABLE IF EXISTS {src_temp_table_name_withid}')

            # merge_sql = f"MERGE INTO {mergeDbRef}{self.__targetTableName} AS tgt \
            # USING {src_view_name}  AS src \
            # ON {join_condition} \
            # {scd1_update}\
            # WHEN NOT MATCHED THEN INSERT ( \
            # {insert_list} \
            # ) VALUES ( \
            # {insert_values} \
            # )\
            # "
            # self.__logger.info("----SCD1 MERGE SQL")
            # self.__logger.info(merge_sql)
            # self._spark.sql(merge_sql)

            return
        
        msg = 'large table mrege strategy'
        lgr.info(msg)
        
        tgt_temp_table_name = f'temp_scd_tgt_{self.__targetTableName}'
        lgr.info(f'pooling rows for update to {tgt_temp_table_name}')
        tgt_df_query = f'''
        SELECT tgt.* 
        FROM {src_temp_table_name_withid} AS src
        INNER JOIN {self.__targetTableName} AS tgt ON {join_condition}
        '''

        lgr.info(tgt_df_query)
        tgt_df = self._spark.sql(tgt_df_query)

        tgt_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{tgt_temp_table_name}')

        # waiting for the new tables to "come online"
        time.sleep(10)

        
        df_tgt_temp = self._spark.read.format('delta').load(f'Tables/{tgt_temp_table_name}')
        lgr.info(f'tgt temp count {df_tgt_temp.count()}')
        

        # 5. merge update and insert to the temp destination from source view
        
        merge_sql_update_temp = f"MERGE INTO {mergeDbRef}{tgt_temp_table_name} AS tgt \
                    USING {src_temp_table_name_withid}  AS src \
                    ON {join_condition} \
                    {scd1_update}\
                    "
        lgr.info('merge update temp')
        lgr.info(merge_sql_update_temp)
        self._spark.sql(merge_sql_update_temp)

        merge_sql_insert_temp = f"MERGE INTO {mergeDbRef}{tgt_temp_table_name} AS tgt \
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
        FROM {self.__targetTableName} AS tgt
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
        df_full.write.mode("overwrite").format("delta").save(self.__tableLocation)

        self._spark.sql(f'DROP TABLE IF EXISTS {src_temp_table_name_withid}')
        self._spark.sql(f'DROP TABLE IF EXISTS {tgt_temp_table_name}')

# ################





#         ############################

#         all_columns = self.__data.columns
#         key_columns = list(map(lambda x: self.__sanitize_column_name(x), self.__key_columns))
#         non_key_columns = self.__list_diff(self.__list_diff(all_columns, key_columns), [self.__identity_column, self.__insert_date_column, self.__audit_column])
        
#         # print('key columns')
#         # print(key_columns)
#         # print('non-key columns')
#         # print(non_key_columns)
        
#         if len(key_columns) == 0:
#             raise Exception('No key columns set for merge increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')

#         join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])
#         key_col_list = ", ".join([f"`{item}`" for item in key_columns])
#         insert_list = ", ".join([f"`{item}`" for item in all_columns])
#         insert_values = ", ".join([f"src.`{item}`" for item in all_columns])
        
        
#         # table consisisting only of key columns - no need for the update clause
#         if len(non_key_columns) == 0:
#             scd1_update = ""

#         src_view_name = f"src_{self.__target_table_name}"
#         src_temp_table_name_withid = f"temp_src_{self.__target_table_name}_withid"
#         #src_view_name_withid_part = f"src_{self.__target_table_name}_withid_part"
#         self.__data.createOrReplaceTempView(src_view_name)
        
#         src_news_df = spark.sql(f'''
#         SELECT src.* 
#         FROM {src_view_name} AS src
#         LEFT JOIN {self.__target_table_name} AS tgt ON {join_condition}
#         WHERE tgt.`{key_columns[0]}` IS NULL
#         ''')
#         news_df = self.__insert_identity_column(src_news_df)
        

#         #print(f'IC {self.__identity_column}')
#         #display(news_df)

#         if self.__identity_column_pattern is None:
#             src_updates_df = spark.sql(f'''
#             SELECT src.* 
#             FROM {src_view_name} AS src
#             INNER JOIN {self.__target_table_name} AS tgt ON {join_condition}
#             ''')    
#         else:
#             self.__identity_column = self.__identity_column_pattern.format(table_name = self.__target_table_name)
#             insert_list = f'`{self.__identity_column}`, {insert_list}'
#             insert_values = f'src.`{self.__identity_column}`, {insert_values}'
#             src_updates_df = spark.sql(f'''
#             SELECT tgt.{self.__identity_column}, src.* 
#             FROM {src_view_name} AS src
#             INNER JOIN {self.__target_table_name} AS tgt ON {join_condition}
#             ''')
        
#         uni_df = news_df.union(src_updates_df)

#         tableBytes = spark.sql(f'describe detail {self.__target_table_name}').select("sizeInBytes").collect()[0][0]
#         tableGB = tableBytes / 1024 / 1024 / 1024
#         print(f'Target GB: {tableGB}')

#         delete_src_temp_table_name_withid = False
#         delete_tgt_temp_table_name = False

#         if tableGB >= 20:

#             print(f'writing to {src_temp_table_name_withid}')
            
#             uni_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(f'Tables/{src_temp_table_name_withid}')
            
#             #uni_df.createOrReplaceTempView(src_temp_table_name_withid)
#             input_row_count = uni_df.count()
#             print(f'input count {input_row_count}')

#             inputBytes = spark.sql(f'describe detail {src_temp_table_name_withid}').select("sizeInBytes").collect()[0][0]
#             inputGB = inputBytes / 1024 / 1024 / 1024
#             print(f'Source GB: {inputGB}')
#             delete_src_temp_table_name_withid = True
#         else:
#             inputBytes = 0
#             uni_df.createOrReplaceTempView(src_temp_table_name_withid)

            
            

#         print('checking for duplicates')
#         duplicates_check = spark.sql(f'''
#         SELECT {key_col_list}, COUNT(*) FROM {src_temp_table_name_withid}
#         GROUP BY {key_col_list}
#         HAVING COUNT(*) > 1
#         ''')
#         if duplicates_check.count() > 0:
#             display(duplicates_check)
#             raise Exception(f'There are duplicate records about to be merged into {self.__target_table_name}')

#         #mergeDbRef = f'{self.__lhMeta.lakehouseName}.' 
#         merge_db_ref = f'' 

        

#         update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
#         scd1_update = f"WHEN MATCHED THEN UPDATE SET \
#             {update_list} "


    def __snapshotTarget(self):
        # first delete the snapshot to be replaced
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.snapshotKeyColumns))
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for snapshot increment. Please set the snapshotKeyColumns property in destinationTable configuration to the list of column names.')
        
        join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])

        join_column_list = ", ".join([f"`{item}`" for item in key_columns])
        
        src_view_name = f"src_{self.__lhMeta.lakehouseName}_{self.__targetTableName}"
        self.__data.createOrReplaceTempView(src_view_name)
        mergeDbRef = f'{self.__lhMeta.lakehouseName}.'

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
        #fileExists = notebookutils.mssparkutils.fs.exists(self.__tableLocation)
        #return fileExists
        
        df_tables = self._spark.sql('SHOW TABLES')
        df_f = df_tables.filter(lower('tableName') == self.__targetTableName.lower())
        if df_f.count() > 0:
            return True
        return False

        #  dstLh = self.__config.destinationStorage.destinationLakehouse
        #  dstWs = self.__config.destinationStorage.destinationWorkspace
        #  p = f'Tables/{self.__targetTableName}'
        #  print(f'Looking for table {p}')
        #  fileExists = fsUtils.fileExists(path = p, lakehouse = dstLh, workspace = dstWs)
         
        #  if fileExists:
        #      print('exists')
        #  else:
        #      print('does not exist')
    
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
        
        max_watermark_sql = f'SELECT MAX(`{watermarkColumn}`) FROM {self.__lhMeta.lakehouseName}.{self.__targetTableName}'
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
        # print(filter)
        # print(self.__data.count())