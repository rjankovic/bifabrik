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
import uuid
import pyodbc
import pandas as pd

# reuse the table destination configuration
class WarehouseTableDestination(DataDestination, TableDestinationConfiguration):
    """Saves data to a warehouse table.

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
        self.__data = None
        self.__config = None
        self.__lhBasePath = None
        self.__identityColumn = None
        self.__insertDateColumn = None
        self.__tableExists = False
        self.__logger = None
        self.__tempTableName = None
        self.__tempTableLocation = None
        self.__odbcConnection = None
        self.__identityColumnName = None

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
        
        # save to temp table in the lakehouse
        dstLh = mergedConfig.destinationStorage.destinationLakehouse
        dstWs = mergedConfig.destinationStorage.destinationWorkspace
        self.__lhBasePath = fsUtils.getLakehousePath(dstLh, dstWs)
        if self.__lhBasePath is None:
            raise Exception(f'''The warehouse destination needs to use a warehouse for temporary data storage. 
                            Either connect the notebook to a lakehouse or configure the destinationLakehouse and destinationWorkspace properties in bifabrik.config.destinationStorage.''')
        self.__lhMeta = fsUtils.getLakehouseMeta(dstLh, dstWs)
        self.__tempTableName = f"temp_{self.__targetTableName}_{str(uuid.uuid4())}".replace('-', '_')
        self.__tempTableLocation = self.__lhBasePath + "/Tables/" + self.__tempTableName
        self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__tempTableLocation)
        
        # connect ODBC
        odbcServer = mergedConfig.destinationStorage.destinationWarehouseConnectionString
        odbcDatabase = mergedConfig.destinationStorage.destinationWarehouseName
        odbcTimeout = mergedConfig.destinationStorage.destinationWarehouseConnectionTimeout
        principalClientId = mergedConfig.security.servicePrincipalClientId
        principalClientSecret = notebookutils.mssparkutils.credentials.getSecret(mergedConfig.security.keyVaultUrl, mergedConfig.security.servicePrincipalClientSecretKVSecretName)
        constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};database={odbcDatabase};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        self.__odbcConnection = pyodbc.connect(constr)

        # create schema if not exists
        findSchemaQuery = f"SELECT COUNT(*) FROM sys.schemas WHERE [name] = {self.__targetSchemaName}"
        findSchemaDf = self.__execute_select(findSchemaQuery)
        schemaCount = findSchemaDf[0][0]
        if schemaCount == 0:
            self.__execute_dml(f"CREATE SCHEMA [{self.__targetSchemaName}]")

        # determine column types
        destinationTableColumns = []

        # add identity column to table structure if specified
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is not None:
            self.__identityColumnName = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName)
            destinationTableColumns.append([self.__identityColumnName, 'BIGINT'])
        
        # ordinary columns...
        dts = self.__data.dtypes
        for sc in dts:
            col_type = 'VARCHAR(8000)'
            s_name = sc[0]
            s_type = sc[1]
            if s_type == 'string' or s_type.startswith('char') or s_type.startswith('varchar'):
                col_type = 'VARCHAR(8000)'
            elif s_type == 'long' or s_type == 'bigint':
                col_type = 'BIGINT'
            elif s_type.startswith('bool'):
                col_type = 'BIT'
            elif s_type.startswith('int'):
                col_type = 'INT'
            elif s_type == 'tinyint' or s_type == 'byte' or s_type == 'smallint' or s_type == 'short':
                col_type = 'SMALLINT'
            elif s_type == 'double':
                col_type = 'DOUBLE'
            elif s_type == 'float' or s_type == 'real':
                col_type = 'REAL'
            elif s_type == 'date' or s_type == 'timestamp':
                col_type = 'DATETIME2'
            elif s_type.startswith('binary') or s_type.startswith('byte'):
                col_type = 'VARBINARY(8000)'
            elif s_type.startswith('decimal('):
                col_type = s_type.replace('decimal', 'DECIMAL')
            
            destinationTableColumns.append([s_name, col_type])

        # add insert date column
        insertDateColumn = self.__tableConfig.insertDateColumn
        if insertDateColumn is not None:
            destinationTableColumns.append([insertDateColumn, 'DATETIME2'])
        
# CREATE TABLE Test8
# (
#  ID INT NULL,
#  CreatedDateTime VARBINARY(8000),
#  D DECIMAL(38,38),
#  VC VARCHAR(8000)
# )

#         CREATE TABLE [dbo].[bing_covid-19_data]
# (
#     [id] [int] NULL,
#     [updated] [date] NULL,
#     [confirmed] [int] NULL,
#     [confirmed_change] [int] NULL,
#     [deaths] [int] NULL,
#     [deaths_change] [int] NULL,
#     [recovered] [int] NULL,
#     [recovered_change] [int] NULL,
#     [latitude] [float] NULL,
#     [longitude] [float] NULL,
#     [iso2] [varchar](8000) NULL,
#     [iso3] [varchar](8000) NULL,
#     [country_region] [varchar](8000) NULL,
#     [admin_region_1] [varchar](8000) NULL,
#     [iso_subdivision] [varchar](8000) NULL,
#     [admin_region_2] [varchar](8000) NULL,
#     [load_time] [datetime2](6) NULL
# )


# bigint
# timestamp
# int
# double
# string
# decimal(10,0)

        # create warehouse table if not exists


        # sync schema if table exists
        # handle increments as in a lakehouse

        self.__odbcConnection.close()

        #####################

        self.__tableExists = self.__tableExistsF()

        self.__replaceInvalidCharactersInColumnNames()
        self.__insertIdentityColumn()
        self.__insertInsertDateColumn()


        incrementMethod = mergedConfig.destinationTable.increment
        if incrementMethod is None:
            incrementMethod = 'overwrite'
        self.__incrementMethod = incrementMethod

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

    def __execute_select(self, query: str):
        pd_df = pd.read_sql(query, self.__odbcConnection)
        df = self._spark.createDataFrame(pd_df)
        return df
    
    def __execute_dml(self, query: str):
        self.__odbcConnection.execute(query)
        self.__odbcConnection.commit()
    
    
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
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is None:
            return
        
        lhName = self.__lhMeta.lakehouseName
        initID = 0
        identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, lakehousename = lhName)
        self.__identityColumn = identityColumn

        if self.__tableExists:
            query = f"SELECT MAX({identityColumn}) FROM {lhName}.{self.__targetTableName}"
            initID = self._spark.sql(query).collect()[0][0]
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
        compare = (
            cols_new.join(cols_old, cols_new.new_name == cols_old.old_name, how="outer")
            .fillna("")
        )
        difference = compare.filter(compare.new_type != compare.old_type)

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
        all_columns = self.__data.columns
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.mergeKeyColumns))
        non_key_columns = self.__list_diff(self.__list_diff(all_columns, key_columns), [self.__identityColumn, self.__insertDateColumn])
        
        # print('key columns')
        # print(key_columns)
        # print('non-key columns')
        # print(non_key_columns)
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for merge increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')
        

        # todo: instead of SQL, use pyspark for cross-workspace ETL
        join_condition = " AND ".join([f"src.`{item}` = tgt.`{item}`" for item in key_columns])
        update_list = ", ".join([f"`{item}` = src.`{item}`" for item in non_key_columns])
        insert_list = ", ".join([f"`{item}`" for item in all_columns])
        insert_values = ", ".join([f"src.`{item}`" for item in all_columns])
        
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
         fileExists = notebookutils.mssparkutils.fs.exists(self.__tableLocation)
         
        #  dstLh = self.__config.destinationStorage.destinationLakehouse
        #  dstWs = self.__config.destinationStorage.destinationWorkspace
        #  p = f'Tables/{self.__targetTableName}'
        #  print(f'Looking for table {p}')
        #  fileExists = fsUtils.fileExists(path = p, lakehouse = dstLh, workspace = dstWs)
         
        #  if fileExists:
        #      print('exists')
        #  else:
        #      print('does not exist')
         return fileExists
    
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