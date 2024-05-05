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
import bifabrik.dst.CommonDestinationUtils as commons

# reuse the table destination configuration
class WarehouseTableDestination(DataDestination, TableDestinationConfiguration):
    """Saves data to a warehouse table. Uses pyodbc with service principal authentication, which needs to be configured.
    See https://rjankovic.github.io/bifabrik/tutorial/dst_warehouse_table.html

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
    > bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
    > bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'
    > bif.config.destinationStorage.destinationWarehouseName = 'WH_GOLD'
    > bif.config.destinationStorage.destinationWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'
    >
    > bif.fromSql('''
    > SELECT countryOrRegion `CountryOrRegion`
    > ,YEAR(date) `Year` 
    > ,COUNT(*) `PublicHolidayCount`
    > FROM LH_SILVER.publicholidays
    > GROUP BY countryOrRegion
    > ,YEAR(date)
    > ''').toWarehouseTable('HolidayCountsYearly').run()
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
        self.__watermarkColumn = None
        self.__addNARecord = False

    def __str__(self):
        return f'Warehouse table destination: [{self.__targetSchemaName}].[{self.__targetTableName}]'
    
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
        self.__databaseName = mergedConfig.destinationStorage.destinationWarehouseName

        incrementMethod = mergedConfig.destinationTable.increment
        if incrementMethod is None:
            incrementMethod = 'overwrite'
        self.__incrementMethod = incrementMethod
        
        # connect ODBC
        odbcServer = mergedConfig.destinationStorage.destinationWarehouseConnectionString
        odbcDatabase = mergedConfig.destinationStorage.destinationWarehouseName
        odbcTimeout = mergedConfig.destinationStorage.destinationWarehouseConnectionTimeout
        principalClientId = mergedConfig.security.servicePrincipalClientId
        
        if odbcServer is None:
            raise Exception('Cannot connect to warehouse - destinationStorage.destinationWarehouseConnectionString not configured (copy this from the properties of your warehouse)')
        if odbcDatabase is None:
            raise Exception('Cannot connect to warehouse - destinationStorage.destinationWarehouseName not configured')
        if principalClientId is None:
            raise Exception('Cannot connect to warehouse - security.servicePrincipalClientId not configured (service principal authentication is used)')
        if mergedConfig.security.keyVaultUrl is None:
            raise Exception('Cannot connect to warehouse - security.keyVaultUrl not configured (service principal authentication is used and the client secret needs to be stored in key vault)')
        if mergedConfig.security.servicePrincipalClientSecretKVSecretName is None:
            raise Exception('Cannot connect to warehouse - security.servicePrincipalClientSecretKVSecretName not configured (service principal authentication is used and the client secret needs to be stored in key vault)')
        
        principalClientSecret = notebookutils.mssparkutils.credentials.getSecret(mergedConfig.security.keyVaultUrl, mergedConfig.security.servicePrincipalClientSecretKVSecretName)
        constr = f"driver=ODBC Driver 18 for SQL Server;server={odbcServer};database={odbcDatabase};UID={principalClientId};PWD={principalClientSecret};Authentication=ActiveDirectoryServicePrincipal;Encrypt=yes;Timeout={odbcTimeout};"
        self.__odbcConnection = pyodbc.connect(constr)

        # create schema if not exists
        findSchemaQuery = f"SELECT COUNT(*) FROM sys.schemas WHERE [name] = '{self.__targetSchemaName}'"
        findSchemaDf = self.__execute_select(findSchemaQuery)
        schemaCount = findSchemaDf[0][0]
        if schemaCount == 0:
            self.__execute_dml(f"CREATE SCHEMA [{self.__targetSchemaName}]")

        # check if table exists
        findTableDf = self.__execute_select(f'''
        SELECT COUNT(*)
        FROM sys.tables t 
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{self.__targetSchemaName}' AND t.name = '{self.__targetTableName}'
        ''')

        tableCount = findTableDf[0][0]
        if tableCount == 0:
            self.__tableExists = False
        else:
            self.__tableExists = True


        # watermark filter filters self.__data DF - this has to be done before thedata is written to the lakehouse temp table
        self.__filterByWatermark()
        # the same replacement as in a lakehouse needs to be applied to the column in order to save the temp table to the lakehouse
        self.__replaceInvalidCharactersInColumnNames()
        

        # save to temp table in the lakehouse
        dstLh = mergedConfig.destinationStorage.destinationLakehouse
        dstWs = mergedConfig.destinationStorage.destinationWorkspace
        self.__lhBasePath = fsUtils.getLakehousePath(dstLh, dstWs)
        if self.__lhBasePath is None:
            raise Exception(f'''The warehouse destination needs to use a warehouse for temporary data storage. 
                            Either connect the notebook to a lakehouse or configure the destinationLakehouse and destinationWorkspace properties in bifabrik.config.destinationStorage.''')
        self.__lhMeta = fsUtils.getLakehouseMeta(dstLh, dstWs)
        self.__destinationLakehouse = self.__lhMeta.lakehouseName
        self.__tempTableName = f"temp_{self.__targetTableName}_{str(uuid.uuid4())}".replace('-', '_')
        self.__tempTableLocation = self.__lhBasePath + "/Tables/" + self.__tempTableName
        if dstLh is None:
            self.__tempTableLocation = "Tables/" + self.__tempTableName
        self.__data.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(self.__tempTableLocation)
        # sleep for 3 s until the lakehouse table comes online
        # LESS THAN IDEAL
        time.sleep(3)

        # determine column types
        inputTableColumns = []

        # add identity column to table structure if specified
        identityColumnPattern = self.__tableConfig.identityColumnPattern
        if identityColumnPattern is not None:
            self.__identityColumn = self.__tableConfig.identityColumnPattern.format(tablename = self.__targetTableName, databasename = self.__databaseName)
            inputTableColumns.insert(0, [self.__identityColumn, 'BIGINT'])
        
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
                col_type = 'FLOAT'
            elif s_type == 'float' or s_type == 'real':
                col_type = 'FLOAT'
            elif s_type == 'date' or s_type == 'timestamp':
                col_type = 'DATETIME2(6)'
            elif s_type.startswith('binary') or s_type.startswith('byte'):
                col_type = 'VARBINARY(8000)'
            elif s_type.startswith('decimal('):
                col_type = s_type.replace('decimal', 'DECIMAL')
            
            inputTableColumns.append([s_name, col_type])

        # add insert date column
        insertDateColumn = self.__tableConfig.insertDateColumn
        if insertDateColumn is not None:
            self.__insertDateColumn = insertDateColumn
            inputTableColumns.append([self.__insertDateColumn, 'DATETIME2(6)'])
        
        self.__tableColumns = inputTableColumns
        
        # create warehouse table if not exists        
        columnDefs = map(lambda x: f'{x[0]} {x[1]} NULL', inputTableColumns)
        column_defs_join = ',\n'.join(columnDefs)
        createTableSql = f'''
CREATE TABLE [{self.__targetSchemaName}].[{self.__targetTableName}](
{column_defs_join}
)
'''
        if not self.__tableExists:
            self.__execute_dml(createTableSql)
            self.__insertNARecord()
            self.__append_target()
            drop_temp_sql = f'DROP TABLE IF EXISTS `{self.__destinationLakehouse}`.`{self.__tempTableName}`'
            self._spark.sql(drop_temp_sql)
            self.__odbcConnection.close()
            self._completed = True
            return

        # sync schema if table exists
        origColumnsTypesDf = self.__get_current_table_structure()

        consistent_changes = True
        schema_change = False

        if len(origColumnsTypesDf) < len(inputTableColumns):
            schema_change = True

        broken_column_name = None
        broken_column_type = None
        type_error = None

        # find orig columns in new columns (added columns are ok)
        for orig_col in origColumnsTypesDf:
            orig_col_name = orig_col.column_name
            orig_col_type = orig_col.type_name.upper()
            for input_col in inputTableColumns:
                match_found = False
                if input_col[0] == orig_col_name:
                    match_found = True
                    input_type = input_col[1]
                    # type change - nogo
                    input_type_base = input_type.replace('(6)', '').replace('(8000)', '')
                    if input_type_base != orig_col_type:
                        consistent_changes = False
                        schema_change = True
                        broken_column_name = orig_col_name
                        broken_column_type = orig_col_type
                        type_error = f'type does not match source type {input_type}'
                        break
                    # decimal - check the precision and scale
                    if orig_col_type == 'DECIMAL':
                        orig_precision = orig_col.precision
                        orig_scale = orig_col.scale
                        input_decimal_mid = input_type.replace('DECIMAL(', '').replace(')', '').split(',')
                        input_decimal_precision = int(input_decimal_mid[0])
                        input_decimal_scale = int(input_decimal_mid[1])
                        if orig_precision != input_decimal_precision or orig_scale != input_decimal_scale:
                            consistent_changes = False
                            schema_change = True
                            broken_column_name = orig_col_name
                            broken_column_type = orig_col_type
                            type_error = f'precision or scale ({orig_precision},{orig_scale}) does not match source type {input_type}'
                            break
                    # no type difference found - go to next orig table column
                    break
            if not match_found:
                schema_change = True
                consistent_changes = False
                broken_column_name = orig_col_name
                broken_column_type = orig_col_type
                type_error = 'column deleted'
                # deleted or renamed column - nogo
                break
            if consistent_changes == False:
                break

        if consistent_changes == False:
            # problematic schema changes, but it's overwrite, so just drop and create the table
            if incrementMethod == 'overwrite':
                dropTableSql = f'''
DROP TABLE [{self.__targetSchemaName}].[{self.__targetTableName}]
'''
                lgr.info('Recreating the table due to schema changes')
                self.__execute_dml(dropTableSql)
                self.__execute_dml(createTableSql)
                self.__append_target()
                self.__odbcConnection.close()
                self._completed = True
                return
            
            raise Exception(f'Cannot resolve schema chnages in table [{self.__targetSchemaName}].[{self.__targetTableName}], column {broken_column_name}({broken_column_type}) - {type_error}')
            
        if schema_change:
            canAddColumns = self.__tableConfig.canAddNewColumns
            if not canAddColumns:
                raise Exception(f'Cannot resolve schema chnages in destination table [{self.__targetSchemaName}].[{self.__targetTableName}] because the `canAddNewColumns` setting is False')
            
            msg = f'Rebuilding table [{self.__targetSchemaName}].[{self.__targetTableName}], to add new columns...'
            self.__logger.info(msg)
            print(msg)

            # create temp_old copy of the old table in the WH
            # copy data to the temp_old table
            # SELECT * INTO Dim_Customer_temp FROM Dim_Customer
            whTempTableName = f"temp_{self.__targetTableName}_{str(uuid.uuid4())}".replace('-', '_')
            selectIntoQuery = f"SELECT * INTO [{self.__targetSchemaName}].[{whTempTableName}] FROM [{self.__targetSchemaName}].[{self.__targetTableName}]"
            self.__execute_dml(selectIntoQuery)
            
            
            # DROP old table
            dropQuery = f"DROP TABLE [{self.__targetSchemaName}].[{self.__targetTableName}]"
            self.__execute_dml(dropQuery)

            # create table with new schema
            self.__execute_dml(createTableSql)

            # copy data from temp_old table to new table
            insert_columns = list(map(lambda x: f'[{x.column_name}]', origColumnsTypesDf))
            insert_columns_join = ", \n".join(insert_columns)
            insertBackQuery = f'''INSERT INTO [{self.__targetSchemaName}].[{self.__targetTableName}]
            ({insert_columns_join})
            SELECT {insert_columns_join}
            FROM [{self.__targetSchemaName}].[{whTempTableName}]
            '''
            self.__execute_dml(insertBackQuery)

            # DROP temp_old table
            dropQuery = f'DROP TABLE [{self.__targetSchemaName}].[{whTempTableName}]'
            self.__execute_dml(dropQuery)
        
        if incrementMethod != 'overwrite':
            # overwrite will handle it's N/A record by itself
            self.__insertNARecord()

        # handle increments as in a lakehouse
        if incrementMethod == 'overwrite':
            self.__overwrite_target()
        elif incrementMethod == 'append':
            self.__append_target()
        elif incrementMethod == 'merge':
            self.__merge_taget()
        elif incrementMethod == 'snapshot':
            self.__snapshot_target()
        else:
            raise Exception(f'Unrecognized increment type: {incrementMethod}')

        self.__odbcConnection.close()
        drop_temp_sql = f'DROP TABLE IF EXISTS `{self.__destinationLakehouse}`.`{self.__tempTableName}`'
        self._spark.sql(drop_temp_sql)

        self._completed = True


    def __execute_select(self, query: str):
        pd_df = pd.read_sql(query, self.__odbcConnection)
        df = self._spark.createDataFrame(pd_df)
        return df.collect()
    
    def __execute_dml(self, query: str):
        queryOneLine = query.replace('\n', ' ')
        self.__logger.info(queryOneLine)
        try:
            self.__odbcConnection.execute(query)
            self.__odbcConnection.commit()
        except Exception as e:
            if str(e.args[0]) == '42000' and e.args[1].find('the object accessed by the statement has been modified by a DDL statement in another concurrent transaction') > -1:
                warn = 'Waiting 5 s for blocking DDL to finish'
                print(warn)
                self.__logger.warning(e.args[1])
                self.__logger.warning(warn)
                time.sleep(5)
                self.__odbcConnection.execute(query)
                self.__odbcConnection.commit()
            elif str(e.args[0]) == '42S02' and e.args[1].find('Invalid object name') > -1 and e.args[1].find('temp') > -1:
                warn = 'Waiting 5 s for the lakehouse temp table to come online'
                print(warn)
                self.__logger.warning(e.args[1])
                self.__logger.warning(warn)
                time.sleep(5)
                try:
                    self.__odbcConnection.execute(query)
                    self.__odbcConnection.commit()
                except Exception as ee:
                    if str(ee.args[0]) == '42000' and ee.args[1].find('the object accessed by the statement has been modified by a DDL statement in another concurrent transaction') > -1:
                        warn = 'Waiting 5 s for blocking DDL to finish'
                        print(warn)
                        self.__logger.warning(ee.args[1])
                        self.__logger.warning(warn)
                        time.sleep(5)
                        self.__odbcConnection.execute(query)
                        self.__odbcConnection.commit()
                    else:
                        raise ee
            else:
                raise e

    def __get_current_table_structure(self):
        df = self.__execute_select(f'''
SELECT s.name schema_name, t.name table_name, c.name column_name, tt.name type_name, tt.max_length, tt.precision, tt.scale 
FROM sys.schemas s
INNER JOIN sys.tables t ON s.schema_id = t.schema_id
INNER JOIN sys.columns c ON c.object_id = t.object_id
INNER JOIN sys.types tt ON tt.system_type_id = c.system_type_id
WHERE s.name = '{self.__targetSchemaName}' AND t.name = '{self.__targetTableName}'
        ''')
        return df
    
    def __list_diff(self, first_list, second_list):
        diff = [item for item in first_list if item not in second_list]
        return diff
    
    # INSERT INTO destination_table(destination_columns) SELECT {destination_columns, including TS and ID} FROM src
    # [src] to be defined later
    def __create_insert_query(self):
        col_list = list(map(lambda x: x[0], self.__tableColumns))
        insert_sql = ''
        select_sql = ''
        for i in range(len(col_list)):
            if i > 0:
                insert_sql = insert_sql + ',\n'
                select_sql = select_sql + ',\n'
            col_name = col_list[i]
            insert_sql = insert_sql + f'[{col_name}]'
            
            if col_name == self.__identityColumn:
                init_id = 0
                if self.__incrementMethod != 'overwrite' and self.__tableExists:
                    maxIdDf = self.__execute_select(f'SELECT ISNULL(MAX([{self.__identityColumn}]), 0) AS MaxID FROM [{self.__targetSchemaName}].[{self.__targetTableName}] ')
                    init_id = maxIdDf[0][0]
                select_sql = select_sql + f'ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) + {init_id} [{col_name}]'
            elif col_name == self.__insertDateColumn:
                select_sql = select_sql + f'GETDATE() [{col_name}]'
            else:
                select_sql = select_sql + f'src.[{col_name}]'
            
        complete_sql = f'''INSERT INTO [{self.__targetSchemaName}].[{self.__targetTableName}](
{insert_sql}        
)
SELECT
{select_sql}
FROM src
'''
        return complete_sql
    
    def __append_target_query(self, src_query):
        insert_query = self.__create_insert_query()
        append_sql = f'''WITH
src AS ({src_query}
)
{insert_query}
'''
        self.__execute_dml(append_sql)

    def __insertNARecord(self):        
        self.__addNARecord = self.__tableConfig.addNARecord
        if not self.__addNARecord:
            return
        if self.__identityColumn is None:
            raise Exception('Configuration error - when addNARecord is enabled, identityColumnPattern needs to be configured as well.')
                
        # IF NOT EXISTS(SELECT TOP 1 1 FROM [dbo].[SurveyData] WHERE [SurveyDataID] = -1)
        # BEGIN
        #     INSERT INTO [dbo].[SurveyData] ([SurveyDataID], [Year], [Industry_aggregation_NZSIOC])
        #     VALUES(-1, 0, 'N/A')
        # END

        columnsTypesDf = self.__get_current_table_structure()

        insert_list = []
        value_list = []

        for c in columnsTypesDf:
            col_name = c.column_name
            col_type = c.type_name.upper()
            insert_list.append(col_name)
            if col_name == self.__identityColumn:
                value_list.append('-1')
            elif col_type in ['INT', 'BIGINT', 'SMALLINT', 'BIT']:
                value_list.append('0')
            elif col_type in ['REAL', 'FLOAT']:
                value_list.append('0.0')
            elif col_type in ['DATETIME', 'DATETIME2']:
                value_list.append("'2000-01-01'")
            elif col_type in ['VARCHAR', 'CHAR']:
                value_list.append("'N/A'")
        
        insert_columns = list(map(lambda x: f'[{x}]', insert_list))
        insert_columns_join = ", \n".join(insert_columns)
        values_join = ", \n".join(value_list)

        insert_query = f'''
IF NOT EXISTS(SELECT TOP 1 1 FROM [{self.__targetSchemaName}].[{self.__targetTableName}] WHERE [{self.__identityColumn}] = -1)
BEGIN
    INSERT INTO [{self.__targetSchemaName}].[{self.__targetTableName}] ({insert_columns_join})
    VALUES({values_join})
END
'''
        self.__execute_dml(insert_query)
    
    def __append_target(self):
        src_query = f'SELECT * FROM [{self.__destinationLakehouse}].[dbo].[{self.__tempTableName}]'
        self.__append_target_query(src_query)
    
    def __overwrite_target(self):
        delte_sql = f'DELETE FROM [{self.__targetSchemaName}].[{self.__targetTableName}]'
        self.__execute_dml(delte_sql)
        self.__insertNARecord()
        self.__append_target()
    
    def __merge_taget(self):
        all_columns = list(map(lambda x: x[0], self.__tableColumns))
        key_columns = self.__tableConfig.mergeKeyColumns
        non_key_columns = self.__list_diff(self.__list_diff(all_columns, key_columns), [self.__identityColumn, self.__insertDateColumn])
        
        # print('key columns')
        # print(key_columns)
        # print('non-key columns')
        # print(non_key_columns)
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for merge increment. Please set the mergeKeyColumns property in destinationTable configuration to the list of column names.')
        
        join_condition = " AND ".join([f"src.[{item}] = tgt.[{item}]" for item in key_columns])
        
        if len(non_key_columns) > 0:
            update_list = ",\n".join([f"tgt.[{item}] = src.[{item}]" for item in non_key_columns])
            update_query = f'''
UPDATE tgt SET
{update_list}
FROM [{self.__destinationLakehouse}].[dbo].[{self.__tempTableName}] src
INNER JOIN [{self.__targetSchemaName}].[{self.__targetTableName}] tgt ON {join_condition}
'''
            self.__execute_dml(update_query)

        insert_src_query = f'''
SELECT src.*
FROM [{self.__destinationLakehouse}].[dbo].[{self.__tempTableName}] src
LEFT JOIN [{self.__targetSchemaName}].[{self.__targetTableName}] tgt ON {join_condition}
WHERE tgt.{key_columns[0]} IS NULL
'''
        self.__append_target_query(insert_src_query)

    def __snapshot_target(self):
        # first delete the snapshot to be replaced
        key_columns = list(map(lambda x: self.__sanitizeColumnName(x), self.__tableConfig.snapshotKeyColumns))
        
        if len(key_columns) == 0:
            raise Exception('No key columns set for snapshot increment. Please set the snapshotKeyColumns property in destinationTable configuration to the list of column names.')
        
        join_condition = " AND ".join([f"src.[{item}] = tgt.[{item}]" for item in key_columns])
        delete_query = f'''
DELETE tgt FROM [{self.__targetSchemaName}].[{self.__targetTableName}] tgt
INNER JOIN [{self.__destinationLakehouse}].[dbo].[{self.__tempTableName}] src ON {join_condition}
'''
        self.__execute_dml(delete_query)
        self.__append_target()

        
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
        

        ###############
        # DDL conflict would occur when using watermark before introducing the 5s retry in __execute_dml
        #
        # pyodbc.ProgrammingError: ('42000', "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Snapshot isolation transaction failed in database 'LH_exp' because the object accessed by the statement has been modified by a DDL statement in another concurrent transaction since the start of this transaction.  It is disallowed because the metadata is not versioned. A concurrent update to metadata can lead to inconsistency if mixed with snapshot isolation. (3961) (SQLExecDirectW)")
        #
        #     361         insert_query = self.__create_insert_query()
        #     362         append_sql = f'''WITH
        #     363 src AS ({src_query}
        #     364 )
        #     365 {insert_query}
        #     366 '''
        # --> 367         self.__execute_dml(append_sql)
        ###############

        self.__watermarkColumn = watermarkColumn
        max_watermark_sql = f'SELECT COALESCE(MAX([{watermarkColumn}]), \'\') FROM [{self.__targetSchemaName}].[{self.__targetTableName}]'
        # print(max_watermark_sql)
        max_watermark = self.__execute_select(max_watermark_sql)[0][0]
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
    
    def __sanitizeColumnName(self, colName):
        replacement = self.__tableConfig.invalidCharactersInColumnNamesReplacement
        if replacement is None:
            return colName
        
        invalids = " ,;{}()\n\t="
        name = colName        
        for i in invalids:
            name = name.replace(i, replacement)
        return name
    
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