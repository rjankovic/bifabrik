"""
Delta table utilities - add / rename / remove columns
"""

import pyspark.sql.session as pss
from pyspark.sql.functions import lit, col
import bifabrik.utils.fsUtils as fsu
from datetime import datetime

spark = pss.SparkSession.builder.getOrCreate()

def addTableColumnFromValue(databaseName: str, tableName: str, columnName: str, value):
    """Adds a new column to the delta table with the provided value.
    The `database` parameter refers to a lakehouse in the current workspace
    """
    # find out the data type
    df = spark.createDataFrame([('0','0')], ['____', '_____'])
    df = df.withColumn(columnName, lit(value))
    dts = df.dtypes
    dfColT = dts[2]
    type_name = dfColT[1]
    alterQuery = f'ALTER TABLE `{databaseName}`.`{tableName}` ADD columns (`{columnName}` {type_name})'
    updateQuery = f'UPDATE `{databaseName}`.`{tableName}` SET `{columnName}` = CAST(\'{value}\' AS {type_name})'

    print(alterQuery)
    spark.sql(alterQuery)
    print(updateQuery)
    spark.sql(updateQuery)

def addTableColumnFromType(databaseName: str, tableName: str, columnName: str, typeName: str):
    """Adds a new empty column to the delta table.
    The `database` parameter refers to a lakehouse in the current workspace
    """
    alterQuery = f'ALTER TABLE `{databaseName}`.`{tableName}` ADD columns (`{columnName}` {typeName})'
    print(alterQuery)
    spark.sql(alterQuery)

def renameTableColumn(databaseName: str, tableName: str, columnName: str, newColumnName: str):
    """Renames a column in a delta table"""
    alterPropsQuery = f'''ALTER TABLE `{databaseName}`.`{tableName}` SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
    )
    '''
    print(alterPropsQuery)
    spark.sql(alterPropsQuery)

    alterQuery = f'ALTER TABLE `{databaseName}`.`{tableName}` RENAME COLUMN `{columnName}` TO `{newColumnName}`'
    print(alterQuery)
    spark.sql(alterQuery)

def dropTableColumn(databaseName: str, tableName: str, columnName: str):
    """Drops a column from a delta table"""
    alterPropsQuery = f'''ALTER TABLE `{databaseName}`.`{tableName}` SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
    )
    '''
    print(alterPropsQuery)
    spark.sql(alterPropsQuery)

    alterQuery = f'ALTER TABLE `{databaseName}`.`{tableName}` DROP COLUMN `{columnName}`'
    print(alterQuery)
    spark.sql(alterQuery)


def listTables():
    """List table names in the current lakehouse"""
    currentLh = fsu.currentLakehouse()
    if currentLh is None:
        return []
    
    df = spark.sql('SHOW TABLES')
    df = df.filter('isTemporary = False').select('tableName')
    l = list(map(lambda x: x.tableName, df.collect()))

    dfw = spark.sql('SHOW VIEWS')
    lw = list(map(lambda x: x.viewName, dfw.collect()))

    res = []
    for element in l:
        if element not in lw:
            res.append(element)
    
    return res

'''The delta table history for all tables in the list, returned as a single dataframe

Examples
--------

>>> import bifabrik.utils.tableUtils as tu
>>> 
>>> tableList = tu.listTables()
>>> th = tu.tablesHistory(tableList)
'''
def tablesHistory(tables):
    '''Lists the history for all tables in the list in one dataframe'''
    uni_df = None
    for tbl in tables:
        df = spark.sql(f'DESCRIBE HISTORY {tbl}')
        df = df.select(lit(tbl).alias("tableName"), "*")
        if uni_df is None:
            uni_df = df
        uni_df = uni_df.union(df)
    return uni_df

def restoreToPIT(tables, timestamp):
    '''Restores the tables in the current lakehouse to the point in time (their last version before the given timestamp)

    Parameters:
        tables (array[str]):    list of table names (['table1', 'table2']). You can get the list of all tables using listTables()
        timestamp:  Either a string in the format '%Y-%m-%d %H:%M:%S.%f' (UTC time) or datetime.datetime

    Examples
    --------

    >>> import bifabrik.utils.tableUtils as tu
    >>> tables = tu.listTables()
    >>> tu.restoreToPIT(tables, '2024-05-08 16:30:00')

    '''
    ts = None
    if isinstance(timestamp, datetime):
        ts = timestamp
    elif isinstance(timestamp, str):
        if '.' in timestamp:
            ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        else:
            ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    
    history = tablesHistory(tables)
    eq = history.where(f"timestamp = '{ts}'")
    if eq.count() > 0:
        raise Exception(f"There are transactions with the exact timestamp '{ts}'. Please select a point in time between the transactions.")
    
    prev = history.where(f"timestamp < '{ts}'")
    for t in tables:
        tprev = prev.filter(f"tableName = '{t}'")
        if tprev.count() == 0:
            print(f"`{t}` has no versions before {ts}; skipping")
            continue
        h = tprev.orderBy(tprev.timestamp.desc()).head()
        print(f"Restoring `{h.tableName}` to version {h.version} ({h.timestamp})")
        rsql = f"RESTORE TABLE `{h.tableName}` TO VERSION AS OF {h.version}"
        spark.sql(rsql)
        
