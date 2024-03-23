"""
Delta table utilities - add / rename / remove columns
"""

import pyspark.sql.session as pss
from pyspark.sql.functions import lit, col

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