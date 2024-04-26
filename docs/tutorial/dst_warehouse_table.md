# Warehouse table destination

When loading data to a Fabric Warehouse, `bifabrik` can assist by
 - automatically creating the destination tables / adding columns when needed
 - using lakehouse tables as temporary storage for better performance
 - Implementing incremental load methods, identity columns, etc. similar to the [lakehouse table destination](dst_table.md)

## Connecting to a Fabric warehouse
While Fabric notebooks can easily connect to lakehouses, things are not so straightforward when connecting to a Fabric warehouse (a.k.a Synapse Data Warehouse). We cannot attach a warehouse to a notebook and we have to use TSQL to write any data.

`bifabrik` uses [pyodbc](https://pypi.org/project/pyodbc/) with service principal authentication to connect to warehouses. For this to work, we need to set a few things up.

### Allow service principal access

In the Power BI admin portal, under Developer settings, enable "Service principals can access Fabric APIs

![image](https://github.com/rjankovic/bifabrik/assets/2221666/ec77aeda-3076-4a45-9129-ce25890af7bc)

### Register an app and give it permissions

Next, head over to Azure and create an app registration. Also create a secret for its authentication.

Save the app's client secret value in Azure Key Vault. (`bifabrik` doesn't support direct input for credentials, just to be on the safe side.)

Make sure that this app has read and write permissions to the Fabric warehouse - either through giving the app the workspace contributor (or higher) role, or directly by setting permissions to the warehouse.

Note that you probably cannot give a service principal access to your personal workspace (the one called "My workspace" in the UI), so place your warehouse in some other workspace.

### Configure destination warehouse in bifabrik
Your configuration can look like this

```python
import bifabrik as bif

# client ID of the app registration
bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'

# key vault where you saved the app's client secret
bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'

# name of the key vault secret that contains the app's client secret
bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'

# name of the destination warehouse (you don't say!)
bif.config.destinationStorage.destinationWarehouseName = 'DW1'

# SQL connection string of the warehouse - can be found in the settings of the warehouse
bif.config.destinationStorage.destinationWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'

```

You can save this to a file for later use - [learn more about configuration](configuration.md)

### Set the default lakehouse for the notebook

Why do we need a lakehouse here? To get better performance when loading data into the warehouse. The source data is first written to a temporary delta table in the lakehouse before being inserted to the warehouse using TSQL (see [Ingest data into your Warehouse using Transact-SQL](https://learn.microsoft.com/en-us/fabric/data-warehouse/ingest-data-tsql)).

So, if you haven't already, set a default lakehouse for your notebook, where the temporary tables will be stored.

![default_lakehouse](https://github.com/rjankovic/bifabrik/assets/2221666/60951119-b0ce-40b1-8e7e-ba07b78ac06a)

## From lakehouse to warehouse

Let's say we have data in a lakehouse and want to aggregate it and save into into a warehouse. Here is one way to do that, using Spark SQL for the aggregation.

```python
import bifabrik as bif

bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'
bif.config.destinationStorage.destinationWarehouseName = 'WH_GOLD'
bif.config.destinationStorage.destinationWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'

bif.fromSql('''
SELECT countryOrRegion `CountryOrRegion`
,YEAR(date) `Year` 
,COUNT(*) `PublicHolidayCount`
FROM LH_SILVER.publicholidays
GROUP BY countryOrRegion
,YEAR(date)
''').toWarehouseTable('HolidayCountsYearly').run()
```

## UNDER CONSTRUCTION

If you just want to do a full load into a lakehouse table, go ahead:

```python
import bifabrik as bif

bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv') \
  .toTable('Survey2021').run()

# alternatively, you can use
# .toLakehouseTable('Survey2021').run()
```

This writes a parquet (delta) file into the `Tables/` directory in the lakehouse. Fabric then makes a table out of it.

If you want to save data to a different lakehouse / workspace, check out [cross-lakehouse pipelines](cfg_storage.md)

## Incremental load

The `increment` setting of the destination task has 4 options
 - overwrite
 - append
 - merge
 - snapshot

### overwrite

This is the default. It just overwrites any data in the table, including the table schema.

The example seen above is equivalent to
```python
bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv') \
  .toTable('Survey2021') \
  .increment('overwrite') \
  .run()
```

### append

Appends the incoming data to the table

```python
bif.fromCsv('CsvFiles/orders_240301.csv') \
  .toTable('FactOrder') \
  .increment('append') \
  .run()
```

You may also want to use watermark to filter the data before appending.

```python
bif.fromSql('''

SELECT orderno, inset_timestamp, value
FROM LH_Stage.vNewOrders

''').toTable('FactOrder') \
  .increment('append').watermarkColumn('inset_timestamp') \
  .run()
```
> The `watermarkColumn` configuration is a general feature of the table destination, not just for the `append` increment
>
> When `watermarkColumn` is specified, `bifabrik` checks the maximum value of that column in the destination table and then filters the incoming dataset to only load the rows where `{watermarkColumn} > {MAX(watermarkColumn) from target table}`.
>
> This is done before applying the increment logic.

### merge

This is basically the *slowly changing dimenstion type 1* option. To get this working, you need to configure the `mergeKeyColumns` array (the business key based on which to merge).

```python
bif.fromSql('''

SELECT Variable_Code
,AVG(`Value`) AS AvgValue
FROM LakeHouse1.Survey2021
GROUP BY Variable_Code

''').toTable('SCDTest1') \
  .increment('merge') \
  .mergeKeyColumns(['Variable_Code']) \
  .run()
```

Internally, Spark SQL `MERGE` statement is used to insrt new rows and update rows that get matched using the merge key. The key can have multiple columns specified in the array.

### snapshot

For this option, you need to configure the `snapshotKeyColumns` array to the column or columnss that specify the snapshot.

```python
bif.fromCsv('CsvFiles/fact_append_pt1.csv').toTable('snapshot1') \
.increment('snapshot').snapshotKeyColumns(['Date', 'Code']) \
.run()
```

Here, the snapshot is "replaced". That is, all the rows from the target table that match one of the snapshot keys in the source table get deleted, before the new rows are inserted.

This solves the *deleted rows* issue - if some rows get deleted in the source, you can remove them in the lakehouse by reloading the corresponding snapshot. The `merge` increment, by comparison, does not remove rows that were deleted in the source.

## Identity column

You can configure the `identityColumnPattern` to add an auto-increment column to the table. The name of the column can contain the name of the table / lakehouse.

```python
import bifabrik as bif

bif \
  .fromCsv('CsvFiles/orders_pt2.csv') \
  .delimiter(';') \
  .toTable('DimOrder') \
  .increment('merge') \
  .mergeKeyColumns(['Code']) \
  .identityColumnPattern('{tablename}ID') \
  .run()
```

The values are 1, 2, 3..., each new record gets a new number.

If configured, the column will be placed at the beginning of the table.

```
Supported placeholders:
{tablename}     : the delta table name
{lakehousename} : the lakehouse name
```

## Insert timestamp

You can append a column with the current timestamp to newly inserted columns - just set the `insertDateColumn` on the table destination.

If configured, this will be added at the end of the table.

```python
bif.fromCsv('CsvFiles/dimBranch.csv') \
    .toTable('DimBranch') \
    .insertDateColumn('InsertTimestamp').run()
```

## Fixing invalid column names

The characters `(space),;{}()\n\t=` are invalid in delta tables. By default, `bifabrik` replace these with and underscore (`_`) so that these invalid characters in column names don't stop you from loading your data.

You may want to just remove the invalid characters or replace them with something else. In that case, use the `invalidCharactersInColumnNamesReplacement` setting.

```python
bif.fromCsv('CsvFiles/dim_branch.csv') \
    .toTable('DimBranch') \
    .invalidCharactersInColumnNamesReplacement('').run()
```

## Adding new columns

Sometimes, we need to add new columns to an existing table. If the table is a full-load (overwrite), this is no problem - the table gets overwritten including the schema.

If there is a different increment menthod and the target table already exists, `bifabrik` will compare the structure of the target table against the incoming dataset. After this
  - if there are new columns in the input data, these columns will be added to the target table (for the old records, these columns will be empty)
  - if there are any other differences between the table schemas, `bifabrik` throws an error - deleting / renaming / changing data type of columns cannot be resolved automatically for now

This column adding feature is __enabled by default__. If you want, you can disable it like this:

```python
import bifabrik as bif

# disable adding columns for the whole session
bif.config.destinationTable.canAddNewColumns = False

# disable adding columns for a specific pipeline
bif.fromSql('''
  SELECT ShipmentId
  ,CountryCode
  ,FullName
  ,SalesType
  ,SalesTypeShortcut
  ,WarehouseId, 'ABC' AS NewColumn1 
  FROM DimBranchZ LIMIT 3
''').toTable('SchemaMerge1').increment('append') \
    .canAddNewColumns(False).run()
```
[Learn more about configuration](configuration.md)

Also, some of these [table utilities](util_table.md) can come in handy.

## Everything all at once

Just for clarity, yes, you can combine multiple settings for the table destination

```python
import bifabrik as bif

(
bif
  .fromCsv('CsvFiles/fact_append_pt2.csv')
  .toTable('snapshotTable1')
  .increment('snapshot')
  .snapshotKeyColumns(['Date', 'Code'])
  .identityColumnPattern('{tablename}ID')
  .insertDateColumn('RowStartDate')
  .run()
)
```

You can also save your configuration preferences to a JSON file and then apply it to all tables loaded in one session - read more about [configuration](configuration.md)

[Back](../index.md)
