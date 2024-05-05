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

With this, a `WH_GOLD.dbo.HolidayCountsYearly` table will be created (if it doesn't exist yet) and data filled with the results of the Spark SQL query. By default, this is a full load, overwriting any previous data in the table.

We can also change the destination schema name and add an identity column:

```python
bif.fromSql('''
SELECT countryOrRegion `CountryOrRegion`
,COUNT(*) `PublicHolidayCount`
FROM LH_SILVER.publicholidays
WHERE YEAR(date) = 2024
GROUP BY countryOrRegion
''').toWarehouseTable(targetTableName = 'HolidayCounts2024', targetSchemaName = 'pbi') \
.identityColumnPattern('{tablename}Id') \
.run()
```
The schema will also be created automatically. The result can look like this:
![image](https://github.com/rjankovic/bifabrik/assets/2221666/2cfa0856-dda6-4fd7-b33b-389e9a9788d2)

To show other configuration options, use `help()` on the table destination:

```python
tableDestination = bif.fromSql('''
SELECT * FROM LH_SILVER.publicholidays
''') \
.toWarehouseTable(targetTableName = 'HolidayCounts2024', targetSchemaName = 'pbi')

help(tableDestination)
```

>When creating the warehouse table, `bifabrik` maps the Spark dataframe types to SQL types similarly to a [SQL analytics endpoint over a lakehouse](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types#autogenerated-data-types-in-the-sql-analytics-endpoint), although in a simplified fashion.
>For example, strings are stored as `VARCHAR(8000)` and dates as `DATETIME2(6)`.

## Incremental load

Incremental load has the same options as in [lakehouse table destination](dst_table.md), where it is described in details. It bears repeating here briefly, though.

### append

Also using watermark to filter the incoming data

```python
bif.fromCsv('Files/CsvFiles/fact_append_*.csv').toWarehouseTable('TransactionsTable') \
    .increment('append').watermarkColumn('Date').run()
```

### merge
```python
bif.fromCsv('Files/CsvFiles/scd_source_*.csv').toWarehouseTable('Dimension1') \
    .increment('merge').mergeKeyColumns(['Code']).identityColumnPattern('{tablename}ID').run()
```

### snapshot
```python
bif.fromCsv('CsvFiles/fact_append_*.csv').toWarehouseTable('snapshot1') \
    .increment('snapshot').snapshotKeyColumns(['Date', 'Code']) \
    .run()
```

## Identity column

The identity column can be configured the same way as for the [lakehouse table destination](dst_table.md).

## Adding the N/A (Unknown) record

When loading dimensions for a dimensional model, it's common practice to add an N/A (or "Unknown") record into your dimensions so that you can link facts to that one if your lookups fail.

To accommodate this, `bifabrik` has the `addNARecord` option (ture / false, false by default). If enabled, it adds a record to the table that has -1 in the identity column, 0 in other numeric columns and "N/A" in string columns.

If the table already has a "-1" record, this will not add another one. Also, this option is only available when you have the `identityColumnPattern` configured.

```python
bif.fromSql('''
 SELECT Variable_code, COUNT(*) ResponseCount
 FROM SurveyData
 GROUP BY Variable_code
''').toWarehouseTable('SurveyDataAgg') \
.identityColumnPattern('{tablename}ID') \
.addNARecord(True) \
.increment('merge') \
.mergeKeyColumns(['Variable_code']) \
.run()```
```

## Adding new columns

Sometimes, we need to add new columns to an existing table. If the table is a full-load (overwrite), this is no problem - when there is a change in the target schema, the table gets recreated with the new schema.

If there is a different increment menthod and the target table already exists, `bifabrik` will compare the structure of the target table against the incoming dataset. After this
  - if there are new columns in the input data, these columns will be added to the target table (for the old records, these columns will be empty - `NULL`)
  - if there are any other differences between the table schemas, `bifabrik` throws an error - deleting / renaming / changing data type of columns cannot be resolved automatically for now

This column adding feature is __enabled by default__. If you want, you can disable it like this:

```python
import bifabrik as bif

# configure warehouse connection...

# disable adding columns for the whole session
bif.config.destinationTable.canAddNewColumns = False

# disable adding columns for a specific pipeline
bif.fromSql('''
SELECT countryOrRegion `CountryOrRegion`
,COUNT(*) `PublicHolidayCount`
FROM LH_SILVER.publicholidays
WHERE YEAR(date) = 2024
GROUP BY countryOrRegion
''').toWarehouseTable(targetTableName = 'HolidayCounts20242') \
.canAddNewColumns(False) \
.run()
```

## Insert timestamp

You can append a column with the current timestamp to newly inserted columns - just set the `insertDateColumn` on the table destination.

If configured, this will be added at the end of the table.

```python
bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv').toWarehouseTable('AnnualSurvey') \
    .insertDateColumn('InsertTimestamp').identityColumnPattern('{tablename}ID').run()
```
## Warehouse to warehouse transformations

Besides this destination, there is also a [Fabric warehouse T-SQL data source](src_warehouse_sql.md). You can use [this](src_warehouse_sql.md) to run warehouse to warehouse transformations from notebooks.

[Back](../index.md)
