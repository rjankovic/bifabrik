# Lakehouse table destination

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

> For larger tables, the SparkSQL `MERGE` implementation can run into memory issues. Therefore, if both the destination table and source dataset cross a certain threshold, `bifabrik` wiil, by default, use a step-by-step method of merging.
> First, it will identify the records that will be affected by the merge. These are then copied to a temporary table and the merge is performed there so that the whole table doesn't need to be part of the merge operation.
> Finally, the data is copied back to the original destination table.
>
> To check or change the settings, use the following properties
> ```python
> print(bif.config.destinationTable.largeTableMethodEnabled) # default True
> print(bif.config.destinationTable.largeTableMethodSourceThresholdGB) # default 0.2
> print(bif.config.destinationTable.largeTableMethodDestinationThresholdGB) # default 20
> ```

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

## Adding the N/A (Unknown) record

When loading dimensions for a dimensional model, it's common practice to add an N/A (or "Unknown") record into your dimensions so that you can link facts to that one if your lookups fail.

To accommodate this, `bifabrik` has the `addNARecord` option (ture / false, false by default). If enabled, it adds a record to the table that has -1 in the identity column, 0 in other numeric columns and "N/A" in string columns.

If the table already has a "-1" record, this will not add another one. Also, this option is only available when you have the `identityColumnPattern` configured.

```python
bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-*.csv') \
.toTable('SurveyData') \
.identityColumnPattern('{tablename}ID') \
.addNARecord(True) \
.run()
```

The result can look like this

![image](https://github.com/rjankovic/bifabrik/assets/2221666/3556a20b-4eb7-4b85-9f50-29d1bf341f3d)

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

An example combining different aspects of the configuration

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
