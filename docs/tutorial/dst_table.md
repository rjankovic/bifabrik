# Table destination

If you just want to do a full load into a lakehouse table, go ahead:

```python
import bifabrik as bif

bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv') \
  .toTable('Survey2021').run()
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
bif.fromCsv('CsvFiles/dim_branch.csv')
    .toTable('DimBranch') \
    .invalidCharactersInColumnNamesReplacement('').run()
```

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
