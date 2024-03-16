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
bif.fromCsv('CsvFiles/orders_240301.csv').toTable('FactOrder').increment('append').run()
```

You may also want to use watermark to filter the data before appending.

```python
bif.fromCsv('CsvFiles/orders.csv').toTable('FactOrder').increment('append').watermarkColumn('timestamp').run()
```
> The `watermarkColumn` configuration is a general feature of the table destination, not just for the `append` increment
>
> When `watermarkColumn` is specified, `bifabrik` checks the maximum value of that column in the destination table and then filters the incoming dataset to only load the rows where `{watermarkColumn} > {MAX(watermarkColumn) from target table}`

### merge

This is basically the *slowly changing dimenstion type 1* option. To get this working, you need to configure the `mergeKeyColumns` array (the business key based on which to merge).

```python
bif \
  .fromCsv('CsvFiles/scd_pt1.csv') \
  .toTable('SCDTest1') \
  .increment('merge') \
  .mergeKeyColumns(['Code']) \
  .run()
```

Internally, Spark SQL `MERGE` statement is used to insrt new rows and update rows that get matched using the merge key. The key can have multiple columns specified in the array.

## Identity column

## Insert timestamp

## Fixing invalid column names

Read more about [configuration](configuration.md)

[Back](../index.md)
