# Lakehouse table utilities

A few tools created as a side-effect of the `bifabrik` library for manipulating delta tables.

```python
from bifabrik.utils import tableUtils as tu
help(tu)
```
## Adding new columns

There are two options when adding new columns to a table

### New column from value

`addTableColumnFromValue` takes the target database name (lakehouse), table name, new column name and the value the column will have. The data type is infered from the value provided.

```python
from bifabrik.utils import tableUtils as tu
tu.addTableColumnFromValue('Lakehouse1', 'DimBranch', 'NewCol1', 123.45)
```

### New column from type

This adds an empty column with the provided [Spark data type](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)

```python
tu.addTableColumnFromType('LH1', 'Table1', 'Column1', 'string')
```

## Rename column

```python
from bifabrik.utils import tableUtils as tu
tu.renameTableColumn('LH1', 'FactLedger', 'OriginalColumnName', 'NewColumnName')
```

This uses SQL to alter the column name. In order to do that, it also needs to upgrade the table like this (if it already isn't "up to scratch")

```sql
ALTER TABLE `LH1`.`FactLedger` SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
    )
```

## Drop column

```python
from bifabrik.utils import tableUtils as tu
tu.dropTableColumn('LH1', 'FactLedger', 'ColumnToBeRemoved')
```
Similarly to renaming, the delta properties `minWriterVersion` and `columnMapping.mode` will be upgraded as needed.

[Back](../index.md)
