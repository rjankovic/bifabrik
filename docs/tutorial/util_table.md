# Lakehouse table utilities

A few tools created as a side-effect of the `bifabrik` library for manipulating delta tables.

```python
from bifabrik.utils import tableUtils as tu
help(tu)
```

## Restore tables to to earlier version

These tools makes use of time travel in delta tables and the [RESTORE command](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-restore), making it easier to perform batch restore operations.

### List lakehouse tables

```python
import bifabrik.utils.tableUtils as tu
tableList = tu.listTables()

# > ['dimension_date', 'dimension_employee', 'dimension_stock_item', 'fact_sale']
```
`listTables()` simply lists the names of the tables in the default lakehouse for use by subsequent functions

### Show tables history

As a next step, let's have a look at the history of our tables using `tablesHistory({list of table names})`

```python
import bifabrik.utils.tableUtils as tu

tableList = tu.listTables()
history = tu.tablesHistory(tableList)
display(history)
```

The result can look like this

![image](https://github.com/rjankovic/bifabrik/assets/2221666/20552a54-d67c-446b-a72f-b95b967ef34d)

This is basically the union of [DESCRIBE HISTORY](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-describe-history) executed for each of the tables

### Restore tables to a point in time

Finally, restore the tables to the desired point in time. For each table, their latest version before the given time is taken. If the table has no history before that point, it is skipped.

```python
tableList = tu.listTables()
# filter a subset of the tables as needed

tu.restoreToPIT(tableList, '2024-05-08 14:00:00')

# All restores or table skips are printed out like this:
#
# > Restoring `dimension_employee` to version 1 (2024-05-08 12:32:51.325000)
# > Restoring `dimension_stock_item` to version 1 (2024-05-08 12:34:15.257000)
# > Restoring `fact_sale` to version 3 (2024-05-08 13:42:00.739000)
# > ...
```

## Add new column

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
