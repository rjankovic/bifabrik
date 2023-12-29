# bifabrik

[Bifabrik Index](../../README.md#bifabrik-index) /
`src` /
[Bifabrik](./index.md#bifabrik) /
bifabrik

> Auto-generated documentation for [src.bifabrik.bifabrik](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/bifabrik.py) module.

- [bifabrik](#bifabrik)
  - [bifabrik](#bifabrik-1)
    - [bifabrik().fromCsv](#bifabrik()fromcsv)
    - [bifabrik().fromJson](#bifabrik()fromjson)
    - [bifabrik().fromSql](#bifabrik()fromsql)

## bifabrik

[Show source in bifabrik.py:8](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/bifabrik.py#L8)

The starting point for using the library.

To create an instance, pass the class `SparkSession` ("spark") from your Fabric notebook.
You can then use various methods to load data to tables to the current lakehouse.
The notebook has to have a default lakehouse selected - it references files from the Files/
path of the lakehouse and tables from Tables/

Examples
--------
Load a CSV file from Files/Folder/orders*.csv (all files matching the pattern) to a table called OrdersTable.

```python
>>> from bifabrik import bifabrik
>>> bif = bifabrik(spark)
>>>
>>> bif.fromCsv.path('Folder/orders*.csv').toTable('OrdersTable').save()
```

Load the results of a SQL query to a table

```python
>>> bif.fromSql.query("SELECT * FROM OrdersTable LIMIT 10").toTable('TenOrders').save()
```

#### Signature

```python
class bifabrik:
    def __init__(self, spark: SparkSession): ...
```

### bifabrik().fromCsv

[Show source in bifabrik.py:35](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/bifabrik.py#L35)

Load data from CSV

Examples
--------

```python
>>> from bifabrik import bifabrik
>>> bif = bifabrik(spark)
>>>
>>> bif.fromCsv.path('orders*.csv').toTable('Orders').save()
```

#### Signature

```python
@property
def fromCsv(self) -> CsvSource: ...
```

### bifabrik().fromJson

[Show source in bifabrik.py:50](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/bifabrik.py#L50)

Load data from JSON

Examples
--------

```python
>>> from bifabrik import bifabrik
>>> bif = bifabrik(spark)
>>>
>>> bif.fromJson.path('invoices.json').toTable('Invoices').save()
```

#### Signature

```python
@property
def fromJson(self) -> JsonSource: ...
```

### bifabrik().fromSql

[Show source in bifabrik.py:65](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/bifabrik.py#L65)

Load the result of a SQL query to a table

Examples
--------

```python
>>> from bifabrik import bifabrik
>>> bif = bifabrik(spark)
>>>
>>> bif.fromSql.query('SELECT A, B, C FROM Table1 WHERE D = 1').toTable('Table2').save()
```

#### Signature

```python
@property
def fromSql(self) -> SqlSource: ...
```