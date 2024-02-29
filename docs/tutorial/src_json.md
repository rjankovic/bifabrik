# Loading JSON files

Load JSON data to a lakehouse table:

```python
import bifabrik as bif

bif.fromJson('Files/JSON/orders_2023.json').toTable('DimOrder').run()
```
Table is now in place

```python
display(spark.sql('SELECT * FROM DimOrders'))
```
Or you can make use of pattern matching
```python
# take all files matching the pattern and concat them
bif.fromJson('Files/*/orders*.json').toTable('OrdersAll').run()
```
These are full loads, overwriting the target table if it exists.

## Configure load preferences
The backend of the JSON source uses the standard [PySpark dataframe loader](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameReader.json.html). Most of the apsects that can be configured there can also be set in bifabrik.

To see the options available, use `help(bif.fromJson())`.

For example, you may need to switch from the default "compressed" JSON format (one object per line in the file) to multiline:

```python
bif.fromJson('Files/JSON/orders.json').multiLine(True).toTable('DimOrders').run()
```
If you prefer the PySpark "option" syntax, you can use that too:

```python
bif.fromJson('Files/JSON/ITA_TabOrders.json').option('multiLine', 'true').toTable('TabOrders1').run()
```

You can also chain multiple settings together to configure multiple options, and more - see [Configuration](configuration.md)

[Back](../index.md)
