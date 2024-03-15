# Loading from a Spark / Pandas DataFrame

If you already have your data prepared in a Spark DataFrame, you can pass it to bifabrik

```python
import bifabrik as bif

df = spark.read.format("csv").option("header","true").load("Files/CsvFiles/annual-enterprise-survey-2021.csv")
bif.fromSparkDf(df).toTable('Table1').run()
```
This can be useful when you only need the [table destination](dst_table.md) functionality - something like

```python
bif.fromSparkDf(df) \
  .toTable('DimensionTable') \
  .increment('merge') \
  .mergeKeyColumns(['Code']) \
  .identityColumnPattern('{tablename}ID') \
  .run()
```

If you prefer pandas, you can similarly load data from a pandas DataFrame

```python
import pandas as pd

df = pd.read_csv('data.csv')
bif.fromPandasDf(df).toTable('Table1').run()
```

[Back](../index.md)
