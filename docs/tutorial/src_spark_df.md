# Loading from a Spark dataframe

If you already have your data prepared in a spark dataframe, you can pass it to bifabrik

```python
import bifabrik as bif

df = spark.read.format("csv").option("header","true").load("Files/CsvFiles/annual-enterprise-survey-2021.csv")
bif.fromSparkDf(df).toTable('Table1').run()
```
You can use this if you only need the [table destination](dst_table.md) functionality - something like

```python
bif.fromSparkDf(df).toTable('DimensionTable').increment('merge').mergeKeyColumns(['Code']).identityColumnPattern('{tablename}ID').run()
```

[Back](../index.md)
