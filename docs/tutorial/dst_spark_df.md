# Saving to a Spark / Pandas DataFrame

Rather than saving data to a lakehouse table, you can get it in a dataframe

```python
import bifabrik as bif

df = bif.fromCsv("Files/CsvFiles/dimBranch.csv").delimiter(';').decimal(',').toSparkDf().run()
```
This can be useful when you only need the source functionality - whether it be [JSON](src_json.md), [CSV](src_csv.md) or something else - and want to take care of data transformations in Spark.

Once you are done with transforming the dataframe, you can again use `bifabrik` to save the data from the DF to a table using a [dataframe source](src_spark_df.md)

If you prefer pandas, you can similarly save data to a pandas DataFrame

```python
import bifabrik as bif

pandas_df = bif.fromSql('SELECT * FROM SomeTable').toPandasDf().run()
```

[Back](../index.md)
