# Transforming data using Spark / Pandas DataFrames

`bifabrik` is mostly concerned with loading data from sources and saving it to lakehouses. For the transformations in between, you can use lambda functions for transforming the data as a Spark DataFrame. Just pass the transformation to the `transformSparkDf` function

```python
import bifabrik as bif
from pyspark.sql.functions import col,lit

(
bif
    .fromCsv("Files/CsvFiles/annual-enterprise-survey-2021.csv")
    .transformSparkDf(lambda df: df.withColumn('NewColumn', lit('NewValue')))
    .toTable("Survey2")
    .run()
)
```
The transformSparkDf takes a function where a spark dataframe is both the input and output.

This gets applied to the result of the previous operation and then passed to the next one (in this case the table destination)

If you prefer pandas, go for the `transformPandasDf` function. Similarly to spark, the function takes a pandas DataFrame and returns the transformed DataFrame.

```python
import bifabrik as bif

def transformFunction(df):
    df.insert(0, 'NewColumn', 'NewValue')
    return df


bif \
    .fromCsv("Files/CsvFiles/annual-enterprise-survey-2021.csv") \
    .transformPandasDf(transformFunction) \
    .toTable("Survey3") \
    .run()
```

You can also [save the loaded source data to a data frame](dst_spark_df.md), do the transformations and then [use dataframe source](src_spark_df.md) to save the result.

[Back](../index.md)
