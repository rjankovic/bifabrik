# bifabrik
Microsoft Fabric ETL toolbox

## What is the point?
 - make BI development in Microsoft Fabric easier by providing a fluent API for common ETL tasks
 - reduce repetitive code by setting preferences in config files

See the **[project page](https://rjankovic.github.io/bifabrik/)** for info on all the features and check the **[changelog](https://github.com/rjankovic/bifabrik/blob/main/CHANGELOG.md)** to see what's new.

If you find a problem or have a feature request, please submit it here: https://github.com/rjankovic/bifabrik/issues. Thanks!

## Quickstart

First, let's install the library. Either add the bifabrik library to an environment in Fabric and attach that environment to your notebook.

Or you can add `%pip install bifabrik` at the beginning of the notebook.

### Import the library

```python
import bifabrik as bif
```

Also, make sure that your notebook is connected to a lakehouse. This is the lakehouse to which bifabrik will save data by default.

![default_lakehouse](https://github.com/rjankovic/bifabrik/assets/2221666/60951119-b0ce-40b1-8e7e-ba07b78ac06a)

You can also configure it to [target different lakehouses](https://rjankovic.github.io/bifabrik/tutorial/cfg_storage.html).

### Load CSV files (JSON is similar)
Simple tasks should be easy.

```python
import bifabrik as bif
bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv').toTable('Survey2021').run()
```
...and the table is in place

```python
display(spark.sql('SELECT * FROM Survey2021'))
```
Or you can make use of pattern matching
```python
# take all files matching the pattern and concat them
bif.fromCsv('Files/*/annual-enterprise-survey-*.csv').toTable('SurveyAll').run()
```
These are full loads, overwriting the target table if it exists.

### Configure load preferences
Is your CSV is a bit...special? No problem, we'll tend to it.

Let's say you have a European CSV with commas instead of decimal points and semicolons instead of commas as separators.
```python
bif.fromCsv("Files/CsvFiles/dimBranch.csv").delimiter(';').decimal(',').toTable('DimBranch').run()
```

The backend uses pandas, so you can take advantage of many other options - see `help(bif.fromCsv())`

### Keep the configuration
What, you have more files like that?  Well then, you probably don't want to repeat the setup each time.
Good news is, the bifabrik object can keep all your preferences:

```python
import bifabrik as bif

# set the configuration
bif.config.csv.delimiter = ';'
bif.config.csv.decimal = ','

# the configuration will be applied to all these loads
bif.fromCsv("Files/CsvFiles/dimBranch.csv").toTable('DimBranch').run()
bif.fromCsv("Files/CsvFiles/dimDepartment.csv").toTable('DimDepartment').run()
bif.fromCsv("Files/CsvFiles/dimDivision.csv").toTable('DimDivision').run()

# (You can still apply configuration in the individual loads, as seen above, to override the general configuration.)
```
If you want to persist your configuration beyond the PySpark session, you can save it to a JSON file - see [Configuration](https://rjankovic.github.io/bifabrik/tutorial/configuration.html)

> Consistent configuration is one of the core values of the project.
> 
> We like our lakehouses to be uniform in terms of loading patterns, table structures, tracking, etc. At the same time, we want to keep it [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself).
> 
> bifabrik configuration aims to cover many aspects of the lakehouse so that you can define your conventions once, use it repeatedly, and override when neccessary.


### Spark SQL transformations
Enough with the files! Let's make a simple Spark SQL transformation, writing data to another SQL table - a straightforward full load:

```python
bif.fromSql('''

SELECT Industry_name_NZSIOC AS Industry_Name 
,AVG(`Value`) AS AvgValue
FROM LakeHouse1.Survey2021
WHERE Variable_Code = 'H35'
GROUP BY Industry_name_NZSIOC

''').toTable('SurveySummarized').run()

# The resulting table will be saved to the lakehouse attached to your notebook.
# You can refer to a different source warehouse in the query, though.
```

### More options

`bifabrik` can help with incremental loads, identity columsn (auto-increment), dataframe transformations and more

For example
```python
import bifabrik as bif
from pyspark.sql.functions import col, upper

(
bif
  .fromCsv('CsvFiles/fact_append_*.csv')
  .transformSparkDf(lambda df: df.withColumn('CodeUppercase', upper(col('Code'))))
  .toTable('SnapshotTable1')
  .increment('snapshot')
  .snapshotKeyColumns(['Date', 'Code'])
  .identityColumnPattern('{tablename}ID')
  .run()
)
```
For more details, see the **[project page](https://rjankovic.github.io/bifabrik/)**
