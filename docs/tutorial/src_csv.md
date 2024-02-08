# Loading CSV files

Simple tasks should be easy.

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

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

## Configure load preferences
Is your CSV is a bit...special? No problem, we'll tend to it.

Let's say you have a European CSV with commas instead of decimal points and semicolons instead of commas as separators.
```python
bif.fromCsv("Files/CsvFiles/dimBranch.csv").delimiter(';').decimal(',').toTable('DimBranch').run()
```

The backend uses pandas, so you can take advantage of many other options - see `help(bif.fromCsv())`

[__⇨__ Next - Configuration](configuration.md)

[Back](../index.md)
