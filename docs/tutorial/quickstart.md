# Quickstart

## Install the library
First, let's install the library. Either add the bifabrik library to an environment in Fabric and attach that environment to your notebook.

![bifabrik_install](https://github.com/rjankovic/bifabrik/assets/2221666/a7127858-2768-4b91-ae2d-71804a20ddcb)

or add 
```python
%pip install bifabrik
``` 
at the beginning of the notebook.

## Init the class
To load data, `bifabrik` needs to access the spark session.
```python
from bifabrik import bifabrik
bif = bifabrik(spark)
```

Also, __make sure that your notebook is attached to a lakehouse__. This is the lakehouse to which bifabrik will save data.

![default_lakehouse](https://github.com/rjankovic/bifabrik/assets/2221666/60951119-b0ce-40b1-8e7e-ba07b78ac06a)

## Load CSV files (JSON is similar)
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

## "But my CSV is a bit...special"
No problem, we'll tend to it.
Let's say you have a European CSV with commas instead of decimal points and semicolons instead of commas as separators.
```python
bif.fromCsv("Files/CsvFiles/dimBranch.csv").delimiter(';').decimal(',').toTable('DimBranch').run()
```

The backend uses pandas, so you can take advantage of many other options (see `help(bif.fromCsv())`)
