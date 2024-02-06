# Quickstart

## Install the library
First, let's install the library. Either add the bifabrik library to an environment in Fabric and attach that environment to your notebook
![image](/docs/assets/images/bifabrik_install.png)

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
![image](/docs/assets/images/default_lakehouse.png)

## Load CSV files
Simple tasks should be easy.

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.fromCsv.path('Files/CsvFiles/annual-enterprise-survey-2021.csv').toTable('Survey2021').run()

# ...and the table is in place
display(spark.sql('SELECT * FROM Survey2021'))
```

## 'But my CSV is a bit...special'
No problem, we'll tend to it.
