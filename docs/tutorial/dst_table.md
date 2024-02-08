# Table destination

Fro now, the only supported option is a full-load of a table into the default lakehouse attached to the notebook:

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv').toTable('Survey2021').run()
```

There is a lot to be done - support for incremental loads, slowly changing dimensions, surrogate keys and more. Will keep you posted :)

[Back](../index.md)
