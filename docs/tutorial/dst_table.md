# Table destination

For now, the only supported option is a full-load of a table into the default lakehouse attached to the notebook:

```python
import bifabrik as bif

bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv').toTable('Survey2021').run()
```
This writes a parquet (delta) file into the `Tables/` directory in the lakehouse and Fabric then makes a table out of it, as it does.

There is a lot to be done - support for incremental loads, slowly changing dimensions, surrogate keys and more. Will keep you posted :)

[Back](../index.md)
