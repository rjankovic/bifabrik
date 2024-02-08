# Configuration

Consistent configuration is one of the core values of the project.

We like our lakehouses to be uniform in terms of loading patterns, table structures, tracking, etc. At the same time, we want to keep it [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself).

bifabrik configuration aims to cover many aspects of the lakehouse so that you can define your conventions once, use it repeatedly, and override when neccessary.

## Local configuration
By local configuration, we mean settings that only apply to a specific pipeline. Global configuration, on the other hand, sets the preferences for all subsequent data loads.

This code loads data from a CSV file, specifying that the field delimiter is ";" and decimals in numbers are delimited by ",".
```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.fromCsv('Files/CsvFiles/dimBranch.csv').delimiter(';').decimal(',').toTable('DimBranch').run()
```

Let's break it down a little bit. First, we say that we want to load data from CSV. Based on that context, bifabik gives us configuration options. We can check those using `help()`:

```python
src = bif.fromCsv('Files/CsvFiles/dimBranch.csv')

# show the configuration options
help(src)
```
The output looks like this:
![image](https://github.com/rjankovic/bifabrik/assets/2221666/8dc00d97-e5d3-4d23-a8f5-211095218d9d)


We can see that most of these settings are taken from pandas, as the library uses pandas to load CSV files. Generally, any action you begin to define will give you config options based on its type and expose them through the fluent API.

With that, you can build your pipeline step by step, exploring the config options and verifying the setup before running the pipeline:

```python
src = bif.fromCsv('Files/CsvFiles/dimBranch.csv')

# show the configuration options
help(src)

# configure the source
src.delimiter(';').decimal(',')

# check the new values
print(src.option('decimal'))
print(src.option('delimiter'))

# run the pipeline
src.toTable('DimBranch').run()
```
If you prefer the key-value setup as in PySpark, you can do that too:

```python
bif.fromCsv('Files/CsvFiles/dimBranch.csv').option('delimiter', ';').option('decimal', ',').toTable('DimBranch').run()
```


[__⇨__ Next - Configuration](configuration.md)

[Back](../index.md)
