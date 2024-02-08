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

## Global preferences
The `bifabrik` class has a configuration of its own - see

```python
bif = bifabrik(spark)
help(bif.cfg)
```

This is gathered from all the other modules - data sources, destinations, etc. - so that you can configure all sorts of behavior in one place. This then gets applied to all subsequent pipelines.

```python
# set the configuration
bif.cfg.csv.delimiter = ';'
bif.cfg.csv.decimal = ','

# the configuration will be applied to all these loads
bif.fromCsv("Files/CsvFiles/dimBranch.csv").toTable('DimBranch').run()
bif.fromCsv("Files/CsvFiles/dimDepartment.csv").toTable('DimDepartment').run()
bif.fromCsv("Files/CsvFiles/dimDivision.csv").toTable('DimDivision').run()

# (You can still apply configuration in the individual loads, as seen above, to override the global configuration.)
```

## Save configuration to a file

If we want to keep our configuration across multiple Spark session, we'll need to save it to a file and load later. Here is how to do that with some JSON source settings:

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.cfg.json.jsonDateFormat = 'dd/MM/yyyy'
bif.cfg.json.jsonCorruptRecordsMode = 'FAILFAST'
bif.cfg.json.multiLine = True

bif.cfg.saveToFile('Files/cfg/jsonSrcSettings.json')
```

The saved config file looks like this:

```json
{
    "csv": {},
    "fileSource": {},
    "json": {
        "jsonDateFormat": "dd/MM/yyyy",
        "jsonCorruptRecordsMode": "FAILFAST",
        "multiLine": true
    },
    "log": {}
}
```
Note that only the settings we gave an explicit value for are saved - all the default values stay as is.
Later on you can load the configuration:

```python
bif.cfg.loadFromFile('Files/cfg/jsonSrcSettings.json')
```

## Default values and merging configurations

All the settings have a default value - for example, for CSV it's the default behavior of [pandas.read_csv](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html). For JSON, it's the default spark JSON reader. When we start modifying the configuration, `bifabrik` keeps track of which settings were specified explicitly. When you then save the configuration to a file, only these explicit settings are saved, as we saw above. When we later load configuration from that file, only these explicitly set properties are set.

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.cfg.json.jsonDateFormat = 'yyyy-MM-dd'
bif.cfg.json.jsonCorruptRecordsMode = 'PERMISSIVE'
bif.cfg.json.multiLine = False

bif.cfg.csv.decimal = ','
bif.cfg.csv.delimiter = ';'

# now, the JSON settings will be overwritten, but the CSV settings stay
bif.cfg.saveToFile('Files/cfg/jsonSrcSettings.json')
```

In other words, the loaded configuration is __merged__ into the current config state.

Now, assume you had different parts of your configuration saved in different files. Then you would load the configuration from each of them. Again, the configurations would be merged. In case of conflict, the config loaded later wins.

This is also what happens when applying local configuration to a task in a pipeline - the local configuration is merged into the global one.

```python
from bifabrik import bifabrik
bif = bifabrik(spark)

bif.cfg.csv.delimiter = ';'
bif.cfg.csv.decimal = ','

# the delimiter will be '|', but the decimal separator will stay ','
bif.fromCsv('Files/CsvFiles/dimBranch.csv').delimiter('|').toTable('DimBranch').run()
```

[__⇨__ Next - Configuration](configuration.md)

[Back](../index.md)
