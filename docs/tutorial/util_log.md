# Logging utility

This tool uses the standard python [logging facility](https://docs.python.org/3/library/logging.html) and simplifies writing logs in Fabric to a CSV-like file. Additionaly, errors are written to a separate error log file. 

`bifabrik` pipelines log their progress and errors by default (see below).

You can use `bifabrik.utils.log` independently of the rest of `bifabrik` for your custom logs.

## Log bifabrik pipelines

### Default setup

By default, `bifabrik` will log pipeline progress to `Files/BifabrikLog.log`  and errors to `Files/BifabrikErrorLog.log` in the default lakehouse of the notebook.

Thus, if you just run a few pipelines like
```python
import bifabrik as bif

bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv') \
    .toTable('Survey2021').run()

bif.fromSql('''
SELECT CountryCode, FullName
FROM DimBranchZ LIMIT 3
''').toTable('DimBranch2').run()
```

You can end up with something like
```
2024-03-25 16:33:52,436	INFO	Executing CSV source: Files/CsvFiles/annual-enterprise-survey-2021.csv
2024-03-25 16:33:52,437	INFO	Searching location Files
2024-03-25 16:33:52,510	INFO	Searching location Files/CsvFiles
2024-03-25 16:33:52,550	INFO	Loading CSV files: [/lakehouse/default/Files/CsvFiles/annual-enterprise-survey-2021.csv]
2024-03-25 16:33:53,390	INFO	Executing Table destination: Survey2021
2024-03-25 16:33:59,866	INFO	Executing SQL source: 
SELECT CountryCode, FullName
FROM DimBranchZ LIMIT 3

2024-03-25 16:34:14,668	INFO	Executing Table destination: DimBranch2
```

### Custom configuration

You can modify the logging level and file paths:

```python
import bifabrik as bif

# default = 'Files/BifabrikLog.log'
bif.config.log.logPath = '/log/log.csv'

# default = 'Files/BifabrikErrorLog.log'
bif.config.log.errorLogPath = '/log/error_log.csv'

# default = 'INFO'
bif.config.log.loggingLevel = 'DEBUG'
```

Or you can disable logging altogether:

```python
bif.config.log.loggingEnabled = False
```

You may also want to save the logging configuration along with other common preferences to a  JSON file to be reused in different notebook - [see more about configuration](configuration.md)

## Configure and use the logger independently

Let's ignore the rest of the library and use the logging utility independently. First, we need to create a `LogConfiguration` object:

```python
from bifabrik.cfg.specific.LogConfiguration import LogConfiguration
from bifabrik.utils import log

cfg = LogConfiguration()

# default = 'Files/BifabrikLog.log'
cfg.logPath = '/logTest/log.csv'

# default = 'Files/BifabrikErrorLog.log'
cfg.errorLogPath = '/logTest/error_log.csv'

# default = 'INFO'
cfg.loggingLevel = 'DEBUG'

# set the config and get the logger
logger = log.configureLogger(cfg)
```

The `configureLogger` function returns a python [Logger](https://docs.python.org/3/library/logging.html#logging.Logger). Once configured, the logger is available globlly and can be retrieved using `getLogger()`

```python
from bifabrik.utils import log
logger = log.getLogger()
```

If you don't set the configuration, logs will not be written.

## Logging custom messages

```python
from bifabrik.utils import log
logger = log.getLogger()

logger.info('test info log 1')
logger.info('test info log 2')
logger.info('test info log 3')
logger.error('error 1')
logger.error('error 2')
```
The log file can look like this

```
2024-02-01 23:19:45,909	INFO	test info log 1
2024-02-01 23:19:45,961	INFO	test info log 2
2024-02-01 23:19:45,961	INFO	test info log 3
2024-02-01 23:19:45,961	ERROR	error 1
2024-02-01 23:19:45,974	ERROR	error2
```

The error log is similar, but it only writes logs with severity `ERROR` or `CRITICAL`.

Remember we configured `logPath` to `'/logTest/error_log.csv'` above? That wasn't an accident. Let's try this:

```python
df = spark.read.format("csv").option('header','false').option('delimiter', '\t').load('Files/logTest/log.csv')
display(df)
```

![image](https://github.com/rjankovic/bifabrik/assets/2221666/23359ce9-5922-466d-bd86-9cd493c6e816)

> Note that it can take a while for the logs to be flushed to files, maybe until the end of the PySpark session.
> 
> If you don't want to wait, consider flushing the logger manually or using [logging.shutdown](https://docs.python.org/3/library/logging.html#logging.shutdown).

## Logging function calls

Once you have your logging set up, you can take advantage of one more feature - logging function calls, including the arguments passed to each call, using a special decorator

```python
from bifabrik.utils.log import logCalls

# ...configure the logger as before...

# each call of this function will be logged, without calling the logger directly
@logCalls
def loggedFunction(str):
    print(str)

loggedFunction('functionLog1')
loggedFunction('functionLog2')
loggedFunction('functionLog3')
```

The log file will look like this:

```
2024-02-01 23:15:00,149	INFO	Calling loggedFunction(	str = 'functionLog1')
2024-02-01 23:15:00,149	INFO	Calling loggedFunction(	str = 'functionLog2')
2024-02-01 23:15:00,150	INFO	Calling loggedFunction(	str = 'functionLog3')
```


[Back](../index.md)
