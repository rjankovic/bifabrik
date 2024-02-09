# Logging utility

This tool uses the standard python [logging facility](https://docs.python.org/3/library/logging.html) to simplify writing logs in fabric to a CSV-like file. Additionaly, errors are written to a separate error log file. 

You can use `bifabrik.utils.log` independently of the rest of `bifabrik` for your custom logs. Also, if you configure this logging, `bifabrik` pipelines can use this to log their progress and errors, which would be nice.

## Configure the logger

First, we need to create a `LogConfiguration` object:

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

## Logging

But let's not get ahead of ourselves.

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


[Back](../index.md)
