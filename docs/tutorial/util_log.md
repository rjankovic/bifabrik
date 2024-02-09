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

[Back](../index.md)
