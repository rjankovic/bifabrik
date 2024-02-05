The assembly line for your lakehouse

### Under construction

```python
from bifabrik import bifabrik
from bifabrik.cfg.specific.LogConfiguration import LogConfiguration
from bifabrik.utils import log
cfg = LogConfiguration()
cfg.logPath = '/log.csv'
cfg.errorLogPath = '/error_log.csv'
cfg.loggingLevel = 'DEBUG'

bif = bifabrik(spark)
log.configureLogger(cfg)
```
