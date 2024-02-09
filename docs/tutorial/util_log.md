# Logging utility

This tool uses the standard python [logging facility](https://docs.python.org/3/library/logging.html) to simplify writing logs in fabric to a CSV-like file. Additionaly, errors are written to a separate error log file. 

You can use `bifabrik.utils.log` independently of the rest of `bifabrik` for your custom logs. Also, if you configure this logging, `bifabrik` pipelines can use this to log their progress and errors, which would be nice.




[Back](../index.md)
