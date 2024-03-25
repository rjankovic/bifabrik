"""
Logging to 2 files (log file + error log file), can be used both independently and in bifabrik pipelines.
The files use a CSV-like format so that they can be analysed using Spark
You can also log function calls including the arguments using the @logCalls decorator

    Examples
    --------
    
    a) Configure logging for use with bifabrik pipelines

    >>> import bifabrik as bif
    >>> 
    >>> # default = 'Files/BifabrikLog.log'
    >>> bif.config.log.logPath = '/log/log.csv'
    >>> 
    >>> # default = 'Files/BifabrikErrorLog.log'
    >>> bif.config.log.errorLogPath = '/log/error_log.csv'
    >>> 
    >>> # default = 'INFO'
    >>> bif.config.log.loggingLevel = 'DEBUG'
    >>> 
    >>> bif.fromCsv('Files/CsvFiles/annual-enterprise-survey-2021.csv') \
    >>>     .toTable('Survey2021').run()
    >>> 
    >>> bif.fromSql('''
    >>> SELECT CountryCode, FullName
    >>> FROM DimBranchZ LIMIT 3
    >>> ''').toTable('DimBranch2').run()

    b) Configure logging independently of the rest of the library and using the logger

    >>> from bifabrik.cfg.specific.LogConfiguration import LogConfiguration
    >>> from bifabrik.utils import log
    >>> from bifabrik.utils.log import logCalls
    >>>
    >>> cfg = LogConfiguration()
    >>> cfg.logPath = '/logFolder/log.csv'
    >>> cfg.errorLogPath = '/logFolder/error_log.csv'
    >>> cfg.loggingLevel = 'DEBUG'
    >>> logger = log.configureLogger(cfg)
    >>>
    >>> logger.info('test info log 1')
    >>> logger.info('test info log 2')
    >>> logger.error('error 1')
    >>> logger.error('error 2')
    >>> 
    >>> @logCalls
    >>> def loggedFunction(str):
    >>>   print(str)
    >>> 
    >>> loggedFunction('functionLog1')
    >>> loggedFunction('functionLog2')

"""

from bifabrik.cfg.specific.LogConfiguration import LogConfiguration
import bifabrik.utils.fsUtils as fsu

import logging
from functools import wraps
from inspect import getcallargs, getfullargspec
from collections.abc import Iterable
from collections import OrderedDict
from itertools import *
from bifabrik.utils.fsUtils import getDefaultLakehouseAbfsPath

__logger = None #logging.getLogger('bifabrik')
__errorLogHandler = None
__logHandler = None
__missingConfigWarningIssued = False

# called by bifabrik when starting a pipeline
def configureLogger(cfg: LogConfiguration) -> logging.Logger:
    """Initializes logging based on configuration"""
    
    global __logger
    global __errorLogHandler
    global __logHandler
    global __missingConfigWarningIssued

    if getDefaultLakehouseAbfsPath() is None:
        print('The logger cannot be setup because the notebook is not attached to a lakehouse. The logs would be written to the attached lakehouse under Files/.')
        __missingConfigWarningIssued = False
        return None
    
    # check if the logging cfg was modified
    # only do the setup if the log has not been set up yet or the cfg has been modified
    if (not cfg.modified) and (__logger is not None):
        if (len(__logger.handlers) > 0):
            return
    cfg.modified = False

    __logger = logging.getLogger('bifabrik')

    for handler in __logger.handlers[:]:
        __logger.removeHandler(handler)
    __logger.propagate = False

    if cfg.loggingEnabled == False:
        return

    level = logging.getLevelName(cfg.loggingLevel)
    __logger.setLevel(level)
    
    # logging to a different lakehouse cannot be supported now, because the default logger aims at a mounted directory in the environment
    # and other lakeouses are not in mounts
    # lhPath = fsu.getLakehousePath(lakehouse = cfg.logLakehouse, workspace = cfg.logWorkspace)
    # logPath = fsu.normalizeAbfsFilePath(cfg.logPath, lhPath)
    # errorLogPath = fsu.normalizeAbfsFilePath(cfg.errorLogPath, lhPath)

    logPath = fsu.normalizeFileApiPath(cfg.logPath)
    errorLogPath = fsu.normalizeFileApiPath(cfg.errorLogPath)

    __logHandler = logging.FileHandler(logPath, mode='a')
    __errorLogHandler = logging.FileHandler(errorLogPath, mode='a')
    
    __logHandler.setLevel(logging.DEBUG)
    __errorLogHandler.setLevel(logging.ERROR)

    log_format = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    __errorLogHandler.setFormatter(log_format)
    __logHandler.setFormatter(log_format)

    __logger.addHandler(__logHandler)
    __logger.addHandler(__errorLogHandler)
    return __logger


def getLogger(suppressWarnings = False):
    """Returns the 'bifabrik' logger if it has been initialized (using setLogger(LogConfiguration))
    Otherwise returns a new logger
    """
    global __logger
    global __errorLogHandler
    global __logHandler
    global __missingConfigWarningIssued

    if __missingConfigWarningIssued == False and __logger is None and suppressWarnings == False:
        print('Error: The bifabrik logger has not been properly configured. Please use configureLogger(cfg: LogConfiguration) first.')
        __missingConfigWarningIssued = True

    # this is done so that in case the log is not configured correcty, 
    # calls to the logger still pass and don't crash data loads
    if __logger is None:    
        __logger = logging.getLogger('bifabrik')

    return __logger
    #return logging.getLogger('bifabrik')

def logCalls(f):
    """A decorator to log every call to function (function name and arg values).
    The calls are logged at the INFO level.
    Based on https://gist.github.com/DarwinAwardWinner/1170921
    """
    @wraps(f)
    def func(*args, **kwargs):
        msg = __describe_call(f, *args, **kwargs)
        getLogger().info(msg)
        return f(*args, **kwargs)
    return func

def __flatten(l):
    """Flatten a list (or other iterable) recursively"""
    for el in l:
        if isinstance(el, Iterable) and not isinstance(el, str):
            for sub in __flatten(el):
                yield sub
        else:
            yield el

def __getargnames(func):
    """Return an iterator over all arg names, including nested arg names and varargs.
    Goes in the order of the functions argspec, with varargs and
    keyword args last if present."""
    #print(getfullargspec(func))
    (argnames, varargname, kwargname, defaults1, kwonlyargs1, kwonlydefaults1, annotations1) = getfullargspec(func)
    return chain(__flatten(argnames), filter(None, [varargname, kwargname]))

def __getcallargs_ordered(func, *args, **kwargs):
    """Return an OrderedDict of all arguments to a function.
    Items are ordered by the function's argspec."""
    argdict = getcallargs(func, *args, **kwargs)
    return OrderedDict((name, argdict[name]) for name in __getargnames(func))

def __describe_call(func, *args, **kwargs):
    res = "Calling %s("% func.__name__
    for argname, argvalue in __getcallargs_ordered(func, *args, **kwargs).items():
        res = res + ("\t%s = %s" % (argname, repr(argvalue)))
    res = res + ")"
    return res