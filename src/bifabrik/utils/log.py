from bifabrik.cfg.specific.LogConfiguration import LogConfiguration
import bifabrik.utils.fsUtils as fsu

import logging
from functools import wraps
from inspect import getcallargs, getfullargspec
from collections.abc import Iterable
from collections import OrderedDict
from itertools import *

__logger = logging.getLogger('bifabrik')
__errorLogHandler = None
__logHandler = None

# called by bifabrik when starting a pipeline
def configureLogger(cfg: LogConfiguration) -> logging.Logger:
    """Initializes logging based on configuration"""
    # check if the logging cfg was modified
    if not cfg.__modified:
        return
    cfg.__modified = False

    __logger = logging.getLogger('bifabrik')

    for handler in __logger.handlers[:]:
        __logger.removeHandler(handler)
    __logger.propagate = False

    if cfg.loggingEnabled == False:
        return

    level = logging.getLevelName(cfg.loggingLevel)
    __logger.setLevel(level)

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


def getLogger():
    """Returns the 'bifabrik' logger if it has been initialized (using setLogger(LogConfiguration))
    Otherwise returns a new logger
    """
    return logging.getLogger('bifabrik')

def logCalls(f):
    """A decorator to log every call to function (function name and arg values).
    The calls are logged at the INFO level.
    Based on https://gist.github.com/DarwinAwardWinner/1170921
    """
    @wraps(f)
    def func(*args, **kwargs):
        for line in __describe_call(f, *args, **kwargs):
                __logger.info(line)
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
    res = "Calling %s(" % func.__name__
    for argname, argvalue in __getcallargs_ordered(func, *args, **kwargs).items():
        res = res + ("\t%s = %s" % (argname, repr(argvalue)))
    res = res + ")"
    return res