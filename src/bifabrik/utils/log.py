import logging
from bifabrik.cfg.specific.LogConfiguration import LogConfiguration

logger = logging.getLogger('bifabrik')
errorHandler = None
logHandler = None

def updateLogger(cfg: LogConfiguration):
    if errorHandler is not None:
        logger.removeHandler(errorHandler)
        errorHandler = None
    if logHandler is not None:
        logger.removeHandler(logHandler)
        logHandler = None

    # TODO: apply cfg
    