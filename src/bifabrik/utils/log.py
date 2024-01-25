import logging

logger = logging.getLogger('bifabrik')
errorHandler = None
logHandler = None

def updateLogger():
    if errorHandler is not None:
        logger.removeHandler(errorHandler)
        errorHandler = None
    if logHandler is not None:
        logger.removeHandler(logHandler)
        logHandler = None
    