import logging
from bifabrik.cfg.specific.LogConfiguration import LogConfiguration

logger = logging.getLogger('bifabrik')
errorHandler = None
logHandler = None

# called by bifabrik when starting a pipeline
def updateLogger(cfg: LogConfiguration):
    # check if the logging cfg was modified
    if not cfg.__modified:
        return
    cfg.__modified = False

    if errorHandler is not None:
        logger.removeHandler(errorHandler)
        errorHandler = None
    if logHandler is not None:
        logger.removeHandler(logHandler)
        logHandler = None

    # TODO: apply cfg
    