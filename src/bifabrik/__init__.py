# read version from installed package
from importlib.metadata import version
__version__ = version("bifabrik")
__loggername__ = "bifabrik_logger"

from bifabrik.bifabrik import bifabrik
from bifabrik.utils.fsUtils import getDefaultLakehouseAbfsPath

if getDefaultLakehouseAbfsPath() is None:
    print('bifabrik warning: the notebook is not attached to a lakehouse - some features of bifabrik will not work correctly.')