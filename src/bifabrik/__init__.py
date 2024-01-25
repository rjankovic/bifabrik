# read version from installed package
from importlib.metadata import version
__version__ = version("bifabrik")
__loggername__ = "bifabrik_logger"

from bifabrik.bifabrik import bifabrik



