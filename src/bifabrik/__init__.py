# read version from installed package
from importlib.metadata import version
from bifabrik.bifabrik import bifabrik

__version__ = version("bifabrik")


