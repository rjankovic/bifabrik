# read version from installed package
from importlib.metadata import version
__version__ = version("bifabrik")

#from bifabrik import *
from bifabrik.src import csv as csv
