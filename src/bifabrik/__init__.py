"""
A collection of ETL tools for Microsoft Fabric
Includes logging, configuration management and other tools

For more info see https://rjankovic.github.io/bifabrik/

Sample usage:
    > from bifabrik import bifabrik
    > bif = bifabrik(spark)
    > bif.fromCsv.path('DATA/factOrderLine*.csv').delimiter(';').decimal(',').toTable('FactOrderLine').run()
"""

# read version from installed package
from importlib.metadata import version
__version__ = version("bifabrik")
__loggername__ = "bifabrik_logger"

#import bifabrik
from bifabrik.utils.fsUtils import getDefaultLakehouseAbfsPath

if getDefaultLakehouseAbfsPath() is None:
    print('bifabrik warning: the notebook is not attached to a lakehouse - some features of bifabrik will not work correctly.')