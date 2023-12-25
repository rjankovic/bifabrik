from pyspark.sql import *
from notebookutils import mssparkutils

class bifabrik:
    def __init__(self, spark: pyspark.sql.session.SparkSession):
        self.spark = spark
        self._filesPrefix = 'Files/'
        self._tablesPrefix = 'Tables/'
    
    