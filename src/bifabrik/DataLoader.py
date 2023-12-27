from bifabrik.src.DataSource import DataSource
from bifabrik.dst.TableDestination import TableDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
#from dst import
        
class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.source = None
        self.destination = None
    
