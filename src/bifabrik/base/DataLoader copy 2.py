from bifabrik.src.DataSource import DataSource
from bifabrik.dst.TableDestination import TableDestination
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
        
class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.source = None
        self.destination = None
    

class Task:
    _pipeline = None
    def __init__(self, parentPipeline):
        
        if parentPipeline != None:
            parentPipeline.addTask(self)
        

    def prepare():
        pass

    def execute():
        pass

    def rollback():
        pass

class Pipeline(Task):

    def addTask(t: Task):
        pass

    def getTasks() -> list[Task]:
        pass
