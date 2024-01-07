from bifabrik.base.Task import Task

class Pipeline:
    
    def __init__(self) -> None:
        self._tasks = []

    def addTask(self, t: Task):
        self._tasks.append(t)

    def getTasks(self) -> list[Task]:
        return self._tasks
    
    def getTaskResult(self, task) -> any:
        targetIdx = self._tasks.index(task)
        self._executeUpToIndex(targetIdx)
        return task.result
    
    def execute(self) -> any:
        taskCount = len(self._tasks)
        if taskCount == 0:
            return None
        
        self.clearResults()
        self.getTaskResult(self._tasks[taskCount - 1])

    def clearResults(self):
        for t in self._tasks:
            t.clearResults()
    
    def _executeUpToIndex(self, index: int):
        for ix in range(0, index):
            tsk = self._tasks[ix]
            if tsk.result != None:
                continue
            
            if tsk.error != None:
                raise Exception(tsk.error)
            
            tsk.execute()





# from bifabrik.src.DataSource import DataSource
# from bifabrik.dst.TableDestination import TableDestination
# from pyspark.sql.session import SparkSession
# from pyspark.sql.dataframe import DataFrame
        
# class DataLoader:
#     def __init__(self, spark: SparkSession):
#         self.spark = spark
#         self.source = None
#         self.destination = None
    

# class Task:
#     _pipeline = None
#     def __init__(self, parentPipeline):
        
#         if parentPipeline != None:
#             parentPipeline.addTask(self)
        

#     def prepare():
#         pass

#     def execute():
#         pass

#     def rollback():
#         pass

