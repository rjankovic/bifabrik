from bifabrik.base.Task import Task
from bifabrik.cfg.CompleteConfiguration import CompleteConfiguration
import uuid
import bifabrik.utils.log as lg

class Pipeline:
    
    def __init__(self, spark, configuration: CompleteConfiguration) -> None:
        self._tasks = []
        self._id = str(uuid.uuid4())
        self.spark = spark
        self.configuration = configuration

    #t: bifabrik.base.Task.Task
    def addTask(self, t: Task):
        self._tasks.append(t)

    def getTasks(self) -> list[Task]:
        return self._tasks
    
    def getTaskResult(self, task) -> any:
        """Execute all tasks in the pipeline up to the given one and return its result
        """
        targetIdx = self._tasks.index(task)
        self._executeUpToIndex(targetIdx)
        return task.result
    
    def execute(self) -> any:
        taskCount = len(self._tasks)
        # if not tasks in pipeline yet, do nothing
        if taskCount == 0:
            return None
        
        self.clearResults()
        result = self.getTaskResult(self._tasks[taskCount - 1])
        self.cleanup()
        return result

    def cleanup(self):
        for ix in range(0, len(self._tasks) - 1):
            self._tasks[ix].cleanup()

    def clearResults(self):
        for t in self._tasks:
            t.clearResults()
    
    def _executeUpToIndex(self, index: int):
        prevResult = None
        lgr = lg.getLogger()

        for ix in range(0, index+1):
            tsk = self._tasks[ix]
            try:
                if tsk.completed == True:
                    if tsk.error != None:
                        raise Exception(tsk.error)
                    prevResult = tsk.result
                    continue
                
                if tsk.error != None:
                    raise Exception(tsk.error)
                
                lgr.info(f'Executing {tsk}')
                tsk.execute(prevResult)
                prevResult = tsk.result
            except Exception as e:
                lgr.exception(e)
                raise e
    @property
    def id(self) -> str:
        return self._id




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

