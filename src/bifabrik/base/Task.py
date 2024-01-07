class Task:
    
    def __init__(self, parentPipeline, name):
        
        parentPipeline.addTask(self)
        self._pipeline = parentPipeline
        self._spark = parentPipeline.spark
        self._name = name
        self._result = None
        self._error = None

    def prepare():
        pass

    def execute():
        pass

    def cleanup():
        pass

    def rollback():
        pass

    @property
    def error(self):
        return self._error
    
    @property
    def result(self):
        return self._result
    
    def getTaskResult(self) -> any:
        return self._pipeline.getResult(self)

    def clearResults(self):
        self._result = None
        self._error = None