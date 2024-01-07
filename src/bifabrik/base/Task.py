class Task:
    
    def __init__(self, parentPipeline, name):
        
        parentPipeline.addTask(self)
        self._pipeline = parentPipeline
        self._spark = parentPipeline.spark
        self._name = name
        self._result = None
        self._completed = False
        self._success = False

    def prepare():
        pass

    def execute():
        pass

    def cleanup():
        pass

    def rollback():
        pass

    @property
    def name(self):
        return self._name
    
    def getTaskResult(self):
        self._pipeline.getResult(self)