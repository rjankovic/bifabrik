from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
#from bifabrik.base.Pipeline import Pipeline

class Task:
    
    # parentPipeline: Pipeline
    def __init__(self, parentPipeline):
        parentPipeline.addTask(self)
        self._pipeline = parentPipeline
        self._spark = parentPipeline.spark
        self._result = None
        self._error = None
        self._completed = False

    def execute(self, input):
        """Obtain the result (typically a dataframe) or save the data in case of a destination
        """
        pass

    def cleanup(self):
        """Cleanup after the execution (archive processed files, remove temp data)
        """
        pass

    def rollback(self):
        """Revert changes made during a failed execution (restore modified delta tables or processed files)
        """
        pass

    @property
    def error(self):
        """The error message from the last execution (str)
        """
        return self._error
    
    @property
    def result(self):
        """The result (if any) of a completed task - typically a pyspark.sql.dataframe.DataFrame"""
        return self._result
    
    @property
    def completed(self):
        """Indicator of the task being completed"""
        return self._completed
    
    def getTaskResult(self) -> any:
        """Execute all tasks in the pipeline up to this one and return its result
        """
        return self._pipeline.getTaskResult(self)

    def clearResults(self):
        self._result = None
        self._error = None
        self._completed = False

    def run(self) -> any:
        """Run the pipeline.
        Returns the result of the last task, if any.
        """
        return self._pipeline.execute()
