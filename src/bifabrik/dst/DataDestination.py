from bifabrik.base.Task import Task

class DataDestination(Task):
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)

    def save(self) -> None:
        """Save the data to the destination. This is the "run" method at the end of the chain. 
        Returns the result of the last task, if any.
        """
        self._pipeline.execute()