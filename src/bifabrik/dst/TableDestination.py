from bifabrik.dst.DataDestination import DataDestination
from bifabrik.base.Pipeline import Pipeline
from bifabrik.cfg.TableDestinationConfiguration import TableDestinationConfiguration
from pyspark.sql.dataframe import DataFrame
from bifabrik.utils import fsUtils
from bifabrik.utils import log

class TableDestination(DataDestination, TableDestinationConfiguration):
    """Saves data to a lakehouse table.

    Examples
    --------
    > import bifabrik as bif
    >
    > bif.fromSql.query('SELECT * FROM SomeTable').toTable('DestinationTable1').run()
    """
    def __init__(self, pipeline: Pipeline, targetTableName: str):
        super().__init__(pipeline)
        TableDestinationConfiguration.__init__(self)
        self._targetTableName = targetTableName

    def __str__(self):
        return f'Table destination: {self._targetTableName}'
    
    def execute(self, input: DataFrame) -> None:
        lgr = log.getLogger()

        if input is None:
            msg = f'The table destination `{self._targetTableName}` has no input; terminating'
            print(msg)
            lgr.warning(msg)
            return
        
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)
        dstLh = mergedConfig.destinationStorage.destinationLakehouse
        dstWs = mergedConfig.destinationStorage.destinationWorkspace
        basePath = fsUtils.getLakehousePath(dstLh, dstWs)

        input.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(basePath + "/Tables/" + self._targetTableName)