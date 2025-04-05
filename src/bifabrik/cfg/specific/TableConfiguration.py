from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class TableConfiguration(Configuration):
    """Configuration related to saving data to delta tables in a Fabric lakehouse or warehouse
    """
    def __init__(self):
        super().__init__()
        self.__watermarkColumn = None
        self.__increment = None
        self.__identityColumnPattern = None
        self.__insertDateColumn = None
        self.__updateDateColumn = None
        self.__mergeKeyColumns = []
        self.__snapshotKeyColumns = []
        self.__invalidCharactersInColumnNamesReplacement = '_'
        self.__canAddNewColumns = True
        self.__allowMissingColumnsInSource = True
        self.__addNARecord = False
        self.__addBadValueRecord = False
        self.__largeTableMergeMethodEnabled = False
        self.__largeTableMergeMethodSourceThresholdGB: float = 0.2
        self.__largeTableMergeMethodDestinationThresholdGB: float = 20
        self.__mergeSourceMaterializationThresholdRowcount: int = 1000000
        self.__rowStartColumn = 'RowStart'
        self.__rowEndColumn = 'RowEnd'
        self.__currentRowColumn = 'CurrentRow'
        self.__scd2SoftDelete = False
        self.__scd2ExcludeColumns = []
        self.__partitionByColumns = []
        self.__ensureTableIsReadyAfterSaving = True

    @CfgProperty
    def watermarkColumn(self) -> str:
        """Which column to use as the watermark when filtering records for incremental load.
        If the target table already exists and the watermark column is set, only records where the watermark is greater than the MAX from the table will be inserted.
        default None
        """
        return self.__watermarkColumn
    @watermarkColumn.setter(key='watermarkColumn')
    def watermarkColumn(self, val):
        self.__watermarkColumn = val

    @CfgProperty
    def increment(self) -> str:
        """Increment method
        None or "overwrite" : full load
        "append"            : add incoming rows to the table
        "merge"             : update / insert (SCD 1) after matching based on the mergeKeyColumns setting
        "scd2"              : perform SCD2 matching rows based on the mergeKeyColumns setting, using RowStartColumn, RowEndColumn and CurrentRowColumn column names 
        "snapshot"          : based on the snapshotKeyColumns setting (typically a date), inseert the new records and remove any old records that refer to a newly loaded snapshot key
        default None
        """
        return self.__increment
    @increment.setter(key='increment')
    def increment(self, val):
        if val is not None:
            val = val.lower()
        self.__increment = val

    @CfgProperty
    def mergeKeyColumns(self) -> str:
        """Array of column names used as the business key columns when the 'merge' increment method is used
        default []
        """
        return self.__mergeKeyColumns
    @mergeKeyColumns.setter(key='mergeKeyColumns')
    def mergeKeyColumns(self, val):
        t = str(type(val))
        if t == "<class 'str'>":
            self.__mergeKeyColumns = val.split(',')
        else:
            self.__mergeKeyColumns = val

    @CfgProperty
    def snapshotKeyColumns(self) -> str:
        """Array of column names used as the snapshot key columns when the 'snapshot' increment method is used
        default []
        """
        return self.__snapshotKeyColumns
    @snapshotKeyColumns.setter(key='snapshotKeyColumns')
    def snapshotKeyColumns(self, val):
        self.__snapshotKeyColumns = val

    @CfgProperty
    def identityColumnPattern(self) -> str:
        """The name pattern of the identity column to be added to the table. The values are 1, 2, 3..., each new record gets a new number.
        If configured, the column will be placed at the beginning of the table.
        
        Supported placeholders:
        {tablename} : the delta table name
        {lakehousename} : the lakehouse name

        example: '{tablename}_ID'

        default None
        """
        return self.__identityColumnPattern
    @identityColumnPattern.setter(key='identityColumnPattern')
    def identityColumnPattern(self, val):
        self.__identityColumnPattern = val

    @CfgProperty
    def insertDateColumn(self) -> str:
        """The name of the timestamp column to be added to each record, reflecting the time when the record was inserted.
        default None
        """
        return self.__insertDateColumn
    @insertDateColumn.setter(key='insertDateColumn')
    def insertDateColumn(self, val):
        self.__insertDateColumn = val
    
    @CfgProperty
    def updateDateColumn(self) -> str:
        """The name of the timestamp column to be added to each record, reflecting the time when the record was inserted or last updated.
        This gets updated during SCD1 updates, unlike insertDateColumn
        default None
        """
        return self.__updateDateColumn
    @updateDateColumn.setter(key='updateDateColumn')
    def updateDateColumn(self, val):
        self.__updateDateColumn = val

    @CfgProperty
    def invalidCharactersInColumnNamesReplacement(self) -> str:
        """The characters ' ,;{}()\n\t=' are invalid in delta tables. This sets the replacement character. You can use '' to just remove the invalid characters.
        default '_'
        """
        return self.__invalidCharactersInColumnNamesReplacement
    @invalidCharactersInColumnNamesReplacement.setter(key='invalidCharactersInColumnNamesReplacement')
    def invalidCharactersInColumnNamesReplacement(self, val):
        self.__invalidCharactersInColumnNamesReplacement = val
    
    @CfgProperty
    def canAddNewColumns(self) -> bool:
        """If the table already exists and the input dataset has extra columns, add the columns to the table with empty values before loading the new input.
        default True
        """
        return self.__canAddNewColumns
    @canAddNewColumns.setter(key='canAddNewColumns')
    def canAddNewColumns(self, val):
        self.__canAddNewColumns = val

    @CfgProperty
    def allowMissingColumnsInSource(self) -> bool:
        """If the table already exists and the input dataset has missing columns, add the columns to the source with empty values.
        default True
        """
        return self.__allowMissingColumnsInSource
    @allowMissingColumnsInSource.setter(key='allowMissingColumnsInSource')
    def allowMissingColumnsInSource(self, val):
        self.__allowMissingColumnsInSource = val

    @CfgProperty
    def addNARecord(self) -> bool:
        """Add a dimension "N/A" record with identity -1, N/A for strings, 0 for numbers. identityColumnPattern needs to be configured when this is enabled.
        default False
        """
        return self.__addNARecord
    @addNARecord.setter(key='addNARecord')
    def addNARecord(self, val):
        self.__addNARecord = val

    @CfgProperty
    def addBadValueRecord(self) -> bool:
        """Add a dimension "Bad Value" record with identity 0, Bad Value for strings, 0 for numbers. identityColumnPattern needs to be configured when this is enabled.
        default False
        """
        return self.__addBadValueRecord
    @addBadValueRecord.setter(key='addBadValueRecord')
    def addBadValueRecord(self, val):
        self.__addBadValueRecord = val

    @CfgProperty
    def largeTableMergeMethodEnabled(self) -> bool:
        """For large tables in merge mode, first load the changed records to a separate temp table and perform the merge there before appending back to the original table
        default False
        """
        return self.__largeTableMergeMethodEnabled
    @largeTableMergeMethodEnabled.setter(key='largeTableMergeMethodEnabled')
    def largeTableMergeMethodEnabled(self, val):
        self.__largeTableMergeMethodEnabled = val

    @CfgProperty
    def largeTableMergeMethodSourceThresholdGB(self) -> float:
        """For large tables in merge mode, first load the changed records to a separate temp table and perform the merge there before appending back to the original table - threshold for the size of the source data in GB
        default 0.2
        """
        return self.__largeTableMergeMethodSourceThresholdGB
    @largeTableMergeMethodSourceThresholdGB.setter(key='largeTableMergeMethodSourceThresholdGB')
    def largeTableMergeMethodSourceThresholdGB(self, val):
        self.__largeTableMergeMethodSourceThresholdGB = val
    
    @CfgProperty
    def largeTableMergeMethodDestinationThresholdGB(self) -> float:
        """For large tables in merge mode, first load the changed records to a separate temp table and perform the merge there before appending back to the original table - threshold for the size of the destination table in GB
        default 20
        """
        return self.__largeTableMergeMethodDestinationThresholdGB
    @largeTableMergeMethodDestinationThresholdGB.setter(key='largeTableMergeMethodDestinationThresholdGB')
    def largeTableMergeMethodDestinationThresholdGB(self, val):
        self.__largeTableMergeMethodDestinationThresholdGB = val

    @CfgProperty
    def mergeSourceMaterializationThresholdRowcount(self) -> float:
        """For large tables, merge increment is more efficient if the source data is first materialized into a table and partitioned in the same manner as the target table to enable partition pruning.
        If the source rowcount is above this threshold, the table will be materialized before merging.
        default 1000000
        """
        return self.__mergeSourceMaterializationThresholdRowcount
    @mergeSourceMaterializationThresholdRowcount.setter(key='mergeSourceMaterializationThresholdRowcount')
    def mergeSourceMaterializationThresholdRowcount(self, val):
        self.__mergeSourceMaterializationThresholdRowcount = val

    @CfgProperty
    def rowStartColumn(self) -> str:
        """Only for SCD2 increment - the row start timestamp column name
        default RowStart
        """
        return self.__rowStartColumn
    @rowStartColumn.setter(key='rowStartColumn')
    def rowStartColumn(self, val):
        self.__rowStartColumn = val
    
    @CfgProperty
    def rowEndColumn(self) -> str:
        """Only for SCD2 increment - the row end timestamp column name
        default RowEnd
        """
        return self.__rowEndColumn
    @rowEndColumn.setter(key='rowEndColumn')
    def rowEndColumn(self, val):
        self.__rowEndColumn = val
    
    @CfgProperty
    def currentRowColumn(self) -> str:
        """Only for SCD2 increment - the current row (Ture / False) column name
        default CurrentRow
        """
        return self.__currentRowColumn
    @currentRowColumn.setter(key='currentRowColumn')
    def currentRowColumn(self, val):
        self.__currentRowColumn = val
    
    @CfgProperty
    def scd2SoftDelete(self) -> bool:
        """For SCD 2 increment, set the RowEnd timestamp for rows not found in source (for full load only)
        default False
        """
        return self.__scd2SoftDelete
    @scd2SoftDelete.setter(key='scd2SoftDelete')
    def scd2SoftDelete(self, val):
        self.__scd2SoftDelete = val

    @CfgProperty
    def scd2ExcludeColumns(self) -> bool:
        """For SCD 2 increment - a list of untracked non-key columns - these will not trigger a new row version if their value changes
        default []
        """
        return self.__scd2ExcludeColumns
    @scd2ExcludeColumns.setter(key='scd2ExcludeColumns')
    def scd2ExcludeColumns(self, val):
        t = str(type(val))
        if t == "<class 'str'>":
            self.__scd2ExcludeColumns = val.split(',')
        else:
            self.__scd2ExcludeColumns = val

    @CfgProperty
    def partitionByColumns(self) -> bool:
        """For SCD 2 increment - a list of columns by which to partition the destination table
        default []
        """
        return self.__partitionByColumns
    @partitionByColumns.setter(key='partitionByColumns')
    def partitionByColumns(self, val):
        t = str(type(val))
        if t == "<class 'str'>":
            self.__partitionByColumns = val.split(',')
        else:
            self.__partitionByColumns = val
    
    @CfgProperty
    def ensureTableIsReadyAfterSaving(self) -> bool:
        """It can take a few seconds before a newly created table is available in SparkSQL - with this option, the data saving pipeline waits for the table to be available
        default True
        """
        return self.__ensureTableIsReadyAfterSaving
    @ensureTableIsReadyAfterSaving.setter(key='ensureTableIsReadyAfterSaving')
    def ensureTableIsReadyAfterSaving(self, val):
        self.__ensureTableIsReadyAfterSaving = val