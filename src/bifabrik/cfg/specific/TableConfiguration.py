from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class TableConfiguration(Configuration):
    """Configuration related to saving data to delta tables in a Fabric lakehouse or warehouse
    """
    def __init__(self):
        self._explicitProps = {}
        self.__watermarkColumn = None
        self.__increment = None
        self.__identityColumnPattern = None
        self.__insertDateColumn = None
        self.__mergeKeyColumns = []
        self.__snapshotKeyColumns = []
        self.__invalidCharactersInColumnNamesReplacement = '_'
        self.__canAddNewColumns = True
        self.__addNARecord = False

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
        "snapshot"          : based on the snapshotKeyColumns setting (typically a date), inseert the new records and remove any old records that refer to a newly loaded snapshot key
        default None
        """
        return self.__increment
    @increment.setter(key='increment')
    def increment(self, val):
        self.__increment = val

    @CfgProperty
    def mergeKeyColumns(self) -> str:
        """Array of column names used as the business key columns when the 'merge' increment method is used
        default []
        """
        return self.__mergeKeyColumns
    @mergeKeyColumns.setter(key='mergeKeyColumns')
    def mergeKeyColumns(self, val):
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
    def addNARecord(self) -> bool:
        """Add a dimension "N/A" record with identity -1, N/A for strings, 0 for numbers. identityColumnPattern needs to be configured when this is enabled.
        default False
        """
        return self.__addNARecord
    @addNARecord.setter(key='addNARecord')
    def addNARecord(self, val):
        self.__addNARecord = val
