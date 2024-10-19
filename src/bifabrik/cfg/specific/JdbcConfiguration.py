from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class JdbcConfiguration(Configuration):
    """Configuration related to reading JDBC endpoints
    """
    def __init__(self):
        super().__init__()
        self.__jdbcUrl = None
        # super().createCfgProperty("delimiter", """From pandas:
        # Character or regex pattern to treat as the delimiter. If sep=None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator from only the first valid row of the file by Pythons builtin sniffer tool
        # """)        
        self.__jdbcUser = None
        self.__jdbcQuery = None
        self.__jdbcDbtable = None
        self.__jdbcPrepareQuery = None
        self.__jdbcDriver = None
        self.__jdbcPartitionColumn = None
        self.__jdbcLowerBound = None
        self.__jdbcUpperBound = None
        self.__jdbcNumPartitions = None
        self.__jdbcQueryTimeout = None
        self.__jdbcFetchsize = None
        self.__jdbcBatchsize = None
        self.__jdbcIsolationLevel = None
        self.__jdbcSessionInitStatement = None
        self.__jdbcTruncate = None
        self.__jdbcCreateTableOptions = None
        self.__jdbcCreateTableColumnTypes = None
        self.__jdbcCustomSchema = None
        self.__jdbcPushDownPredicate = None
        self.__jdbcPushDownAggregate = None
        self.__jdbcPushDownLimit = None
        self.__jdbcPushDownOffset = None
        self.__jdbcPushDownTableSample = None
        self.__jdbcKeytab = None
        self.__jdbcPrincipal = None
        self.__jdbcRefreshKrb5Config = None
        self.__jdbcConnectionProvider = None
        self.__jdbcPreferTimestampNTZ = None
        

    @CfgProperty
    def jdbcUrl(self) -> str:
        """From pandas:
        Character or regex pattern to treat as the delimiter. If sep=None, the C engine cannot automatically detect the separator, but the Python parsing engine can, meaning the latter will be used and automatically detect the separator from only the first valid row of the file by Pythons builtin sniffer tool
        """
        return self.__jdbcUrl
    @jdbcUrl.setter(key='jdbcUrl')
    def jdbcUrl(self, val):
        self.__jdbcUrl = val
            
    @CfgProperty
    def jdbcUser(self) -> str:
        """The JDBC user name to connect with."""
        return self.__jdbcUser
    @jdbcUser.setter(key='jdbcUser')
    def jdbcUser(self, val):
        self.__jdbcUser = val

    @CfgProperty
    def jdbcQuery(self) -> str:
        """The JDBC query to execute."""
        return self.__jdbcQuery
    @jdbcQuery.setter(key='jdbcQuery')
    def jdbcQuery(self, val):
        self.__jdbcQuery = val

    @CfgProperty
    def jdbcDbtable(self) -> str:
        """The JDBC database table to connect to."""
        return self.__jdbcDbtable
    @jdbcDbtable.setter(key='jdbcDbtable')
    def jdbcDbtable(self, val):
        self.__jdbcDbtable = val

    @CfgProperty
    def jdbcPrepareQuery(self) -> str:
        """The JDBC prepared query to execute."""
        return self.__jdbcPrepareQuery
    @jdbcPrepareQuery.setter(key='jdbcPrepareQuery')
    def jdbcPrepareQuery(self, val):
        self.__jdbcPrepareQuery = val

    @CfgProperty
    def jdbcDriver(self) -> str:
        """The JDBC driver to use."""
        return self.__jdbcDriver
    @jdbcDriver.setter(key='jdbcDriver')
    def jdbcDriver(self, val):
        self.__jdbcDriver = val

    @CfgProperty
    def jdbcPartitionColumn(self) -> str:
        """The JDBC partition column."""
        return self.__jdbcPartitionColumn
    @jdbcPartitionColumn.setter(key='jdbcPartitionColumn')
    def jdbcPartitionColumn(self, val):
        self.__jdbcPartitionColumn = val

    @CfgProperty
    def jdbcLowerBound(self) -> int:
        """The JDBC lower bound for partitioning."""
        return self.__jdbcLowerBound
    @jdbcLowerBound.setter(key='jdbcLowerBound')
    def jdbcLowerBound(self, val):
        self.__jdbcLowerBound = val

    @CfgProperty
    def jdbcUpperBound(self) -> int:
        """The JDBC upper bound for partitioning."""
        return self.__jdbcUpperBound
    @jdbcUpperBound.setter(key='jdbcUpperBound')
    def jdbcUpperBound(self, val):
        self.__jdbcUpperBound = val

    @CfgProperty
    def jdbcNumPartitions(self) -> int:
        """The number of JDBC partitions."""
        return self.__jdbcNumPartitions
    @jdbcNumPartitions.setter(key='jdbcNumPartitions')
    def jdbcNumPartitions(self, val):
        self.__jdbcNumPartitions = val

    @CfgProperty
    def jdbcQueryTimeout(self) -> int:
        """The JDBC query timeout in seconds."""
        return self.__jdbcQueryTimeout
    @jdbcQueryTimeout.setter(key='jdbcQueryTimeout')
    def jdbcQueryTimeout(self, val):
        self.__jdbcQueryTimeout = val

    @CfgProperty
    def jdbcFetchsize(self) -> int:
        """The JDBC fetch size."""
        return self.__jdbcFetchsize
    @jdbcFetchsize.setter(key='jdbcFetchsize')
    def jdbcFetchsize(self, val):
        self.__jdbcFetchsize = val

    @CfgProperty
    def jdbcBatchsize(self) -> int:
        """The JDBC batch size."""
        return self.__jdbcBatchsize
    @jdbcBatchsize.setter(key='jdbcBatchsize')
    def jdbcBatchsize(self, val):
        self.__jdbcBatchsize = val

    @CfgProperty
    def jdbcIsolationLevel(self) -> str:
        """The JDBC isolation level."""
        return self.__jdbcIsolationLevel
    @jdbcIsolationLevel.setter(key='jdbcIsolationLevel')
    def jdbcIsolationLevel(self, val):
        self.__jdbcIsolationLevel = val

    @CfgProperty
    def jdbcSessionInitStatement(self) -> str:
        """The JDBC session initialization statement."""
        return self.__jdbcSessionInitStatement
    @jdbcSessionInitStatement.setter(key='jdbcSessionInitStatement')
    def jdbcSessionInitStatement(self, val):
        self.__jdbcSessionInitStatement = val

    @CfgProperty
    def jdbcTruncate(self) -> bool:
        """Whether to truncate the JDBC table before loading."""
        return self.__jdbcTruncate
    @jdbcTruncate.setter(key='jdbcTruncate')
    def jdbcTruncate(self, val):
        self.__jdbcTruncate = val

    @CfgProperty
    def jdbcCreateTableOptions(self) -> str:
        """The JDBC create table options."""
        return self.__jdbcCreateTableOptions
    @jdbcCreateTableOptions.setter(key='jdbcCreateTableOptions')
    def jdbcCreateTableOptions(self, val):
        self.__jdbcCreateTableOptions = val

    @CfgProperty
    def jdbcCreateTableColumnTypes(self) -> str:
        """The JDBC create table column types."""
        return self.__jdbcCreateTableColumnTypes
    @jdbcCreateTableColumnTypes.setter(key='jdbcCreateTableColumnTypes')
    def jdbcCreateTableColumnTypes(self, val):
        self.__jdbcCreateTableColumnTypes = val

    @CfgProperty
    def jdbcCustomSchema(self) -> str:
        """The JDBC custom schema."""
        return self.__jdbcCustomSchema
    @jdbcCustomSchema.setter(key='jdbcCustomSchema')
    def jdbcCustomSchema(self, val):
        self.__jdbcCustomSchema = val

    @CfgProperty
    def jdbcPushDownPredicate(self) -> bool:
        """Whether to push down predicates to the JDBC source."""
        return self.__jdbcPushDownPredicate
    @jdbcPushDownPredicate.setter(key='jdbcPushDownPredicate')
    def jdbcPushDownPredicate(self, val):
        self.__jdbcPushDownPredicate = val

    @CfgProperty
    def jdbcPushDownAggregate(self) -> bool:
        """Whether to push down aggregates to the JDBC source."""
        return self.__jdbcPushDownAggregate
    @jdbcPushDownAggregate.setter(key='jdbcPushDownAggregate')
    def jdbcPushDownAggregate(self, val):
        self.__jdbcPushDownAggregate = val

    @CfgProperty
    def jdbcPushDownLimit(self) -> int:
        """The JDBC push down limit."""
        return self.__jdbcPushDownLimit
    @jdbcPushDownLimit.setter(key='jdbcPushDownLimit')
    def jdbcPushDownLimit(self, val):
        self.__jdbcPushDownLimit = val

    @CfgProperty
    def jdbcPushDownOffset(self) -> int:
        """The JDBC push down offset."""
        return self.__jdbcPushDownOffset
    @jdbcPushDownOffset.setter(key='jdbcPushDownOffset')
    def jdbcPushDownOffset(self, val):
        self.__jdbcPushDownOffset = val

    @CfgProperty
    def jdbcPushDownTableSample(self) -> float:
        """The JDBC push down table sample percentage."""
        return self.__jdbcPushDownTableSample
    @jdbcPushDownTableSample.setter(key='jdbcPushDownTableSample')
    def jdbcPushDownTableSample(self, val):
        self.__jdbcPushDownTableSample = val

    @CfgProperty
    def jdbcKeytab(self) -> str:
        """The JDBC keytab file for Kerberos authentication."""
        return self.__jdbcKeytab
    @jdbcKeytab.setter(key='jdbcKeytab')
    def jdbcKeytab(self, val):
        self.__jdbcKeytab = val

    @CfgProperty
    def jdbcPrincipal(self) -> str:
        """The JDBC principal for Kerberos authentication."""
        return self.__jdbcPrincipal
    @jdbcPrincipal.setter(key='jdbcPrincipal')
    def jdbcPrincipal(self, val):
        self.__jdbcPrincipal = val

    @CfgProperty
    def jdbcRefreshKrb5Config(self) -> bool:
        """Whether to refresh the Kerberos configuration."""
        return self.__jdbcRefreshKrb5Config
    @jdbcRefreshKrb5Config.setter(key='jdbcRefreshKrb5Config')
    def jdbcRefreshKrb5Config(self, val):
        self.__jdbcRefreshKrb5Config = val

    @CfgProperty
    def jdbcConnectionProvider(self) -> str:
        """The JDBC connection provider."""
        return self.__jdbcConnectionProvider
    @jdbcConnectionProvider.setter(key='jdbcConnectionProvider')
    def jdbcConnectionProvider(self, val):
        self.__jdbcConnectionProvider = val

    @CfgProperty
    def jdbcPreferTimestampNTZ(self) -> bool:
        """Whether to prefer timestamp without time zone."""
        return self.__jdbcPreferTimestampNTZ
    @jdbcPreferTimestampNTZ.setter(key='jdbcPreferTimestampNTZ')
    def jdbcPreferTimestampNTZ(self, val):
        self.__jdbcPreferTimestampNTZ = val
