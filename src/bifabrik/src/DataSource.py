#from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.dst.LakehouseTableDestination import LakehouseTableDestination
from bifabrik.dst.WarehouseTableDestination import WarehouseTableDestination
from bifabrik.dst.SparkDfDestination import SparkDfDestination
from bifabrik.dst.PandasDfDestination import PandasDfDestination
from bifabrik.tsf.DataTransformation import DataTransformation
from bifabrik.tsf import SparkDfTransformation
from bifabrik.tsf import PandasDfTransformation
from bifabrik.tsf import ValidationTransformation
from bifabrik.base.Task import Task

class DataSource(Task):
    """Generic data source. 
    Contains methods to initiate a DataDestination after you are done setting the source.
    bif.sourceInit.sourceOption1(A).srcOpt2(B).toTable("Tab").destinationOption(X)....save()
    """
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        

    # def toDf(self) -> DataFrame:
    #     """Converts the data to a spark dataframe
    #     """
    #     pass

    def toTable(self, targetTableName: str, targetSchemaName: str = 'dbo') -> LakehouseTableDestination:
        """Sets the lakehouse destination table name.
        The name can contain a schema as in 'schema.table' or you can specify the schema in the targetSchemaName parameter.
        """
        return self.toLakehouseTable(targetTableName, targetSchemaName)
    
    def toTempTable(self) -> LakehouseTableDestination:
        """Savees the data to a temp table with a unique name (generated from the ID of the pipeline).
        """
        temp_table_id = self._pipeline._id.replace('-', '')
        temp_table_name = f'temp_{temp_table_id}'
        return self.toLakehouseTable(targetTableName = temp_table_name)
    
    def toLakehouseTable(self, targetTableName: str, targetSchemaName: str = 'dbo') -> LakehouseTableDestination:
        """Sets the lakehouse destination table name.
        The name can contain a schema as in 'schema.table' or you can specify the schema in the targetSchemaName parameter.
        """
        dst = LakehouseTableDestination(self._pipeline, targetTableName, targetSchemaName)
        return dst
    
    def toWarehouseTable(self, targetTableName: str, targetSchemaName: str = 'dbo') -> WarehouseTableDestination:
        """Sets the warehouse destination table name. The warehouse authentication needs to be configured - see https://rjankovic.github.io/bifabrik/tutorial/dst_warehouse_table.html

        Examples
        --------
        > import bifabrik as bif
        >
        > bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
        > bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
        > bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'
        > bif.config.destinationStorage.destinationWarehouseName = 'WH_GOLD'
        > bif.config.destinationStorage.destinationWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'
        >
        > bif.fromSql('''
        > SELECT countryOrRegion `CountryOrRegion`
        > ,YEAR(date) `Year` 
        > ,COUNT(*) `PublicHolidayCount`
        > FROM LH_SILVER.publicholidays
        > GROUP BY countryOrRegion
        > ,YEAR(date)
        > ''').toWarehouseTable('HolidayCountsYearly').run()
        """
        dst = WarehouseTableDestination(self._pipeline, targetTableName, targetSchemaName)
        return dst
    
    def toSparkDf(self) -> SparkDfDestination:
        """Sets the destination as a Spark DF (for further processing)
        """
        dst = SparkDfDestination(self._pipeline)
        return dst
    
    def toPandasDf(self) -> PandasDfDestination:
        """Sets the destination as a Pandas DF (for further processing)
        """
        dst = PandasDfDestination(self._pipeline)
        return dst
    
    def transformSparkDf(self, func) -> SparkDfTransformation.SparkDfTransformation:
        """Applies the givet function to transform the data as a Spark DF
        """
        tsf = SparkDfTransformation.SparkDfTransformation(self._pipeline, func)
        return tsf
    
    def transformPandasDf(self, func) -> PandasDfTransformation.PandasDfTransformation:
        """Applies the givet function to transform the data as a Pandas DF
        """
        tsf = PandasDfTransformation.PandasDfTransformation(self._pipeline, func)
        return tsf
    
    def validate(self, testName = '') -> ValidationTransformation.ValidationTransformation:
        """Test the input data by checking the ValidationResult and ValidationMessage columns to determine if each row failed / passed the validation.
        """
        tsf = ValidationTransformation.ValidationTransformation(self._pipeline, testName)
        return tsf