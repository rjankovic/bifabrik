# Fabric warehouse T-SQL data source

Usually, you will be loading data from a lakehouse to a warehouse, as in from silver to gold layer in your solution. For that, you can use the [warehouse destination](dst_warehouse_table.md).

However, sometimes you may need to go the other direction - from a warehouse to a lakehouse. In that case, the warehouse source can be used as follows.

```python
import bifabrik as bif

# configure service principal authentication
bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'

bif.config.sourceStorage.sourceWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'

# optionally, you can set the default source database name
# bif.config.sourceStorage.sourceWarehouseName = 'DW1'

bif.fromWarehouseSql('''
SELECT CountryOrRegion, Year, PublicHolidayCount FROM [DW1].[dbo].[HolidayCountsYearly]
''').toTable('HolidayCountsYearlyFromDW').run()
```

## Warehouse to warehouse transformations

Besides datasource, there is also a [Fabric warehouse table destination](dst_warehouse_table.md). You could even combine these two and have a warehouse -> warehouse transformation orchestrated from a python notebook:

```python
import bifabrik as bif

bif.config.security.keyVaultUrl = 'https://kv-contoso.vault.azure.net/'
bif.config.security.servicePrincipalClientId = '56712345-1234-7890-abcd-abcd12344d14'
bif.config.security.servicePrincipalClientSecretKVSecretName = 'contoso-clientSecret'

# can be the same connections string for source and destination;
# would be different if your warehouses were in different workspaces
bif.config.sourceStorage.sourceWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'
bif.config.destinationStorage.destinationWarehouseConnectionString = 'dxtxxxxxxbue.datawarehouse.fabric.microsoft.com'

# destination databse name
bif.config.destinationStorage.destinationWarehouseName = 'WH_GOLD'

bif.fromWarehouseSql('''
SELECT 
    Industry_code_NZSIOC 
    ,Variable_code
    ,Variable_name
    ,AVG(Value) AvgValue
FROM WH_SILVER.dbo.AnnualSurvey
WHERE [Year] = 2020
GROUP BY Industry_code_NZSIOC 
    ,Variable_code
    ,Variable_name
''').toWarehouseTable('AverageSurveyValues') \
.identityColumnPattern('{tablename}Id') \
.run()
```


[Back](../index.md)
