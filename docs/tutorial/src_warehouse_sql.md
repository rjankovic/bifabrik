# Warehouse T-SQL data source

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

[Back](../index.md)
