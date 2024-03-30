# Other data sources

## Excel

Works similarly to other file sources. Uses `pandas` to load the excel file.
```python
import bifabrik as bif

bif.fromExcel('ExcelFiles/Custoemrs.xlsx').sheetName('CustomerList').toTable('Customers').run()

# show other options
help(bif.config.excel)
```

## SharePoint list

Load a list from SharePoint Online (Office 365).

`bifabrik` needs credentials to access SharePoint. For this to be secure, the credentials will be stored in Key Vault secrets and `bifabrik` configuration only refers to those secrets. Thus, you will need to configure security as below - the Key Vault URL, login and password secret names.

```python
import bifabrik as bif

bif.config.security.keyVaultUrl = 'https://kv-fabrik.vault.azure.net/'
bif.config.security.loginKVSecretName = 'SharePointLogin'
bif.config.security.passwordKVSecretName = 'SharePointPwd'

bif.fromSharePointList('https://fabrik.sharepoint.com/sites/BusinessIntelligence', 'CustomerList') \
    .toTable('Customers').run()
```

[Back](../index.md)
