# JDBC data source

JDBC connection can come in handy when the source database is not supported by PySpark out of the box.

For example, if you need to load data from MariaDB, you could try the JDBC MySQL connector like this:

```python
import bifabrik as bif

# You will probably need to configure the user name and password.
# Passwords cannot be set directly - store in Key Vault and run the notebook with sufficient permissions.
bif.config.jdbc.jdbcUser = 'my.jdbc.user'
bif.config.security.keyVaultUrl = 'https://contoso-kv.vault.azure.net/'
bif.config.security.passwordKVSecretName = 'Contoso-MariaDb-Password'

db_host = "127.0.0.1"
db_port = "3306"
db_name = "contoso"
url = f'jdbc:mysql://{db_host}:{db_port}/{db_name}?useUnicode=true&tinyInt1isBit=FALSE&useLegacyDatetimeCode=false'

bif.fromJdbcQuery('SELECT * FROM contoso.orders').jdbcUrl(url).toTable('MariaDB_Orders').run()
```

There is a lot of flexibility with JDBC
 - The JDBC URL usually has options specific to your data source
 - If need be, specify the JDBC driver, e.g. `bif.config.jdbc.jdbcDriver = 'com.mysql.cj.jdbc.Driver'`
 - To see all the JDBC options, run `help(bif.config.jdbc)`. These correspond to their PySpark equivalents

You can save your Key Vault configuration and other repetitive setup in a file - see [Configuration](configuration.md).

[Back](../index.md)
