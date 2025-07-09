# Data lineage

When you use `bifabrik` to transform data and save it to [lakehouse tables](/tutorial/dst_table.md), you can enable data lineage tracking. 

This will give you column-level mapping from the source to destination tables.

## How it works

When lineage is enabled, `bifabrik` parses each dataframe (or dataframe coming from a SQL query) that is being saved to a lakehouse. 

The lineage information is then saved as a JSON file in a folder in the default lakehouse where you can process it further.

The JSON files can be deserialized to `DataFrameLineage` objects that you can use to traverse the columns and their lineage.

Thus, in a typical scenario, you will be running a bunch of notebooks loading different tables with lineage enabled before processing the lineage folder e.g. once a day.

## Example

First, we enable lineage tracking and set the destination folder

```python
import bifabrik as bif

bif.config.lineage.lineageFolder = "Files/bifabrik_lineage"
bif.config.lineage.lineageEnabled = True
```

Then we can run a pipeline as per usual, in this case loading data from Bronze to Silver

```python
(
bif.fromSparkSql('''
    SELECT 
    id AS InvoiceId, 
    total * (1 + vat_percent / 100) TotalWithVAT
    FROM Bronze.is_invoice
    ''')
    .toTable('InvoicesTest')
    .destinationLakehouse('Silver')
    .run()
)
```

This will save the lineage to a file in the `lienageFolder` in the format of `{target_lakehouse}_{target_table}_{timestamp}.json`, in this case `Files/bifabrik_lineage/Silver_InvoicesTest_20250708_184955_346.json`.

You can go through the output folder regularly to process all the lineage. To deserialize the file, use `DataFrameLineage` from `bifabrik.utils.lineage.DataFrameLineage`

```python
from bifabrik.utils.lineage.DataFrameLineage import DataFrameLineage
 
file_path = f'/lakehouse/default/Files/bifabrik_lineage/Silver_InvoicesTest_20250708_184955_346.json'
with open(file_path, "r") as file:
    file_content = file.read()

df_lineage = DataFrameLineage.fromJson(file_content)

print(df_lineage)
```

The output can look like this - showing that e.g. TotalWithVAT depends on the two columns from spark catalog and goes through a transformation in the query:
```
InvoiceId#2314 'InvoiceId'
--- `spark_catalog`.`bronze`.`is_invoice`.`id`#2317
--- InvoiceId#2314 'spark_catalog.Bronze.is_invoice.id AS InvoiceId'
TotalWithVAT#2315 'TotalWithVAT'
--- `spark_catalog`.`bronze`.`is_invoice`.`total`#2345
--- `spark_catalog`.`bronze`.`is_invoice`.`vat_percent`#2337
--- TotalWithVAT#2315 '(spark_catalog.Bronze.is_invoice.total * (CAST(1 AS DECIMAL(1,0)) + (spark_catalog.Bronze.is_invoice.vat_percent / CAST(100 AS DECIMAL(3,0))))) AS TotalWithVAT'
```

You can use the `DataFrameLineage` like this

```python
# check for parsing errors - this shouldn't happen too often though :)
if df_lineage.error is not None:
    print(df_lineage.error) 

for c in df_lineage.columns:
    print(f'output column {c.name}')

    # this only contains the catalog columns (not the intermediate transformations as seen above)
    for dep in c.tableColumnDependencies:
        print(f'--- depends on {dep.dbName}.{dep.tableName}.{dep.name}') 
```

You can get further context about the notebook execution, which can help with processing the lineage data.
```python
# the context of the bifabrik load execution
print(df_lineage.context)
```

The context has the following properties:
```
{
    "executionDateTime": 1752000595.3460455,
    "workspaceName": "Workspace of the notebook",
    "workspaceId": "f899434f-...",
    "targetLakehouseName": "Silver",
    "targetTableName": "InvoicesTest",
    "notebookName": "NB_Lineage_Sample",
    "notebookId": "4ba8abda-...",
    "userName": "Radovan Jankovi\u010d",
    "userId": "3026f5e8-..."
}
```

> Note that issues can happen when parsing the lineage. The tool is now focused on transformations between lakehouse tables, so it can run into issues if you, for example, read data from XML or some other format into a dataframe. In that case, the dependencies will not be contained in the output file. Instead, you may find an error message in the `error` property (as seen above).
> 
> If you encounter issues or need to track data lineage from sources other than tables, you can report them through the [GitHub issues page](https://github.com/rjankovic/bifabrik/issues).

[Back](../index.md)
