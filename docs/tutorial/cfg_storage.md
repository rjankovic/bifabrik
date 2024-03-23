# Cross-lakehouse pipelines

By default, `bifabrik` reads from and saves data to the default lakehouse of the notebook (that is the "pinned") lakehouse in your lakehouse.

![image](https://github.com/rjankovic/bifabrik/assets/2221666/55f81240-0f4a-4cb8-b11b-2343911b8941)

However, you may need to load data from one lakehouse and save it to another, potentially in a different workspace.

```python
import bifabrik as bif

# help(bif.config.destinationStorage)
# help(bif.config.sourceStorage)

bif.config.sourceStorage.sourceWorkspace = 'Workspace 1'
bif.config.sourceStorage.sourceLakehouse = 'LH_src'
bif.config.destinationStorage.destinationWorkspace = 'Workspace 2'
bif.config.destinationStorage.destinationLakehouse = 'LH_dst'

# here, the source files are searched in the Files/ folder in LH_src in Workspace 1
# and the tables are saved to LH_dst in Workspace 2

bif.fromJson('/JsonFiles_src/file1.json').multiLine(True).toTable('TableA').run()
bif.fromJson('/JsonFiles_src/file2.json').multiLine(True).toTable('TableB').run()
bif.fromJson('/JsonFiles_src/file3.json').multiLine(True).toTable('TableC').run()
```
> Note that cross-lakehouse SQL queries are supported, but cross-workspace queries aren't. Or at least not without creating shortcuts between them.
> 
> See https://learn.microsoft.com/en-us/fabric/data-warehouse/tutorial-sql-cross-warehouse-query-editor#execute-a-cross-warehouse-cross-workspace-query
>
> You can pull data from different workspaces, but you'll need to keep the SQL transformations and delta table destinations within one workspace, at least for now.

You can also save this configuration to a file. 

Here, the config file will be saved to the default lakehouse of the notebook - source and destination settings only affect the data pipelines, not metadata storage. Similarly, logs (if you've configured [logging](util_log.md)) will still be saved to the default lakehouse.
```python
bif.config.saveToFile('Files/bifabrik.json')
```

And then load the config when you need it.
```python
bif.config.loadFromFile('Files/bifabrik.json')
```

If you only specify the source / target lakehouse and not the workspace, `bifabrik` will look for that lakehouse in the workspace of the running notebook.

> When using the [SQL source](src_sql.md), you can simply refer to a different lakehouse as a database, without configuring the source lakehouse.

[More about configuration](configuration.md)

[Back](../index.md)
