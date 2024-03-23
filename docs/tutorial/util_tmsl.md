# Semantic model utilities

Semantic models in Fabric are defined [TMSL](https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference?view=asallproducts-allversions) - basically JSON with commands to create / modify the models.

_Now_, have you ever completely broken your semantic model because
 - the refresh started failing with unspecified errors
 - multiple people were making changes at the same time
 - the underlying delta tables changed and the model editor cannot load now
 - you had multiple tabs open editing over your own changes? No? That's only me? ok...

Then you may benefit from this backup / restore feature

## Backup semantic model definition

```python
import bifabrik.utils.tmslUtils as tmsl
tmsl.backupDataset(sourceDataset = 'DS_R1', sourceWorkspace = 'Experimental', targetFilePath = 'Files/DS_R1_backup.json')
```
This saves the model to a file that looks something like this:

```json
{
  "name": "SM_DQ",
  "id": "1d18df73-627b-440b-ad37-8143bce9d6a6",
  "compatibilityLevel": 1604,
  
  "model": {
    "collation": "Latin1_General_100_BIN2_UTF8",
    "defaultPowerBIDataSourceVersion": "powerBI_V3",
    
    "tables": [
      {
        "name": "DQMetric",
        "lineageTag": "84e0a2fe-ef6b-4f59-8b27-c55afff1e837",
        "sourceLineageTag": "[dbo].[DQMetric]",
        
        "columns": [
```
You can also omit the `targetPath`. In that case the library will use a generic path, creating the folders if needed

```python
import bifabrik.utils.tmslUtils as tmsl
tmsl.backupDataset(sourceWorkspace = 'Experimental', sourceDataset = 'DS_LH1_2')

# tmslBackupPathPattern: Files/tmsl_backup/{workspacename}/{datasetname}/{datasetname}_{timestamp}.json
# Writing tmsl to abfss://.../Files/tmsl_backup/Experimental/DS_LH1_2/DS_LH1_2_2024_03_21_18_25_12_954153.json
```
If you want to configure different backup paths, use these `bifabrik` config properties:

```python
bifabrik.config.metadataStorage.metadataWorkspaceName
bifabrik.config.metadataStorage.metadataLakehouseName 
bifabrik.config.metadataStorage.tmslBackupPathPattern
```

[Learn more about configuration](configuration.md)

```python
import bifabrik as bif
import bifabrik.utils.tmslUtils as tmsl

bif.config.metadataStorage.metadataWorkspaceName = 'Experimental'
bif.config.metadataStorage.metadataLakehouseName = 'Lakehouse1'

tmsl.backupDataset(sourceWorkspace = 'DataWS', sourceDataset = 'SM_Test')
```

## Restore model from backup

```python
from bifabrik.utils import fsUtils as fsu

help(fsu)
```

One notable function is `filePatternSearch`, which seems to be a bit of a blind spot in the standard `mssparkutils.fs` in Fabric

```python
from bifabrik.utils import fsUtils as fsu

fsu.filePatternSearch("fld1/*/data/*.csv")
# > ["abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf1/data/file11.csv", "abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf2/data/file21.csv", "Files/fld1/subf2/data/file22.csv"]
```

This uses `glob2` internally, but does not support the recursive pattern (`**/...`)

This utility is independent of the core `bifabrik` class - you don't need to initialize that one or pass the spark session here.

[Back](../index.md)
