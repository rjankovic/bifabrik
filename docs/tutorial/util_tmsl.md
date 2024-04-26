# Semantic model utilities

Semantic models in Fabric are defined in [TMSL](https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference?view=asallproducts-allversions) - basically JSON with commands to create / modify the models.

It's often convenient that semantic models are saved automatically as you make changes in the Fabric web interface, but on the other hand
 - there is not an easy way back if you make a mistake in your model (if you enable git integration in the workspace, you get the model versioned, but how do you use that model.bim file from git?)
 - if you want to separate your DEV and PROD environments and set up some sort of CI/CD, how do you deploy from one environment to the other?
 
In other words, you may benefit from this backup / restore feature, scripting your models out to JSON, and deploying them from said JSON.

Internally, this uses functions from the [sempy.fabric](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python) library.

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
import bifabrik.utils.tmslUtils as tmsl
tmsl.restoreDataset(sourceFilePath = 'Files/DS_R1_2024_03_22_22_05_57_580830.json',
   targetDatasetName = 'SM_R1', targetWorkspaceName = 'Experimental')
```

This function issues a [CreateOrReplace command](https://learn.microsoft.com/en-us/analysis-services/tmsl/createorreplace-command-tmsl?view=asallproducts-allversions) containing the definition from a backup (see above).

If the target dataset already exists, it is overwritten. If it doesn't exist, a new dataset is created.


After restoring the model, you may see a warning sign next to your tables like this:

![image](https://github.com/rjankovic/bifabrik/assets/2221666/9fb90a60-eee8-497a-85be-2c27927d6343)

Usually, this disappears after you refresh the dataset.



[Back](../index.md)
