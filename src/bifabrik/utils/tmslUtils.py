"""
PowerBI dataset scripting utilities - tabular model backup & restore and some dataset modifications.
"""

import bifabrik.utils.fsUtils as fsu
import bifabrik.utils.log as lg
import bifabrik as bif
import datetime
import sempy.fabric as spf
import pyspark.sql.session as pss
import notebookutils.mssparkutils.fs
from pyspark.sql.functions import col
import uuid

def backupDataset(sourceDataset: str, sourceWorkspace: str, targetFilePath = None):
    """Save the dataset definition to a TMSL script. This serves as a backup of the definition. If you damage your semantic model, you can restore it from TMSL using restoreDataset().

    Parameters:
        sourceDataset (str):    Name or ID of the dataset to backup
        sourceWorkspace (str):  Name or ID of the workspace where the dataset is located
        targetFilePath (str):   Path to which the backup will be saved. If this is not specified, the bifabrik configuration will be used to find the metadata storage
                                and construct the file pattern
                                (see
                                    bifabrik.config.metadataStorage.metadataWorkspaceName, 
                                    bifabrik.config.metadataStorage.metadataLakehouseName, 
                                    bifabrik.config.metadataStorage.tmslBackupPathPattern)
    
    Internally, this function uses the semantic link library: https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#sempy-fabric-get-tmsl

    Examples
    --------
    
    This example saves a the dataset definition to Files/SM_Test_backup.json in the default lakehouse. The notebook that runs this needs to have a default lakehouse selected.
    
    >>> import bifabrik.utils.tmslUtils as tmsl
    >>> tmsl.backupDataset(sourceWorkspace = 'DataWS', sourceDataset = 'SM_Test', targetFilePath = 'Files/SM_Test_backup.json')

    This example uses the metadataStorage configuration of bifabrik to set the location for dataset backups.
    
    >>> import bifabrik as bif
    >>> import bifabrik.utils.tmslUtils as tmsl
    >>>
    >>> bif.config.metadataStorage.metadataWorkspaceName = 'Experimental'
    >>> bif.config.metadataStorage.metadataLakehouseName = 'Lakehouse1'
    >>>
    >>> tmsl.backupDataset(sourceWorkspace = 'DataWS', sourceDataset = 'SM_Test')

    The resulting backup path can look like this: abfss://6dcac488-.../Files/tmsl_backup/DataWS/SM_Test/SM_Test_2024_03_18_17_34_52_472325.json
    """
    cfg = bif.config.metadataStorage
    
    #print(cfg)
    #print(f'metadataWorkspaceName: {cfg.metadataWorkspaceName}')
    #print(f'metadataLakehouseName: {cfg.metadataLakehouseName}')
    #print(f'tmslBackupPathPattern: {cfg.tmslBackupPathPattern}')

    ct = datetime.datetime.now()
    cts = ct.strftime("%Y_%m_%d_%H_%M_%S_%f")
    
    if targetFilePath is None:
        metaLakehouseBasePath = fsu.getLakehousePath(lakehouse = cfg.metadataLakehouseName, workspace = cfg.metadataWorkspaceName)
        targetPathReplace = cfg.tmslBackupPathPattern.format(workspacename = sourceWorkspace, datasetname = sourceDataset, timestamp = cts)
        targetFilePath = fsu.normalizeAbfsFilePath(targetPathReplace, metaLakehouseBasePath)

    tmsl = spf.get_tmsl(dataset = sourceDataset, workspace = sourceWorkspace)
    
    print(f'Saving backup to {targetFilePath}')
    notebookutils.mssparkutils.fs.put(targetFilePath, tmsl)

def restoreDataset(sourceFilePath: str, targetDatasetName: str, targetWorkspaceName: str):
    """Restore a dataset from backup. For creating the backup, use the backupDataset() function.
    If a dataset with the target name already exists in the workspace, it will be overwritten.
    If it does not exist, a new dataset will be created.

    Parameters:
        sourceFilePath (str):       Relative Spark path to the backup created by backupDataset()
        targetDatasetName (str):    Name of the restored dataset
        targetWorkspaceName (str):  Name of the workspace where to place the dataset.
    
    Examples
    --------
    >>> import bifabrik.utils.tmslUtils as tmsl
    >>> tmsl.restoreDataset(sourceFilePath = 'Files/tmsl_backup/Experimental/DS_R1/DS_R1_2024_03_22_22_05_57_580830.json', targetDatasetName = 'DS_R1_restored', targetWorkspaceName = 'Experimental')
    """
    datasets_pandas = spf.list_datasets(workspace = targetWorkspaceName)
    spark = pss.SparkSession.builder.getOrCreate()
    datasets = spark.createDataFrame(datasets_pandas)

    target_id = str(uuid.uuid4())
    
    # look for the destination dataset. If it already exists, take its ID (replace target)
    # otherwise, we're creating a new dataset

    ds_filter = datasets.filter(col("Dataset Name") == targetDatasetName)
    create_new = True
    if ds_filter.count() > 0:
        target_id = ds_filter.collect()[0]['Dataset ID']
        create_new = False
    
    rdd = spark.read.text(sourceFilePath)
    tmsl_def = ""
    name_replaced = False
    id_replaced = False
    
    for l in rdd.collect():
        s = l[0]
        if s.strip().startswith('"name":') and name_replaced == False:
            # print('orig ' + s)
            s = f'"name": "{targetDatasetName}",'
            name_replaced = True
            # print(s)
        if s.strip().startswith('"id":') and id_replaced == False:
            # print('orig ' + s)
            s = f'"id": "{target_id}",'
            id_replaced = True
            # print(s)
        tmsl_def = tmsl_def + '\n' + s

    wrap_start = f"""{{
    "createOrReplace": {{
        "object": {{
        "database": "{targetDatasetName}"
        }},
        "database":"""
    wrap_end = " } }"
    tmsl_cmd = wrap_start + tmsl_def + wrap_end

    if create_new:
        print(f'Creating dataset {targetDatasetName} in workspace {targetWorkspaceName}')
    else:
        print(f'Overwriting dataset {targetDatasetName} in workspace {targetWorkspaceName}')
    
    spf.execute_tmsl(script = tmsl_cmd, workspace = targetWorkspaceName)