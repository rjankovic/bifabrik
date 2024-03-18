"""
PowerBI dataset scripting utilities - tabular model backup & restore and some dataset modifications.
"""

import bifabrik.utils.fsUtils as fsu
import bifabrik.utils.log as lg
import bifabrik as bif
import datetime
import sempy.fabric as spf
import notebookutils.mssparkutils.fs

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
    
    #print(f'Writing TMSL to {targetFilePath}')
    notebookutils.mssparkutils.fs.put(targetFilePath, tmsl)