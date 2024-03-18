"""
PowerBI dataset scripting utilities - tabular model backup & restore, removing objects, etc.
"""

import bifabrik.utils.fsUtils as fsu
import bifabrik.utils.log as lg
import bifabrik as bif
import datetime
import sempy.fabric as spf
import notebookutils.mssparkutils.fs

def backupDataset(sourceDataset: str, sourceWorkspace: str, targetFilePath = None):
    cfg = bif.config.metadataStorage
    
    print(cfg)
    print(f'metadataWorkspaceName: {cfg.metadataWorkspaceName}')
    print(f'metadataLakehouseName: {cfg.metadataLakehouseName}')
    print(f'tmslBackupPathPattern: {cfg.tmslBackupPathPattern}')

    ct = datetime.datetime.now()
    cts = ct.strftime("%Y_%m_%d_%H_%M_%S_%f")
    
    if targetFilePath is None:
        metaLakehouseBasePath = fsu.getLakehousePath(lakehouse = cfg.metadataLakehouseName, workspace = cfg.metadataWorkspaceName)
        targetPathReplace = cfg.tmslBackupPathPattern.format(workspacename = sourceWorkspace, datasetname = sourceDataset, timestamp = cts)
        targetFilePath = fsu.normalizeAbfsFilePath(targetPathReplace, metaLakehouseBasePath)

    tmsl = spf.get_tmsl(dataset = sourceDataset, workspace = sourceWorkspace)
    
    print(f'Writing TMSL to {targetFilePath}')
    notebookutils.mssparkutils.fs.put(targetFilePath, tmsl)