"""
PowerBI dataset scripting utilities - tabular model backup & restore, removing objects, etc.
"""

import bifabrik.utils.fsUtils
import bifabrik.utils.log
import bifabrik as bif


def backupDataset(sourceWorkspace: str, sourceDataset: str, targetFileName = None):
    cfg = bif.config.metadataStorage
    print(cfg)
    print(f'metadataWorkspaceName: {cfg.metadataWorkspaceName}')
    print(f'metadataLakehouseName: {cfg.metadataLakehouseName}')
    print(f'tmslBackupPathPattern: {cfg.tmslBackupPathPattern}')
    pass