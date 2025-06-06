# File system utilities

As a side-effect of the library, a few filesystem tools have been created to make Fabric filesystem navigation easier. These tools can be used independently of the core `bifabrik` class. See the fsUtils module:

```python
from bifabrik.utils import fsUtils as fsu

help(fsu)
```
## `currentLakehouseName()`

A straightforward function that returns the name of the lakehouse attached to the notebook. If there is no default lakehouse mounted, returns none

```python
import bifabrik.utils.fsUtils as fsu

fsu.currentLakehouseName()
# >> 'LH_BRONZE'
```

## `currentLakehouse()`

An extended version of `currentLakehouseName()`, returning a dictionary of the attached lakehouse's metadata

```python
fsu.currentLakehouse()

# >> {'lakehouseName': 'LH_BRONZE',
# >>  'lakehouseId': 'abcdxyz-1234-abcd-qqqq-bcc12345678d',
# >>  'workspaceName': 'WS_BI',
# >>  'workspaceId': '1234567-1234-abcd-qqqq-bcc12345678d',
# >>  'basePath': 'abfss://xxxxx@onelake.dfs.fabric.microsoft.com/xxxxxx'}
```

## `getLakehouseMeta()`

find a lakehouse by workspace and lakehouse name or ID. 

If found, returns a metadata object `bifabrik.utils.fsUtils.LakehouseMeta` (with same properties as the dictionary from `currentLakehouse()`)

```python
# the workspace parameter is optional, by default this searches the current workspace
meta = fsu.getLakehouseMeta(workspace = 'WS_BI', lakehouse = 'LH_SILVER')

meta.__dict__
```
> During initialization, `bifabrik` scans the lakehouses across all workspaces and saves this  information to `spark.sparkContext._conf['bifabrik.fs.workspaceMap']` for use through the lifetime of the session.

## `filePatternSearch()`

Recursive file pattern search, supports the '*' notation

```python
from bifabrik.utils import fsUtils as fsu

fsu.filePatternSearch("fld1/*/data/*.csv")
# > ["abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf1/data/file11.csv", "abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf2/data/file21.csv", "Files/fld1/subf2/data/file22.csv"]
```

This uses `glob2` internally, but does not support the recursive pattern (`**/...`)

[Back](../index.md)
