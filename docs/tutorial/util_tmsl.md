# Semantic model utilities

Semantic models in Fabric are defined [TMSL](https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference?view=asallproducts-allversions) - basically JSON with commands to create / modify the models.

_Now_, have you ever completely broken your semantic model because
 - the refresh started failing with unspecified errors
 - multiple people were making changes at the same time
 - the underlying delta tables changed and the model editor cannot load now
 - you had multiple tabs open editing over your own changes? No? That's only me? ok...

## Backup semantic model definition

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
