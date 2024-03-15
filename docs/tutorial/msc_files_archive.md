# Archiving processed source files

When loading data from files, such as [CSV](src_csv.md) or [JSON](src_json.md), you may want to loaded files to an archive folder so that you don't reprocess them the next time you run the pipeline.

```
2024-03-02 19:26:02,437	INFO	Executing CSV source: CsvFiles/dimBranch.csv
2024-03-02 19:26:02,437	INFO	Searching location Files
2024-03-02 19:26:02,528	INFO	Searching location Files/CsvFiles
2024-03-02 19:26:02,562	INFO	Loading CSV files: [/lakehouse/default/Files/CsvFiles/dimBranch.csv]
2024-03-02 19:26:02,595	INFO	Executing Table destination: DimBranchArch
2024-03-02 19:26:05,024	INFO	Archiving abfss://.../Files/CsvFiles/dimBranch.csv -> abfss://.../Files/CsvArchive/processed_dimBranch_2024_03_02_19_26_05_024146.csv
```



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
