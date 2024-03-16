# Archiving processed source files

When loading data from files, such as [CSV](src_csv.md) or [JSON](src_json.md), you may want to loaded files to an archive folder so that you don't reprocess them the next time you run the pipeline.

A sample configuration can look like this

```python
import bifabrik as bif

bif.config.fileSource.moveFilesToArchive = True
bif.config.fileSource.archiveFolder = 'Files/CsvArchive'
bif.config.fileSource.archiveFilePattern = 'processed_{filename}_{timestamp}{extension}'
```

Then you can load the file as usual

```python
bif.fromCsv('CsvFiles/dimBranch.csv').delimiter(';').toTable('DimBranchArch').run()
```

With the archiving feature configured, the pipeline will remember which files were loaded. After the pipeline finishes (by saving data to the destination table), the used source files will be moved to the folder specified, here `Files/Csv_Archive`. Based on the archive file pattern, the file path can look like this: `Files/CsvArchive/processed_dimBranch_2024_03_02_19_09_44_722291.csv (preview)`. This is a location within the source lakehouse.
You can use these placeholders in the file name pattern:
```
{filename}  : source file name without suffix
{extension} : source file name extension, including the '.'
{timestamp} : current timestamp as '%Y_%m_%d_%H_%M_%S_%f'
```
The files will only be moved if the pipeline succeeds. Should it fail, your files will remain in the source folder.

For info on all the general file loading config options, see

```python
help(bif.config.fileSource)
```

You may also want to [configure logging](util_log.md) like this:

```python
import bifabrik as bif
from bifabrik.utils import log
from bifabrik.cfg.specific.LogConfiguration import LogConfiguration

cfg = LogConfiguration()

cfg.logPath = '/log/archive_test_log.csv'
cfg.loggingLevel = 'INFO'
logger = log.configureLogger(cfg)

bif.config.fileSource.moveFilesToArchive = True
bif.config.fileSource.archiveFolder = 'Files/CsvArchive'
bif.config.fileSource.archiveFilePattern = 'processed_{filename}_{timestamp}{extension}'
bif.fromCsv('CsvFiles/dimBranch.csv').delimiter(';').toTable('DimBranchArch').run()
```

With this setup in place, bifabrik can log a nice summary of what the archiving has done

```
2024-03-02 19:26:02,437	INFO	Executing CSV source: CsvFiles/dimBranch.csv
2024-03-02 19:26:02,437	INFO	Searching location Files
2024-03-02 19:26:02,528	INFO	Searching location Files/CsvFiles
2024-03-02 19:26:02,562	INFO	Loading CSV files: [/lakehouse/default/Files/CsvFiles/dimBranch.csv]
2024-03-02 19:26:02,595	INFO	Executing Table destination: DimBranchArch
2024-03-02 19:26:05,024	INFO	Archiving abfss://.../Files/CsvFiles/dimBranch.csv -> abfss://.../Files/CsvArchive/processed_dimBranch_2024_03_02_19_26_05_024146.csv
```

And, if you don't feel like repeating these configuration steps each, time, save them to a file

```python
bif.config.saveToFile('Files/cfg/fileSrcSettings.json')

# and then load it...

bif.config.loadFromFile('Files/cfg/fileSrcSettings.json')
```

See more in [Configuration](configuration.md)

[Back](../index.md)
