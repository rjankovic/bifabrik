# Csvsource

[Bifabrik Index](../../../README.md#bifabrik-index) /
`src` /
[Bifabrik](../index.md#bifabrik) /
[Src](./index.md#src) /
Csvsource

> Auto-generated documentation for [src.bifabrik.src.CsvSource](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/CsvSource.py) module.

- [Csvsource](#csvsource)
  - [CsvSource](#csvsource)
    - [CsvSource().path](#csvsource()path)
    - [CsvSource().toDf](#csvsource()todf)

## CsvSource

[Show source in CsvSource.py:7](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/CsvSource.py#L7)

CSV data source

#### Signature

```python
class CsvSource(DataSource):
    def __init__(self, dataLoader): ...
```

### CsvSource().path

[Show source in CsvSource.py:15](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/CsvSource.py#L15)

Set the path (or pattern) to the source file.
It searches the Files/ folder in the current lakehouse, so
e.g. path("datasource/*.csv") will match "Files/datasource/file1.csv", "Files/datasource/file1.csv", ...

#### Signature

```python
def path(self, path: str): ...
```

### CsvSource().toDf

[Show source in CsvSource.py:23](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/CsvSource.py#L23)

#### Signature

```python
def toDf(self) -> DataFrame: ...
```