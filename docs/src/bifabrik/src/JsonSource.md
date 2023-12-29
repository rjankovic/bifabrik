# Jsonsource

[Bifabrik Index](../../../README.md#bifabrik-index) /
`src` /
[Bifabrik](../index.md#bifabrik) /
[Src](./index.md#src) /
Jsonsource

> Auto-generated documentation for [src.bifabrik.src.JsonSource](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/JsonSource.py) module.

- [Jsonsource](#jsonsource)
  - [JsonSource](#jsonsource)
    - [JsonSource().path](#jsonsource()path)
    - [JsonSource().toDf](#jsonsource()todf)

## JsonSource

[Show source in JsonSource.py:6](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/JsonSource.py#L6)

CSV data source

#### Signature

```python
class JsonSource(DataSource):
    def __init__(self, dataLoader): ...
```

### JsonSource().path

[Show source in JsonSource.py:14](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/JsonSource.py#L14)

Set the path (or pattern) to the source file.
It searches the Files/ folder in the current lakehouse, so
e.g. path("datasource/*.json") will match "Files/datasource/file1.json", "Files/datasource/file1.json", ...

#### Signature

```python
def path(self, path: str): ...
```

### JsonSource().toDf

[Show source in JsonSource.py:22](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/JsonSource.py#L22)

#### Signature

```python
def toDf(self) -> DataFrame: ...
```