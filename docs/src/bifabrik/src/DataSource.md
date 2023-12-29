# Datasource

[Bifabrik Index](../../../README.md#bifabrik-index) /
`src` /
[Bifabrik](../index.md#bifabrik) /
[Src](./index.md#src) /
Datasource

> Auto-generated documentation for [src.bifabrik.src.DataSource](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/DataSource.py) module.

- [Datasource](#datasource)
  - [DataSource](#datasource)
    - [DataSource().toDf](#datasource()todf)
    - [DataSource().toTable](#datasource()totable)

## DataSource

[Show source in DataSource.py:5](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/DataSource.py#L5)

Generic data source.
Contains methods to initiate a DataDestination after you are done setting the source.
bif.sourceInit.sourceOption1(A).srcOpt2(B).toTable("Tab").destinationOption(X)....save()

#### Signature

```python
class DataSource:
    def __init__(self, dataLoader): ...
```

### DataSource().toDf

[Show source in DataSource.py:15](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/DataSource.py#L15)

Converts the data to a dataframe

#### Signature

```python
def toDf(self) -> DataFrame: ...
```

### DataSource().toTable

[Show source in DataSource.py:20](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/DataSource.py#L20)

Sets the destination table name (table in the current lakehouse)

#### Signature

```python
def toTable(self, targetTableName: str) -> TableDestination: ...
```