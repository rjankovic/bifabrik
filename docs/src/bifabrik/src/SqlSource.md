# Sqlsource

[Bifabrik Index](../../../README.md#bifabrik-index) /
`src` /
[Bifabrik](../index.md#bifabrik) /
[Src](./index.md#src) /
Sqlsource

> Auto-generated documentation for [src.bifabrik.src.SqlSource](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/SqlSource.py) module.

- [Sqlsource](#sqlsource)
  - [SqlSource](#sqlsource)
    - [SqlSource().query](#sqlsource()query)
    - [SqlSource().toDf](#sqlsource()todf)

## SqlSource

[Show source in SqlSource.py:5](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/SqlSource.py#L5)

SQL (SparkSQL) data source

#### Signature

```python
class SqlSource(DataSource):
    def __init__(self, dataLoader): ...
```

### SqlSource().query

[Show source in SqlSource.py:13](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/SqlSource.py#L13)

The source SQL query (SparkSQL) to be executed against the current lakehouse

#### Signature

```python
def query(self, query: str): ...
```

### SqlSource().toDf

[Show source in SqlSource.py:19](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/src/SqlSource.py#L19)

#### Signature

```python
def toDf(self) -> DataFrame: ...
```