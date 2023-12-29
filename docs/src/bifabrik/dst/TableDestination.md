# Tabledestination

[Bifabrik Index](../../../README.md#bifabrik-index) /
`src` /
[Bifabrik](../index.md#bifabrik) /
[Dst](./index.md#dst) /
Tabledestination

> Auto-generated documentation for [src.bifabrik.dst.TableDestination](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/dst/TableDestination.py) module.

- [Tabledestination](#tabledestination)
  - [TableDestination](#tabledestination)
    - [TableDestination().save](#tabledestination()save)

## TableDestination

[Show source in TableDestination.py:5](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/dst/TableDestination.py#L5)

Saves data to a lakehouse table.

#### Signature

```python
class TableDestination(DataDestination):
    def __init__(self, dataLoader, sourceDf: DataFrame, targetTableName: str): ...
```

### TableDestination().save

[Show source in TableDestination.py:12](https://github.com/rjankovic/bifabrik.git --create-configs/blob/main/src/bifabrik/dst/TableDestination.py#L12)

Save the data to a lakehouse table. This is the "commit" method at the end of the chain.

#### Signature

```python
def save(self): ...
```