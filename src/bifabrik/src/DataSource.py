from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from bifabrik.dst.TableDestination import TableDestination

class DataSource:
    def __init__(self, dataLoader):
        self._loader = dataLoader
        dataLoader.source = self
        self._spark = self._loader.spark

    def toDf(self) -> DataFrame:
        pass

    def toTable(self, targetTableName: str) -> TableDestination:
        dst = TableDestination(self._loader, self.toDf(), targetTableName)
        return dst
