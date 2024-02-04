from bifabrik.src.DataSource import DataSource
from bifabrik.cfg.CsvSourceConfiguration import CsvSourceConfiguration
from pyspark.sql.dataframe import DataFrame
import pandas as pd
from bifabrik.utils import fsUtils

class CsvSource(DataSource, CsvSourceConfiguration):
    """CSV data source
    uses the pandas loaders
    supports configuration options from https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    
    Examples
    --------
    > from bifabrik import bifabrik
    > bif = bifabrik(spark)
    > bif.fromCsv.path('DATA/factOrderLine*.csv').delimiter(';').decimal(',').toTable('FactOrderLine').run()
    """
    
    def __init__(self, parentPipeline):
        super().__init__(parentPipeline)
        CsvSourceConfiguration.__init__(self)
        self._path = ""
    
    def path(self, path: str):
        """Set the path (or pattern) to the source file.
        It searches the Files/ folder in the current lakehouse
        """
        self._path = path
        return self
    
    def execute(self, input):
        mergedConfig = self._pipeline.configuration.mergeToCopy(self)

        source_files = fsUtils.filePatternSearch(self._path)
        fileDfs = []
        for src_file in source_files:
            csv_path = f'/lakehouse/default/{src_file}'
            pd_df = pd.read_csv(
                filepath_or_buffer = csv_path, 
                encoding = mergedConfig.fileSource.encoding,
                delimiter = mergedConfig.csv.delimiter,
                header = mergedConfig.csv.header,
                thousands = mergedConfig.csv.thousands,
                decimal = mergedConfig.csv.decimal,
                quotechar = mergedConfig.csv.quotechar,
                quoting = mergedConfig.csv.quoting,
                escapechar = mergedConfig.csv.escapechar
                )
            df = self._spark.createDataFrame(pd_df)
            fileDfs.append(df)
        if len(fileDfs) == 0:
            return None
        df = fileDfs[0]
        if len(fileDfs) > 1:
            for i in range(1, len(fileDfs)):
                df = df.union(fileDfs[i])
        
        self._result = df
        self._completed = True