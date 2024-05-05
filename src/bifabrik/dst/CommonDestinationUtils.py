import datetime
from pyspark.sql.functions import *

def addNARecord(df, spark, targetTableName: str, identityColumn:str ):
        na_data = []
        col_names = []
        casts = []

        for f in df.dtypes:
            col_name = f[0]
            col_type = f[1]
            
            if col_name == identityColumn:
                na_data.append(-1)
            elif col_type == 'string' or col_type.startswith('char') or col_type.startswith('varchar'):
                na_data.append('N/A')
            elif col_type == 'date' or col_type == 'timestamp':
                na_data.append('2000-01-01')
            else:
                na_data.append(0)
            casts.append(col(col_name).cast(col_type))
            col_names.append(col_name)
        
        na_df = spark.createDataFrame([tuple(na_data)],col_names)
        cast_df = na_df.select(*casts)
        union_df = cast_df.union(df)
        return union_df
        