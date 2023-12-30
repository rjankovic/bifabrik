# bifabrik
Microsoft Fabric ETL toolbox - the assembly line for your lakehouse

## What is the point?
 - make BI development in Microsoft Fabric easier by providing a fluent API for common ETL tasks
 - reduce repetitive code by setting preferences in config files

## Usage
 1. First, add `bifabrik` to your Fabric environment

 2. In a Fabric notebook, create an instance of bifabrik and give it access to spark

```python
from bifabrik import bifabrik
bif = bifabrik(spark)
```

 3. All set! Now you can

```python
# read CSV or JSON easily - instead of
# df = spark.read.format("csv").option("header","true").options("inferSchema" , "true").load("Files/Sales/FactInternetSales_*.csv")
# df.write.format("delta").mode("overwrite").saveAsTable("FactInternetSales")
#
# do this

bif.fromCsv.path('Sales/FactInternetSales_*.csv').toTable('FactInternetSales')

# and you also get better schema inference than you would with PySpark, as bifabrik will use pandas to load the files :)
```

