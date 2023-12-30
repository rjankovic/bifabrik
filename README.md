# bifabrik
Microsoft Fabric ETL toolbox - the assembly line for your lakehouse

This is an **early build** - if you find a problem, please report it here: https://github.com/rjankovic/bifabrik/issues Thanks!

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

    3.1. load CSV or JSON to tables easily
```python
# instead of
# df = spark.read.format("csv")
# .option("header","true")
# .option("inferSchema" , "true")
# .load("Files/Sales/FactInternetSales_*.csv")
# df.write.format("delta").mode("overwrite").saveAsTable("FactInternetSales")
#
# do this

bif.fromCsv.path('Sales/FactInternetSales_*.csv').toTable('FactInternetSales').save()

# and you also get better schema inference than you would with PySpark, as bifabrik will use pandas to load the files :)
```
    3.2. run straightforward SQL transformations
```python
# (assume agg_sale_by_date_employee is some transformation view)
# 
# instead of
# sale_by_date_employee = spark.sql("SELECT * FROM agg_sale_by_date_employee")
# sale_by_date_employee.write.mode("overwrite")
# .format("delta").option("overwriteSchema", "true")
# .save("Tables/aggregate_sale_by_date_employee")
#
# do this

bif.fromSql.query("SELECT * FROM agg_sale_by_date_employee").toTable('aggregate_sale_by_date_employee').save()
```
### Utilities
Find files using pattern matching
```python
import bifabrik

bifabrik.utils.fsUtils.filePatternSearch("DS/*/subfolder11/*.csv")

#> ['Files/DS/subfolder1/subfolder11/sales3.csv', 'Files/DS/subfolder2/subfolder11/sales4.csv']
```
This uses `glob2` internally, but does not support the recursive pattern (`**/...`)
## General Flow
There's a lot of work to be done, but generally, it should work like this

![https://github.com/rjankovic/bifabrik/blob/main/docs/bifabrik_arch2.drawio.png](https://github.com/rjankovic/bifabrik/blob/main/docs/bifabrik_arch2.drawio.png)
