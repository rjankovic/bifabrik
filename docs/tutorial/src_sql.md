# Spark SQL transformations

Let's make a simple Spark SQL transformation, writing data to another SQL table - a straightforward full load:

```python
import bifabrik as bif

bif.fromSql('''

SELECT Industry_name_NZSIOC AS Industry_Name 
,AVG(`Value`) AS AvgValue
FROM LakeHouse1.Survey2021
WHERE Variable_Code = 'H35'
GROUP BY Industry_name_NZSIOC

''').toTable('SurveySummarized').run()

# The resulting table will be saved to the lakehouse attached to your notebook.
# You can refer to a different source warehouse in the query, though.
```
`bifabrik` also implements incremental load strategies, identity columns and more - see the [table destination options](dst_table.md) to learn more.


[Back](../index.md)
