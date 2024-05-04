# Data validation

Often, you will need to validate your data before loading it into the next layer of your lakehouse. This can involve various scenarios - comparing different lakehouses, validating data types, business rules, etc.

So as not to restrict your validation logic, `bifabrik` only gets involved in processing the test results. The __validation transformation__ checks the values in specific columns indicating errorneous records. If errors / warnings are found, these can either fail the pipeline or not the issue to the [log file](util_log.md).

Let's have a look at an example to see what we mean by this.

## Basic example

```python
import bifabrik as bif

bif.config.log.logPath = '/log/log.csv'
bif.config.log.errorLogPath = '/log/error_log.csv'

bif.fromSql('''
WITH values AS(
    SELECT AnnualSurveyID
    ,Variable_code, Value
    ,CAST(REPLACE(Value, ',', '') AS DOUBLE) ValueDouble 
    FROM LAKEHOUSE1.AnnualSurvey
)
    SELECT v.*
    ,IF(v.ValueDouble IS NULL, 'Error', 'OK') AS ValidationResult
    ,IF(v.ValueDouble IS NULL, CONCAT('"', Value, '" cannot be converted to double'), 'OK') AS ValidationMessage
    FROM values v
''')
.validate('NumberParsingTest') \
.toTable('SurveyValuesParsed').run()
```
Note that most of this code is just a SparkSQL query. If we removed the `.validate('NumberParsingTest')` line, it would be just a regular [SQL source](src_sql.md) saving data to a [table destination](dst_table.md).

By default, the validation transformation is configured to look for the `ValidationResult` column in the incoming data. Then it recognizes two values in this column - `'Error'` and `'Warning'`. Both errors and warnings are logged. Then, if there are some errors, the pipeline fails. If there are only warnings, they are still written to the log and console, but the pipeline continues. The validation transformation *does not change the data*, so if the data makes it throiugh the validation without failing the pipeline, it continues to the data destination / further transformations unaffected.

[Back](../index.md)
