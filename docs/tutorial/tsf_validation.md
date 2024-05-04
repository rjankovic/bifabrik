# Data validation

Often, you will need to validate your data before loading it into the next layer of your lakehouse. This can involve various scenarios - comparing different lakehouses, validating data types, business rules, etc.

So as not to restrict your validation logic, `bifabrik` only gets involved in processing the test results. The __validation transformation__ checks the values in specific columns indicating errorneous records. If errors / warnings are found, these can either fail the pipeline or not the issue to the [log file](util_log.md).

Let's have a look at an example to see what we mean by this.

## Basic example

In this example, the `Value` column is a `string` that we need to convert to `double`. If that fails, we want to throw an error.

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
It so happens that a few rows have the value `'C'` in the `Value` column. Thus, the pipeline fails:

![image](https://github.com/rjankovic/bifabrik/assets/2221666/51683a94-9978-487e-a3a0-1bb621add4f7)

The errors are logged to the console, as well as to the log file. The log contains the `ValidationMessage` and the row values:

```
...
2024-05-03 22:20:36,001	INFO	Executing SQL source: 
WITH values AS(
SELECT AnnualSurveyID
,Variable_code, Value
,CAST(REPLACE(Value, ',', '') AS DOUBLE) ValueDouble 
FROM AnnualSurvey
)
SELECT v.*
,IF(v.ValueDouble IS NULL, 'Error', 'OK') AS ValidationResult
,IF(v.ValueDouble IS NULL, CONCAT('"', Value, '" cannot be converted to double'), 'OK') AS ValidationMessage
FROM values v
WHERE v.ValueDouble IS NULL

2024-05-03 22:20:56,908	INFO	Executing Data validation \`T1'
2024-05-03 22:21:03,988	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 866	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,988	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 868	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 758	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 760	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 1098	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 1232	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 1114	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 1248	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1: "C" cannot be converted to double; `AnnualSurveyID`: 2817	`Variable_code`: H36	`Value`: C	`ValueDouble`: None	
2024-05-03 22:21:03,989	ERROR	Test T1 failed: "C" cannot be converted to double; `AnnualSurveyID`: 866	`Variable_code`: H35	`Value`: C	`ValueDouble`: None	
Traceback (most recent call last):
  File "/home/trusted-service-user/cluster-env/clonedenv/lib/python3.10/site-packages/bifabrik/base/Pipeline.py", line 64, in _executeUpToIndex
    tsk.execute(prevResult)
  File "/home/trusted-service-user/cluster-env/clonedenv/lib/python3.10/site-packages/bifabrik/tsf/ValidationTransformation.py", line 101, in execute
    raise Exception(f'Test {self.__testName} failed: {error[self.__messageColumnName]}; {self.rowToStr(error)}')
Exception: Test T1 failed: "C" cannot be converted to double; `AnnualSurveyID`: 866	`Variable_code`: H35	`Value`: C	`ValueDouble`: None
...
```


Note that most of this code is just a SparkSQL query. If we removed the `.validate('NumberParsingTest')` line, it would be just a regular [SQL source](src_sql.md) saving data to a [table destination](dst_table.md). And if it wasn't for the errors, it would have done just that with the validation included as well.

By default, the validation transformation is configured to look for the `ValidationResult` column in the incoming data. Then it recognizes two values in this column - `'Error'` and `'Warning'`. Both errors and warnings are logged. Then, if there are some errors, the pipeline fails. If there are only warnings, they are still written to the log and console, but the pipeline continues. The validation transformation *does not change the data*, so if the data makes it throiugh the validation without failing the pipeline, it continues to the data destination / further transformations unaffected.

There is another column that the validation uses - the `ValidationMessage`. This is included in the exception / log message for records where the validation result is Error / Warning.

## Validation settings

[Back](../index.md)
