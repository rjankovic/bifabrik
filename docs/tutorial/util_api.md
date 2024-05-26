# REST API Utilities

Tools using the Fabric REST API to run / deploy workspace items

```python
from bifabrik.utils import api
help(api)
```

## Execute Fabric pipeline

You can execute a pipeline either asynchronously

```python
from bifabrik.utils import api

api.executePipeline('MyPipeline', 'MyWorkspace')
```

or synchronously wait for the pipeline to complete:

```python
api.executePipelineWait('PL_ExtractHeliosCZ', 'Data')

#> /v1/workspaces/xxxxx/items/xxxxx/jobs/instances?jobType=Pipeline
#> {'id': 'xxxxx', 'itemId': 'xxxxx', 'jobType': 'Pipeline', 'invokeType': 'Manual', 'status': 'NotStarted', 'failureReason': None, 'rootActivityId': 'xxxxx', 'startTimeUtc': '2024-05-26T15:01:55.76', 'endTimeUtc': None}
#> After 5 s: InProgress
#> After 10 s: InProgress
#> After 20 s: InProgress
#> After 40 s: InProgress
#> After 80 s: InProgress
#> After 160 s: Completed
```

Should the pipeline fail, these functions will throw an exception.

You can also use the `parameters` parameter to pass a dictionary of input parameters to the pipeline.

If you don't specify the workspace, `bifabrik` will look for the pipeline in the current workspace.

[Back](../index.md)
