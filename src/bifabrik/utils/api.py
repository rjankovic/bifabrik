"""
Utilities using the Fabric REST API:
 - running pipelines
"""


import bifabrik.utils.fsUtils as fsu
import bifabrik.utils.log as lg
import bifabrik as bif
import sempy.fabric as spf
import time

def executePipeline(pipeline: str, workspace: str = None, parameters: dict = {}, waitForExit: bool = False):
    """Execute a Fabric pipeline

    Parameters:
        pipeline (str):     Name or ID of the pipeline
        workspace (str):    Name or ID of the workspace where the pipeline is located (default None = current workspace)
        parameters (dict):  Dictionary of parameters to pass to the pipeline (default {})
        waitForExit (bool): Wait for the pipeline to exit (default False)
    """
    workspaceId = fsu.getWorkspaceId(workspace)
    if workspaceId is None:
        raise Exception(f'The workspace {workspace} was not found')
    
    pipelineId = None
    client = spf.FabricRestClient()

    if fsu.isGuid(pipeline):
        pipelineId = pipeline
    else:
        its = client.get(f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items?type=DataPipeline')
        for i in its.json()['value']:
            if i['displayName'] == pipeline:
                pipelineId = i['id']
        
        if pipelineId is None:
            raise Exception(f'Pipeline {pipeline} not found')
        
        jobType = f"Pipeline"
        relativeUrl = f"/v1/workspaces/{workspaceId}/items/{pipelineId}/jobs/instances?jobType={jobType}"
        print(relativeUrl)
        response = None
        if len(parameters) == 0:
            response = client.post(relativeUrl)
        else:
            payload = {
                "executionData": {
                    "parameters": parameters
                }    
            }
            response = client.post(relativeUrl, json = payload)
        
        location = response.headers['Location']

        progress = client.get(location).json()
        print(progress)

        if not waitForExit:
            return progress
        
        status = progress['status']

        waitTime = 5
        totalWait = 0
        while status in ['NotStarted', 'InProgress']:
            time.sleep(waitTime)
            totalWait = totalWait + waitTime
            if totalWait > 5:
                waitTime = waitTime * 2
            progress = client.get(location).json()
            status = progress['status']
            print(f'After {totalWait} s: {status}')
        
        if status == 'Completed':
            return
        
        raise Exception(f'Pipeline ended with status {progress}')
        
    #  {'id': 'a7385224-4b19-48de-99e6-e02c8077efbc',
    #  'itemId': '0e56ed7f-ccfc-4d1f-a5d6-dad14b8ebb79',
    #  'jobType': 'Pipeline',
    #  'invokeType': 'Manual',
    #  'status': 'InProgress',
    #  'failureReason': None,
    #  'rootActivityId': 'fa2c78a5-c643-41bd-b35c-4630a37dbc6a',
    #  'startTimeUtc': '2024-05-26T11:08:13.95',
    #  'endTimeUtc': None}

def executePipelineWait(pipeline: str, workspace: str = None, parameters: dict = {}):
    """Execute a Fabric pipeline and wait for it to exit

    Parameters:
        pipeline (str):     Name or ID of the pipeline
        workspace (str):    Name or ID of the workspace where the pipeline is located (default None = current workspace)
        parameters (dict):  Dictionary of parameters to pass to the pipeline (default {})
    """
    executePipeline(pipeline = pipeline, workspace = workspace, parameters = parameters, waitForExit = True)


