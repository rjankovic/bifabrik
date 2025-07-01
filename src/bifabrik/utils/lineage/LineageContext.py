from datetime import datetime
import time
import json
import notebookutils.mssparkutils

class LineageContext:
    """Metadata about the notebook and bifabrik pipeline execution context"""
    def __init__(self, targetLakehouseName: str = None, targetTableName: str = None):
        self.__executionDateTime = time.time()
        self.__workspaceName = notebookutils.mssparkutils.runtime.context['currentWorkspaceName']
        self.__workspaceId = notebookutils.mssparkutils.runtime.context['currentWorkspaceId']
        self.__targetLakehouseName = notebookutils.mssparkutils.runtime.context['defaultLakehouseName'] if targetLakehouseName is None else targetLakehouseName
        self.__targetTableName = targetTableName
        self.__notebookName = notebookutils.mssparkutils.runtime.context['currentNotebookName']
        self.__notebookId = notebookutils.mssparkutils.runtime.context['currentNotebookId']
        self.__userName = notebookutils.mssparkutils.runtime.context['userName']
        self.__userId = notebookutils.mssparkutils.runtime.context['userId']

    @property
    def executionDateTime(self):
        """Execution time of the bifabrik table load"""
        edt = self.__executionDateTime
        return datetime.fromtimestamp(edt)

    @property
    def workspaceName(self):
        """Workspace in which the load was executed"""
        return self.__workspaceName

    @property
    def workspaceId(self):
        """Workspace in which the load was executed"""
        return self.__workspaceId

    @property
    def targetLakehouseName(self):
        """The target lakehouse to which bifabrik saved this data"""
        return self.__targetLakehouseName

    @property
    def targetTableName(self):
        """The target table to which bifabrik saved this data"""
        return self.__targetTableName

    @property
    def notebookName(self):
        """The name of the notebook in which bifabrik was executed"""
        return self.__notebookName

    @property
    def notebookId(self):
        """The ID of the notebook in which bifabrik was executed"""
        return self.__notebookId

    @property
    def userName(self):
        """The name of the user who executed the bifabrik table load"""
        return self.__userName

    @property
    def userId(self):
        """The ID of the user who executed the bifabrik table load"""
        return self.__userId

    # Serialization to JSON
    def to_json_object(self):
        return {
            "executionDateTime": self.__executionDateTime,
            "workspaceName": self.__workspaceName,
            "workspaceId": self.__workspaceId,
            "targetLakehouseName": self.__targetLakehouseName,
            "targetTableName": self.__targetTableName,
            "notebookName": self.__notebookName,
            "notebookId": self.__notebookId,
            "userName": self.__userName,
            "userId": self.__userId
        }
    
    def __str__(self):
        return json.dumps(self.to_json_object(), indent=4)

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        #data = json.loads(json_str)
        instance = cls(targetLakehouseName=data.get("targetLakehouseName"), targetTableName=data.get("targetTableName"))
        instance.__executionDateTime = data.get("executionDateTime", time.time())
        instance.__workspaceName = data.get("workspaceName")
        instance.__workspaceId = data.get("workspaceId")
        instance.__notebookName = data.get("notebookName")
        instance.__notebookId = data.get("notebookId")
        instance.__userName = data.get("userName")
        instance.__userId = data.get("userId")
        return instance