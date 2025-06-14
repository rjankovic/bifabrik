from datetime import datetime
import time
import notebookutils.mssparkutils

class LineageContext:
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
        edt = self.__executionDateTime
        return datetime.fromtimestamp(edt)

    @property
    def workspaceName(self):
        return self.__workspaceName

    @property
    def workspaceId(self):
        return self.__workspaceId

    @property
    def targetLakehouseName(self):
        return self.__targetLakehouseName

    @property
    def targetTableName(self):
        return self.__targetTableName

    @property
    def notebookName(self):
        return self.__notebookName

    @property
    def notebookId(self):
        return self.__notebookId

    @property
    def userName(self):
        return self.__userName

    @property
    def userId(self):
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