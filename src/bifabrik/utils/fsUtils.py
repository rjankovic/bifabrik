"""
File system utilities - glob pattern search, Fabric path normalization and more
"""

import notebookutils.mssparkutils.fs
import glob2
import regex
import sempy.fabric as spf
import logging
import os
import datetime
import time

__mounts = None
__defaultWorkspaceId = spf.get_notebook_workspace_id()
__defaultWorkspaceRefName = 'default'
__defaultLakehouseId = None
__defaultLakehouseWorkspaceId = None
__defaultLakehouseRefName = 'default'
#lgr = lg.getLogger()


def normalizeFileApiPath(path: str):
    """Normalizes a file path to the form of "/lakehouse/default/Files/folder/..."
    """
    # used internally for logging to a mounted directory (int the notebook's default lakehouse)
    r = path
    if not r.startswith('/'):
        r = '/' + r

    lhpt = '/lakehouse'
    dfpt = '/default'
    fpt = '/files'
    lp = r.lower()
    if lp.startswith(lhpt):
        lp = lp[len(lhpt):]
        r = r[len(lhpt):]
    if lp.startswith(dfpt):
        lp = lp[len(dfpt):]
        r = r[len(dfpt):]
    if lp.startswith(fpt):
        lp = lp[len(fpt):]
        r = r[len(fpt):]
    
    r = '/lakehouse/default/Files' + r
    return r

def normalizeRelativeSparkPath(path: str):
    """Normalizes a file path to the form of "Files/folder/file.csv"
    """
    r = path
    if not r.startswith('/'):
        r = '/' + r
    
    lhpt = '/lakehouse'
    dfpt = '/default'
    fpt = '/files'
    lp = r.lower()
    
    lp = r.lower()
    if lp.startswith(lhpt):
        lp = lp[len(lhpt):]
        r = r[len(lhpt):]
    if lp.startswith(dfpt):
        lp = lp[len(dfpt):]
        r = r[len(dfpt):]
    if lp.startswith(fpt):
        lp = lp[len(fpt):]
        r = r[len(fpt):]
    
    r = 'Files' + r
    return r

# abfss://6dcac488-f099-451a-91f1-0945791bf22c@onelake.dfs.fabric.microsoft.com/a2939674-cc1c-4814-9094-8c16e5682502/Files/CsvFiles
def normalizeAbfsPath(path: str, lakehouseBasePath: str, inFiles = False, inTables = False):
    """Normalizes a path to the form of "abfss://{workspaceid}@onelake.dfs.fabric.microsoft.com/{lakehouseid}/Files/CsvFiles or .../Tables/Table1"
    """
    r = path

    if not r.startswith('/'):
        r = '/' + r
    
    lhpt = '/lakehouse'
    dfpt = '/default'
    fpt = '/files'
    lp = r.lower()
    
    lp = r.lower()
    if lp.startswith('abfss:'):
        return r
    if not r.startswith('/'):
        r = '/' + r
        lp = '/' + lp
    
    if inFiles and not (r.startswith('/Files')):
        r = '/Files' + r
    elif inTables and not (r.startswith('/Tables')):
        r = '/Tables' + r
    
    wPrefix = f'{lakehouseBasePath}{r}'
    return wPrefix


def normalizeAbfsFilePath(path: str, lakehouseBasePath: str):
    """Normalizes a path to the form of "abfss://{workspaceid}@onelake.dfs.fabric.microsoft.com/{lakehouseid}/Files/CsvFiles"
    You can use getLakehousePath() to get the lakehouse path as "abfss://{workspaceid}@onelake.dfs.fabric.microsoft.com"
    """
    return normalizeAbfsPath(path, lakehouseBasePath=lakehouseBasePath, inFiles = True)

def normalizeAbfsTablePath(path: str, lakehouseBasePath: str):
    """Normalizes a path to the form of "abfss://{workspaceid}@onelake.dfs.fabric.microsoft.com/{lakehouseid}/Tables/Table1"
    """
    return normalizeAbfsPath(path, lakehouseBasePath=lakehouseBasePath, inTables = True)

def filePatternSearch(path: str, lakehouse: str = None, workspace: str = None, useImplicitDefaultLakehousePath = False) -> list[str]:
    """Searches the Files/ directory of the current lakehouse
    using glob to match patterns. Returns the list of files as ABFS paths.

    Examples
    --------
    >>> bifabrik.utils.fsUtils.filePatternSearch("fld1/*/data/*.csv")
    ...     ["abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf1/data/file11.csv", "abfss://...@onelake.dfs.fabric.microsoft.com/.../Files/fld1/subf2/data/file22.csv"]

    param useImplicitDefaultLakehousePath indicates whether '/lakehouse/default' should be used to refer to the default lakehouse
    """
    
    
    log = logging.getLogger('bifabrik')
    res = []
    #pathNorm = normalizeRelativeSparkPath(path)

    lhPath = getLakehousePath(lakehouse = lakehouse, workspace = workspace, useImplicitDefaultLakehousePath=useImplicitDefaultLakehousePath)
    pathNorm = normalizeAbfsFilePath(path, lhPath)
    pathNormTrimSuffix = pathNorm[len(lhPath) + len('/Files/'):] #pathNorm[len('Files/'):]
    pathPts = pathNormTrimSuffix.split("/")
    startPath = lhPath + '/Files'
    defaultLh = '/lakehouse/default/'
    isInDefaultLakehouse = False
    if startPath.startswith(defaultLh):
        startPath = startPath[len(defaultLh):]
        isInDefaultLakehouse = True
    searchLocations = [startPath]

    if len(pathPts) == 0:
        return res
    
    for i in range(len(pathPts)):
        pathPt = pathPts[i]
        if len(searchLocations) == 0:
            return res
        nextLevel = []
        for location in searchLocations:
            log.info(f'Searching location {location}')
            if not notebookutils.mssparkutils.fs.exists(location):
                warn = f'Location {location} does not exist'
                print(warn)
                log.warning(warn)
                continue
            subLocations = notebookutils.mssparkutils.fs.ls(location)
            subLocationNames = [fi.name for fi in subLocations]
            subLocationsFilteredT = glob2.fnmatch.filter(subLocationNames, pathPt, True, False, None)
            subLocationsFiltered = [x[0] for x in subLocationsFilteredT] 
            #print(subLocationsFiltered)
            subLocationsDict = {}
            for sbl in subLocations:
                subLocationsDict[sbl.name] = sbl
            #print(subLocationsDict)
            for slf in subLocationsFiltered:
                #print(slf)
                finfo = subLocationsDict[slf]
                slfPath = location + '/' + slf
                if len(pathPts) == i + 1:
                    if finfo.isFile:
                        if isInDefaultLakehouse:
                            res.append(f'{defaultLh}{slfPath}')
                        else:
                            res.append(slfPath)
                elif (finfo.isDir):
                    nextLevel.append(slfPath)
        if len(pathPts) == i + 1:
            return res
        else:
            searchLocations = nextLevel

def fileExists(path: str, lakehouse: str = None, workspace: str = None) -> bool:
    searchResults = filePatternSearch(path = path, lakehouse = lakehouse, workspace = workspace)
    if len(searchResults) > 0:
        return True
    return False

def getMounts():
    global __mounts
    if __mounts is None:
        __mounts = notebookutils.mssparkutils.fs.mounts()
    return __mounts

def getDefaultLakehouseAbfsPath() -> str:
    for mp in getMounts():
        if mp.mountPoint == '/default':
            # print(f"Default Lakehouse is: {mp.source}")
            return mp.source
        return None
    
def isGuid(uuid):
    UUID_PATTERN = regex.compile(r'^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$', regex.IGNORECASE)
    #print(uuid)
    #print(type(uuid))
    r = bool(UUID_PATTERN.match(uuid))
    return r

def getWorkspaceAndLakehouseIdFromMountSource(mountSource):
    m = regex.match(r"abfss://(.+)@onelake.dfs.fabric.microsoft.com/(.+)", mountSource)
    return { 'workspaceId': m.captures(1)[0], 'lakehouseId': m.captures(2)[0] }

class LakehouseMeta:
    lakehouseName: str = None
    lakehouseId: str = None
    workspaceName: str = None
    workspaceId: str = None
    basePath: str = None

class LakehouseMap():

    def __init__(self, workspaceId: str, workspaceName: str):
        self.__dictById = {}
        self.__dictByName = {}
        self.workspaceId = workspaceId
        self.workspaceName = workspaceName
    
    def addLakehouse(self, lh: LakehouseMeta):
        self.__dictById[lh.lakehouseId] = lh
        self.__dictByName[lh.lakehouseName.lower()] = lh

    def __getitem__(self, key) -> LakehouseMeta:
        if isGuid(key):
            if key in self.__dictById:
                return self.__dictById[key]
            return None
        if key.lower() in self.__dictByName:
            return self.__dictByName[key.lower()]
        return None

    def __contains__(self,key):
        if self.__getitem__(key) is not None:
            return True
        return False
    
    def __iter__(self):
        for k in self.__dictById:
            yield self.__dictById[k]

class WorkspaceMap:

    def __init__(self):
        self.__dictById = {}
        self.__dictByName = {}
    
    def addWorkspace(self, lm: LakehouseMap):
        self.__dictById[lm.workspaceId] = lm
        self.__dictByName[lm.workspaceName.lower()] = lm

    def __getitem__(self, key) -> LakehouseMap:
        #print(key)
        #print(self.__dictByName)
        if isGuid(key):
            if key in self.__dictById:
                return self.__dictById[key]
            return None
        if key.lower() in self.__dictByName:
            return self.__dictByName[key.lower()]
        return None
    
    def __contains__(self,key):
        if self.__getitem__(key) is not None:
            return True
        return False
    
    def __iter__(self):
        for k in self.__dictById:
            yield self.__dictById[k]

__workspaceMap = WorkspaceMap()

defaultMount = getDefaultLakehouseAbfsPath()
if defaultMount is not None:
    wsLh = getWorkspaceAndLakehouseIdFromMountSource(defaultMount)    
    __defaultLakehouseId = wsLh['lakehouseId']
    __defaultLakehouseWorkspaceId = wsLh['workspaceId']

def mapLakehouses(workspaceId: str, workspaceName: str):
    lm = LakehouseMap(workspaceId = workspaceId, workspaceName = workspaceName)
    #__workspaceMap.addWorkspace(lm)
    #print(workspaceId)
    #print(workspaceName)
    #print('----')
    lhs = notebookutils.mssparkutils.lakehouse.list(workspaceId)
    for lh in lhs:
        l = LakehouseMeta()
        l.lakehouseName = lh.displayName
        l.lakehouseId = lh.id
        l.workspaceName = workspaceName
        l.workspaceId = workspaceId
        l.basePath = f"abfss://{workspaceId}@onelake.dfs.fabric.microsoft.com/{lh.id}"
        lm.addLakehouse(l)
    return lm

def mapWorkspaces():
    global __workspaceMap
    global __defaultWorkspaceId
    global __defaultWorkspaceRefName
    global __defaultLakehouseId
    global __defaultLakehouseRefName

    wss = spf.list_workspaces()
    #display(wss)
    #print(type(wss[wss.Type == 'Workspace']))
    for wix, ws in wss[wss.Type == 'Workspace'].iterrows():
        lm = mapLakehouses(workspaceId = ws['Id'], workspaceName = ws['Name'])
        __workspaceMap.addWorkspace(lm)

    if __defaultWorkspaceId not in __workspaceMap:
        mwslm = mapLakehouses(workspaceId = __defaultWorkspaceId, workspaceName = 'My workspace')
        __workspaceMap.addWorkspace(mwslm)
    
    return __workspaceMap

mapWorkspaces()


def getLakehouseMeta(lakehouse: str, workspace: str = None, suppressNotFound = False) -> LakehouseMeta:
    """
    Finds the lakehouse amoung available workspaces and returns its metadata
        abfss://{workspace id}@onelake.dfs.fabric.microsoft.com/{lakehouse id}/Files
    
    :param lakehouse: the lakehouse name or ID (if None, the current notebook's deault lakehouse is used)
    :param workspace: the name or ID of the workspace containg the lakehouse (if None, the workspace of the current notebook is used)
    
    :return: LakehouseMeta
    """
    global __workspaceMap
    global __defaultWorkspaceId
    global __defaultLakehouseId

    global __defaultWorkspaceRefName
    global __defaultLakehouseRefName

    # use the workspace of the notebook
    if workspace is None:
        workspace = __defaultWorkspaceId
    if workspace == __defaultLakehouseRefName:
        workspace = __defaultWorkspaceId

    # use the notebook's default lakehouse
    if lakehouse is None:
        lakehouse = __defaultLakehouseId
    if lakehouse == __defaultLakehouseRefName:
        lakehouse = __defaultLakehouseId

    errMsg = f'Could not find lakehouse {lakehouse} in workspace {workspace}'

    if lakehouse is None:
        if suppressNotFound:
            return None
        raise Exception(errMsg)
    
    lhMap = __workspaceMap[workspace]
    if lhMap is None:
        if suppressNotFound:
            return None
        raise Exception(errMsg)
    
    lh = lhMap[lakehouse]
    if lh is None:
        if suppressNotFound:
            return None
        raise Exception(errMsg)
    return lh

def currentLakehouse() -> LakehouseMeta:
    '''Info on the notebook's default (attached) lakehouse
    '''
    l =  getLakehouseMeta(lakehouse = __defaultLakehouseId, workspace = __defaultLakehouseWorkspaceId, suppressNotFound = True)
    if l is None: 
        return None
    return {
        'lakehouseName' : l.lakehouseName,
        'lakehouseId' : l.lakehouseId,
        'workspaceName' : l.workspaceName,
        'workspaceId' : l.workspaceId,
        'basePath' : l.basePath
    } 

def currentLakehouseName() -> str:
    '''Name of the notebook's default (attached) lakehouse
    '''
    clh = currentLakehouse()
    if clh is None:
        return None
    return clh['lakehouseName']

def getLakehousePath(lakehouse: str, workspace: str = None, suppressNotFound = False, useImplicitDefaultLakehousePath = False):
    """
    Returns the lakehouse path as abfss://{workspace id}@onelake.dfs.fabric.microsoft.com/{lakehouse id}
    it can then be appended as e.g
        abfss://{workspace id}@onelake.dfs.fabric.microsoft.com/{lakehouse id}/Tables
        abfss://{workspace id}@onelake.dfs.fabric.microsoft.com/{lakehouse id}/Files
    
    :param lakehouse: the lakehouse name or ID (if None, the current notebook's deault lakehouse is used)
    :param workspace: the name or ID of the workspace containg the lakehouse (if None, the workspace of the current notebook is used)
    """
    meta = getLakehouseMeta(lakehouse, workspace, suppressNotFound)
    if meta is None:
        if suppressNotFound:
            return None
        errMsg = f'Could not find lakehouse {lakehouse} in workspace {workspace}'
        raise Exception(errMsg)
    
    if meta.workspaceId == __defaultWorkspaceId and meta.lakehouseId == __defaultLakehouseId and useImplicitDefaultLakehousePath:
        return '/lakehouse/default'

    return meta.basePath

def archiveFiles(files, archiveFolder, filePattern, lakehouse = None, workspace = None):
    if len(files) == 0:
        return
    if archiveFolder is None:
        raise Exception(f'Cannot archive the file {files[0]}, as the archive folder has not been set')
    if filePattern is None:
        raise Exception(f'Cannot archive the file {files[0]}, as the archive file patter has not been set')
    
    # ABFS path to the archive folder
    lhPath = getLakehousePath(lakehouse = lakehouse, workspace = workspace)
    folderPathNorm = normalizeAbfsFilePath(archiveFolder, lhPath) #.removesuffix('/')
    if not notebookutils.mssparkutils.fs.exists(folderPathNorm):
        notebookutils.mssparkutils.fs.mkdirs(folderPathNorm)

    for file in files:
        log = logging.getLogger('bifabrik')

        fname = os.path.basename(file)
        rootName, extension = os.path.splitext(fname)

        ct = datetime.datetime.now()
        cts = ct.strftime("%Y_%m_%d_%H_%M_%S_%f")
        
        fullPattern = '{folder}/' + filePattern
        fullTargetPath = fullPattern.format(folder = folderPathNorm, filename = rootName, timestamp = cts, extension = extension)

        log.info(f'Archiving {file} -> {fullTargetPath}')
        notebookutils.mssparkutils.fs.mv(file, fullTargetPath)