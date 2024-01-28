import notebookutils.mssparkutils.fs
import glob2

def filePatternSearch(path: str) -> list[str]:
    """Searches the Files/ directory of the current lakehouse
    using glob to match patterns. Returns the list of files as relative Spark paths.

    Examples
    --------
    >>> bifabrik.utils.fsUtils.filePatternSearch("fld1/*/data/*.csv")
    ...     ["Files/fld1/subf1/data/file11.csv", "Files/fld1/subf2/data/file21.csv", "Files/fld1/subf2/data/file22.csv"]
    """
    res = []
    pathPts = path.split("/")
    searchLocations = ["Files"]
    if len(pathPts) == 0:
        return res
    
    for i in range(len(pathPts)):
        pathPt = pathPts[i]
        if len(searchLocations) == 0:
            return res
        nextLevel = []
        for location in searchLocations:
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
                        res.append(slfPath)
                elif (finfo.isDir):
                    nextLevel.append(slfPath)
        if len(pathPts) == i + 1:
            return res
        else:
            searchLocations = nextLevel

def normalizeFileApiPath(path: str):
    """Normalizes a file path to the form of "/lakehouse/default/Files/folder/..."
    """
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

def getDefaultLakehouseAbfsPath() -> str:
    for mp in notebookutils.mssparkutils.fs.mounts():
        # print(mp.mountPoint)
        # print(mp.source)
        # print('-----')
        if mp.mountPoint == '/default':
            # print(f"Default Lakehouse is: {mp.source}")
            return mp.source
        return None