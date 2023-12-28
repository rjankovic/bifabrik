import glob2

def filePatternSearch(path: str) -> list[str]:
    res = []
    pathPts = path.split("/")
    searchLocations = ["Files/"]
    if len(pathPts) == 0:
        return res
    
    for i in range(len(pathPts)):
        pathPt = pathPts[i]
        if len(searchLocations) == 0:
            return res
        nextLevel = []
        for location in searchLocations:
            subLocations = mssparkutils.fs.ls(location)
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