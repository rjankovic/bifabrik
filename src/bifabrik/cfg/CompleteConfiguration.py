import json
from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.CsvConfiguration import CsvConfiguration
from bifabrik.cfg.specific.FileSourceConfiguration import FileSourceConfiguration

class CompleteConfiguration(ConfigContainer):
    """
    Fluent API for configuration
    
    You can set configuration directly through setter methods like .delimiter(';')[.further options...]
    or
    .option(name, value = None) gets / sets a named config property as in
    .option('delimiter', ';') #setter
    .option('delimiter') #getter
    """
    def __init__(self):
        self.__csv = CsvConfiguration()
        self.__fileSource = FileSourceConfiguration()
        super().__init__()
        

    @property
    def csv(self) -> CsvConfiguration:
        return self.__csv
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        return self.__fileSource
    
    def serialize(self) -> str:
        res = {}
        rootAttribs = dir(self)
        noUnderscoreRootAttrNames = list(filter(lambda x: not x.startswith("_"), rootAttribs))
        for rootAttrName in noUnderscoreRootAttrNames:
            rootAttr = getattr(self, rootAttrName)
            attrType = type(rootAttr)
            if attrType.__bases__[0].__name__ == 'Configuration':
                res[rootAttrName] = rootAttr._explicitProps
        j = json.dumps(res, indent=4)
        return j

    @staticmethod
    def deserialize(json_data):
        json_dict = json.loads(json_data)
        cfg = CompleteConfiguration()
        for cfgAttrName in json_dict:
            for cfgPropName in json_dict[cfgAttrName]:
                cfg.option(cfgPropName, json_dict[cfgAttrName][cfgPropName])
        return cfg
    
    def saveToFile(self, path):
        serialized = self.serialize()
        notebookutils.mssparkutils.fs.put(path, serialized, True)

    def loadFromFile(self, path):
        json_data = notebookutils.mssparkutils.fs.head(path, 1024 * 1024 * 1024)
        deser = CompleteConfiguration.deserialize(json_data)
        self.merge(deser)
        return self