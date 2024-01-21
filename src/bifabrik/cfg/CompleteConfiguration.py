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
        """CSV files settings"""
        return self.__csv
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        """General file loading settings"""
        return self.__fileSource
    
    def serialize(self) -> str:
        """Serializes the configuration to a string"""
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
        """Deserializes a CompleteConfiguration from a string"""
        json_dict = json.loads(json_data)
        cfg = CompleteConfiguration()
        for cfgAttrName in json_dict:
            for cfgPropName in json_dict[cfgAttrName]:
                cfg.option(cfgPropName, json_dict[cfgAttrName][cfgPropName])
        return cfg
    
    def saveToFile(self, path):
        """Saves the configuration to a JSON file"""
        serialized = self.serialize()
        notebookutils.mssparkutils.fs.put(path, serialized, True)

    def loadFromFile(self, path):
        """Loads the configuration values from a JSON file"""
        json_data = notebookutils.mssparkutils.fs.head(path, 1024 * 1024 * 1024)
        deser = CompleteConfiguration.deserialize(json_data)
        self.merge(deser)
        return self