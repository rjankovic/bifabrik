class ConfigContainer:
    def __init__(self):
        self.d = dir(self)
        self
    
    def getList(self):
        a = dir(self)
        dnu = list(filter(lambda x: not x.startswith("_"), a))
        res = dict()
        for e in dnu:
            attr = getattr(self, e)
            attrType = type(attr)
            if attrType.__bases__[0].__name__ == 'Configuration':
                cfgProps = list(filter(lambda x: not x.startswith("_"), dir(attr)))
                for cfgp in cfgProps:
                    res[cfgp] = attr
        self.__propDict = res
        print(res)
    
    def setProperty(self, name, value):
        if not (name in self.__propDict):
            raise Exception(f'Configuration key not found: {name}.')
        setattr(self.__propDict[name], name, value)
        return self
    
    def mergeCofiguration(self,other: ConfigContainer):
        """Meges another configuration into this one; the 'other' config container takes priority"""
        for key in other.__propDict:
            if key in __propDict:
                setattr(self.__propDict[key], key, getattr(other.__propDict[key], key))
