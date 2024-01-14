from bifabrik.cfg.Configuration import CfgProperty

class ConfigContainer:
    def __init__(self):
        self.d = dir(self)
        self.getList()
    
    def getList(self):
        a = dir(self)
        dnu = list(filter(lambda x: not x.startswith("_"), a))
        res = dict()
        for e in dnu:
            attr = getattr(self, e)
            attrType = type(attr)
            if attrType.__bases__[0].__name__ == 'Configuration':
                cfgProps = dir(attrType)
                for cfgp in cfgProps:
                    cfgatt = getattr(attrType, cfgp)
                    if isinstance(cfgatt, CfgProperty):
                        #print("PROP " + cfgp + " in " + e + " (" + str(attr) + ")")
                        #print(cfgp)
                        res[cfgp] = attr
        self.__propDict = res
        #print(res)
    
       
    def option(self, name, value = None):
        if not (name in self.__propDict):
            raise Exception(f'Configuration key not found: {name}.')
        if value is None:
            val = getattr(self.__propDict[name], name)
            return val
        setattr(self.__propDict[name], name, value)
        return self

    def merge(self, other):
        """Meges another configuration into this one; the 'other' config container takes priority"""
        
        #print("self")
        #for key in self.__propDict:
        #    print(self.__propDict[key]._explicitProps)
        #print("other")
        #for key in other.__propDict:
        #    print(other.__propDict[key]._explicitProps)

        for key in other.__propDict:
            # that have an explicit value set in the source
            if key in other.__propDict[key]._explicitProps:
                # and are among target attributes
                if key in self.__propDict:
                    setattr(self.__propDict[key], key, getattr(other.__propDict[key], key))
        return self