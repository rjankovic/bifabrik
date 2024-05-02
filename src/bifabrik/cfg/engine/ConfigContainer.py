import copy
from bifabrik.cfg.engine.Configuration import CfgProperty

class ConfigContainer:
    def __init__(self):
        # mapping cfg property names from inenr configurations
        # to the Configuration objects
        self.__propDict = dict()
        self.setCfgOptions()
    
    def setterFactory(self, attrName):
        def directSetter(val):
            #print(f'DirectSetter: {str(self)} - set {attrName} to {val}')
            self.option(attrName, val, True)
            return self
        return directSetter
    
    def setCfgOptions(self):
        rootAttribs = dir(self)
        noUnderscoreRootAttrNames = list(filter(lambda x: not x.startswith("_"), rootAttribs))
        
        
        firstConfigAttr = True
        for rootAttrName in noUnderscoreRootAttrNames:
            rootAttr = getattr(self, rootAttrName)
            attrType = type(rootAttr)
            if attrType.__bases__[0].__name__ == 'Configuration':
                if firstConfigAttr:
                    self.__doc__ = (self.__doc__ or "") + \
f'\nConfiguration parts (properties of this container):\n' + \
'===================================================\n\t'
                firstConfigAttr = False
                self.__doc__ = self.__doc__ + f'{rootAttrName}:\t{rootAttr.__doc__}'

        # for adding config options from the partial configs
        self.__doc__ = self.__doc__ + '\n\nConfiguration options:\n' + '=======================\n'

        for rootAttrName in noUnderscoreRootAttrNames:
            rootAttr = getattr(self, rootAttrName)
            attrType = type(rootAttr)
            if attrType.__bases__[0].__name__ == 'Configuration':

                cfgPropNames = dir(attrType)
                for cfgpName in cfgPropNames:
                    cfgAttribute = getattr(attrType, cfgpName)
                    if isinstance(cfgAttribute, CfgProperty):
                        if cfgpName in self.__propDict:
                            matchingAttrTypes = list(filter(lambda x: str(type(x)) == str(type(rootAttr)), self.__propDict[cfgpName]))
                            if len(matchingAttrTypes) == 0:
                                self.__propDict[cfgpName].append(rootAttr)
                            #if str(type(self.__propDict[cfgpName])) != str(type(rootAttr)):
                            #    raise Exception(f'bifabrik configuration conflict: {cfgpName} is defined both in {self.__propDict[cfgpName]} ({type(self.__propDict[cfgpName])}) and {rootAttr} ({type(rootAttr)})')
                        else:
                            self.__propDict[cfgpName] = [rootAttr]

                        # create a direct setter property for overriding the value in a fluent API
                        # so that we don't need to use .option()
                        # (however, don't override existent properties while doint this - rather ommit some, they will still be available through .ption()_

                        hasA = hasattr(self, cfgpName)
                        if hasA == False:
                            
                            directSetter = self.setterFactory(cfgpName)
                            directSetter.__doc__ = f'{(cfgAttribute.__doc__ or "")} \n(from {rootAttrName} configuration)'
                            directSetter.__name__ = cfgpName
                            setattr(self, cfgpName, directSetter)

                        # (don't use this - the __doc__ is in the type)
                        # instanceAttr = getattr(attr, cfgp)
                        self.__doc__ = (self.__doc__ or "") + f'\n{cfgpName}: {cfgAttribute.__doc__}\n'

       
    def option(self, name, value = None, force_set = False):
        if not (name in self.__propDict):
            raise Exception(f'Configuration key not found: {name}.')
        if value is None and not force_set:
            val = getattr(self.__propDict[name][0], name)
            return val
        #setattr(self.__propDict[name], name, value)
        for prop in self.__propDict[name]:
            setattr(prop, name, value)
        return self
    
    def mergeToCopy(self, other):
        """Meges another configuration into this one; the 'other' config container takes priority.
        Returns a new configuration without affecting the original"""
        cp = self.copy()
        return cp.merge(other)

    
    def merge(self, other):
        """Meges another configuration into this one; the 'other' config container takes priority"""
        for key in other.propDict:
            for configPart in other.propDict[key]:
                # that have an explicit value set in the source
                if key in configPart._explicitProps:
                    # and are among target attributes
                    if key in self.__propDict:
                        for localProp in self.__propDict[key]:
                            if(str(type(localProp)) == str(type(configPart))):
                                setattr(localProp, key, getattr(configPart, key))
                                #print(f'setting {key} of {str(type(localProp))} to {getattr(configPart, key)} based on {str(type(configPart))}')
        return self
    
    def copy(self):
        """Cretes a deep copy of the config container"""
        return copy.deepcopy(self)

    
    @property
    def propDict(self):
        """Dictionary mapping available configuration properties to the Configuration objects containing them.
        For internal use.
        """
        return self.__propDict