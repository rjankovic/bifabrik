class Configuration:
    def __init__(self):
        self._explicitProps = {}

    def createCfgProperty(self, name, description):
        """Return a property that stores values under a private non-public name."""
        storage_name = '__' + name

        @CfgProperty
        def prop(self):
            return getattr(self, storage_name)

        @prop.setter(key=name)
        def prop(self, value):
            setattr(self, storage_name, value)
        
        prop.__doc__ = description
        
        setattr(self, name, prop)
        #return prop

class CfgProperty(property):
    def __set__(self, obj, value):
        super().__set__(obj, value)
        att = getattr(obj, '_explicitProps')
        att[self.key] = value

    def _setter(self, fset):
        obj = super().setter(fset)
        obj.key = self.key
        return obj

    def setter(self, key):
        self.key = key
        return self._setter