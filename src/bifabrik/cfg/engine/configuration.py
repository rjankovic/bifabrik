class Configuration:
    pass

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