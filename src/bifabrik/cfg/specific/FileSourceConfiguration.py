from bifabrik.cfg.Configuration import Configuration
from bifabrik.cfg.Configuration import CfgProperty

class FileSourceConfiguration(Configuration):
    def __init__(self):
        self._explicitProps = {}
        self.__encoding = 'utf-8'

    @CfgProperty
    def encoding(self) -> int:
        """From pandas:
        How encoding errors are treated. https://docs.python.org/3/library/codecs.html#standard-encodings
        """
        return self.__encoding
    @encoding.setter(key='encoding')
    def encoding(self, val):
        self.__encoding = val