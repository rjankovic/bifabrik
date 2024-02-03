from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class FileSourceConfiguration(Configuration):
    """Configuration related to loading data from files (aspects common for all file formats)
    """
    def __init__(self):
        self._explicitProps = {}
        self.__encoding = None
        self.__locale = None

    @CfgProperty
    def encoding(self) -> str:
        """From pandas:
        How encoding errors are treated. https://docs.python.org/3/library/codecs.html#standard-encodings
        """
        return self.__encoding
    @encoding.setter(key='encoding')
    def encoding(self, val):
        self.__encoding = val

    @CfgProperty
    def locale(self) -> str:
        """From spark:
        sets a locale as language tag in IETF BCP 47 format. If None is set, it uses the default value, en-US. For instance, locale is used while parsing dates and timestamps.
        """
        return self.__locale
    @locale.setter(key='locale')
    def jsonLocale(self, val):
        self.__locale = val