from bifabrik.cfg.engine.Configuration import Configuration
from bifabrik.cfg.engine.Configuration import CfgProperty

class JsonConfiguration(Configuration):
    """Configuration related to loading CSV files
    """
    def __init__(self):
        self._explicitProps = {}
        self.__orient = None
        self.__typ = 'frame'
        self.__convert_axes = None
        self.__precise_float = False
        self.__lines = False

    @CfgProperty
    def orient(self) -> str:
        """From pandas:
        Indication of expected JSON string format. Compatible JSON strings can be produced by to_json() with a corresponding orient value. The set of possible orients is:

        'split' : dict like {index -> [index], columns -> [columns], data -> [values]}

        'records' : list like [{column -> value}, ... , {column -> value}]

        'index' : dict like {index -> {column -> value}}

        'columns' : dict like {column -> {index -> value}}

        'values' : just the values array

        'table' : dict like {'schema': {schema}, 'data': {data}}

The allowed and default values depend on the value of the typ parameter.

    when typ == 'series',

         - allowed orients are {'split','records','index'}

         - default is 'index'

         - The Series index must be unique for orient 'index'.

    when typ == 'frame',

         - allowed orients are {'split','records','index', 'columns','values', 'table'}

         - default is 'columns'

         - The DataFrame index must be unique for orients 'index' and 'columns'.

         - The DataFrame columns must be unique for orients 'index', 'columns', and 'records'.
        """
        return self.__orient
    @orient.setter(key='orient')
    def orient(self, val):
        self.__orient = val

    @CfgProperty
    def typ(self) -> str:
        """From pandas:
        {‘frame’, ‘series’}, default ‘frame’
        The type of object to recover.
        """
        return self.__typ
    @typ.setter(key='typ')
    def typ(self, val):
        self.__typ = val

    @CfgProperty
    def convert_axes(self) -> bool:
        """From pandas:
        Try to convert the axes to the proper dtypes.
        For all orient values except 'table', default is True.
        """
        return self.__convert_axes
    @convert_axes.setter(key='convert_axes')
    def convert_axes(self, val):
        self.__convert_axes = val

    @CfgProperty
    def precise_float(self) -> bool:
        """From pandas:
        set to enable usage of higher precision (strtod) function when decoding string to double values. Default (False) is to use fast but less precise builtin functionality
        default False
        """
        return self.__precise_float
    @precise_float.setter(key='precise_float')
    def precise_float(self, val):
        self.__precise_float = val

    @CfgProperty
    def lines(self) -> bool:
        """From pandas:
        Read the file as a json object per line
        default False
        """
        return self.__lines
    @lines.setter(key='lines')
    def quotechar(self, val):
        self.__lines = val