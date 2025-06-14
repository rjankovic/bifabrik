from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.LineageSerializationHelper import LineageSerializationHelper
class LineageDependency:
    
    def __init__(self, source: LineageExpressionId = None, target: LineageExpressionId = None):
        self.__source = source
        self.__target = target
        #print(f"DEPENDENCY {self.__source} >>> {self.__target}")

    @property
    def source(self) -> LineageExpressionId:
        return self.__source

    @source.setter
    def source(self, value: LineageExpressionId):
        self.__source = value

    @property
    def target(self) -> LineageExpressionId:
        return self.__target

    @target.setter
    def target(self, value: LineageExpressionId):
        self.__target = value


    def __str__(self):
        return f"{self.__source} >>> {self.__target}"
    
    # Serialization to JSON
    def to_json_object(self):
        return {
            "source": self.__source.to_json_object(),
            "target": self.__target.to_json_object() #,
            #"_class": self.__class__.__name__
        }

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        #data = json.loads(json_str)
        instance = cls()
        instance.source = LineageSerializationHelper.deserialize_class(data.get("source"))
        instance.target = LineageSerializationHelper.deserialize_class(data.get("target"))
        return instance