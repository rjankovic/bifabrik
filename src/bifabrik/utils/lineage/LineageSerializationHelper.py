from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
from bifabrik.utils.lineage.LineageTableColumnId import LineageTableColumnId

class LineageSerializationHelper:
    @classmethod
    def deserialize_class(cls, data):
        class_name = data["_class"]
        target_class = globals().get(class_name, cls)  # Lookup class by name

        result = getattr(target_class, 'from_json_object')(data)
        return result