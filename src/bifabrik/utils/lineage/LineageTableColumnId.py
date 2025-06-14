from bifabrik.utils.lineage.LineageExpressionId import LineageExpressionId
import re

class LineageTableColumnId(LineageExpressionId):
    
    def __init__(self, node = None, tableIdentifier: str = None):
        super().__init__(node)
        self._tableIdentifier = tableIdentifier
        self.__splitTableIdentifier()
    
    @property
    def tableIdentifier(self) -> str:
        return self._tableIdentifier
    
    @tableIdentifier.setter
    def tableIdentifier(self, value: str):
        self._tableIdentifier = value
        self.__splitTableIdentifier()
    
    def __str__(self):
        return f"{self._tableIdentifier}.`{self.name}`#{self.id}"

    # Serialization to JSON
    def to_json_object(self):
        base = super().to_json_object()
        base['tableIdentifier'] = self._tableIdentifier
        return base

    def __splitTableIdentifier(self):
        self._tableIdentifierParts = []
        if self._tableIdentifier is None:
            return
        parts = re.findall(r'`([^`]*)`', self._tableIdentifier)
        self._tableIdentifierParts = parts
    
    @property
    def tableName(self) -> str:
        if len(self._tableIdentifierParts) > 0:
            return self._tableIdentifierParts[len(self._tableIdentifierParts) - 1]
        else:
            return None
    
    @property
    def dbName(self) -> str:
        if len(self._tableIdentifierParts) > 1:
            return self._tableIdentifierParts[len(self._tableIdentifierParts) - 2]
        else:
            return None

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        #data = json.loads(json_str)
        instance = cls()
        instance.id = data.get("id")
        instance.name = data.get("name")
        instance.sql = data.get("sql")
        instance.tableIdentifier = data.get("tableIdentifier")
        return instance