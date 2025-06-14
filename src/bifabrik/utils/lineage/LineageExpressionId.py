class LineageExpressionId:
    
    def __init__(self, node = None):
        if node is not None:
            exprId = node.exprId()
            self._id = exprId.id()
            self._name = node.name()
            self._sql = node.sql()
    
    # def __init__(self, id: int, name: str):
    #     self.__id = id
    #     self.__name = name
    #     self.__sql = None

    @property
    def id(self) -> int:
        return self._id
    
    @id.setter
    def id(self, value: str):
        self._id = value

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str):
        self._name = value

    @property
    def sql(self) -> str:
        return self._sql

    @sql.setter
    def sql(self, value: str):
        self._sql = value

    def __str__(self):
        return f"{self.name}#{self.id} '{self.sql}'"

    # Serialization to JSON
    def to_json_object(self):
        return {
            "id": self._id,
            "name": self._name,
            "sql": self._sql,
            "_class": self.__class__.__name__
        }

    # Deserialization from JSON
    @classmethod
    def from_json_object(cls, data):
        #data = json.loads(json_str)
        instance = cls()
        instance.id = data.get("id")
        instance.name = data.get("name")
        instance.sql = data.get("sql")
        return instance