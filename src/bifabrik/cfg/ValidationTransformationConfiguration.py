from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.ValidationConfiguration import ValidationConfiguration

class ValidationTransformationConfiguration(ConfigContainer):

    def __init__(self):
        self.__validation = ValidationConfiguration()
        super().__init__()
        

    @property
    def validation(self) -> ValidationConfiguration:
        """Data validation configuration"""
        return self.__validation