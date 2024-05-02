from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.ValidationConfiguration import ValidationConfiguration

class ValidationTransformationConfiguration(ConfigContainer):

    def __init__(self):
        self.__validationConfiguration = ValidationConfiguration()
        super().__init__()
        

    @property
    def validationConfiguration(self) -> ValidationConfiguration:
        """Data validation configuration"""
        return self.__validationConfiguration