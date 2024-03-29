from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.SecurityConfiguration import SecurityConfiguration
from bifabrik.cfg.DataSourceConfigurationBase import DataSourceConfigurationBase

class SharePointListSourceConfiguration(DataSourceConfigurationBase):

    def __init__(self):
        self.__security = SecurityConfiguration()
        super().__init__()
        

    @property
    def security(self) -> SecurityConfiguration:
        return self.__security