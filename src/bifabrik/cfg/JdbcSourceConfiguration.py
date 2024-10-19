from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.JdbcConfiguration import JdbcConfiguration
from bifabrik.cfg.DataSourceConfigurationBase import DataSourceConfigurationBase

class JdbcSourceConfiguration(DataSourceConfigurationBase):

    def __init__(self):
        self.__jdbc = JdbcConfiguration()
        super().__init__()
        

    @property
    def jdbc(self) -> JdbcConfiguration:
        return self.__jdbc