from bifabrik.cfg.engine.ConfigContainer import ConfigContainer
from bifabrik.cfg.specific.ExcelConfiguration import ExcelConfiguration
from bifabrik.cfg.specific.FileSourceConfiguration import FileSourceConfiguration
from bifabrik.cfg.DataSourceConfigurationBase import DataSourceConfigurationBase

class ExcelSourceConfiguration(DataSourceConfigurationBase):

    def __init__(self):
        self.__excel = ExcelConfiguration()
        self.__fileSource = FileSourceConfiguration()
        super().__init__()
        

    @property
    def excel(self) -> ExcelConfiguration:
        return self.__excel
    
    @property
    def fileSource(self) -> FileSourceConfiguration:
        return self.__fileSource