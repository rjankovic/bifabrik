# Other data sources

## Excel
```python
import bifabrik as bif

bif.fromExcel('ExcelFiles/Custoemrs.xlsx').sheetName('CustomerList').toTable('Customers').run()

# show other options
help(bif.config.excel)
```

[Back](../index.md)
