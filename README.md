# bifabrik
Microsoft Fabric ETL toolbox - the assembly line for your lakehouse

## What is the point?
 - make BI development in Microsoft Fabric easier by providing a fluent API for common ETL tasks
 - reduce repetitive code by setting preferences in config files

## Usage
 1. First, add `bifabrik` to your Fabric environment

 2. In a Fabric notebook, create an instance of bifabrik and give it access to spark

```python
from bifabrik import bifabrik
bif = bifabrik(spark)
```

