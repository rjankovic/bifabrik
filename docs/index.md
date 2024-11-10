<!--- The assembly line for your lakehouse -->

<span style="font-family:Consolas; font-size:1.5em;">__pip install bifabrik__</span>

A python library to make a BI dev's life easier when working with MS Fabric. Use fluent API for building simple ETL pipelines. Avoid repetitive setup using configured preferences. And more.

 - **[Quickstart](/tutorial/quickstart.md)**
 - [Configuration](/tutorial/configuration.md)
   - [Cross-lakehouse pipelines](/tutorial/cfg_storage.md)
 - Data sources
   - [CSV](/tutorial/src_csv.md)
   - [JSON](/tutorial/src_json.md)
   - [Archiving processed files](/tutorial/msc_files_archive.md)
   - [Spark SQL (lakehouse)](/tutorial/src_sql.md)
   - [T-SQL (warehouse)](/tutorial/src_warehouse_sql.md)
   - [JDBC](/tutorial/src_jdbc.md)
   - [Spark / Pandas DataFrame](/tutorial/src_spark_df.md)
   - [Other sources](/tutorial/src_other.md)
- Data transformations
   - [Spark / Pandas DataFrame](/tutorial/tsf_spark_df.md)
   - [Data validation](/tutorial/tsf_validation.md)
- Data destinations
   - [Lakehouse table](/tutorial/dst_table.md)
   - [Warehouse table](/tutorial/dst_warehouse_table.md)
   - [Spark / Pandas DataFrame](/tutorial/dst_spark_df.md)
- Utilities
   - [Semantic models](/tutorial/util_tmsl.md)
   - [Lakehouse tables](/tutorial/util_table.md)
   - [File system](/tutorial/util_fs.md)
   - [Logging](/tutorial/util_log.md)
   - [Fabric REST API](/tutorial/util_api.md)

**[Report an issue / feature request](https://github.com/rjankovic/bifabrik/issues)**  
