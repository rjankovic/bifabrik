# Changelog

## v0.14.0 (2025-04-20)
 - Temporary table support
 - `ensureTableIsReadyAfterSaving` option for lakehouse tables enabled by default
 - Better error handling when loading lakehouse metadata

## v0.13.0 (2025-03-22)
 - Removing accents from column names
 - Better merge performance for large tables (materialization and partitioning)
 - Semantic link API congestion prevention (when gathering lakehouse metadata)

## v0.12.0 (2025-02-09)
 - Partitions support
 - Update date setting for merge (SCD1) tables
 - Duplicate keys prevention for SCD1 tables
 - New config option - `destinationTable.allowMissingColumnsInSource`
 - Bug fixes (duplicates prevention for merge destination, adding new columns when identity column is enabled)

## v0.11.0 (2024-12-05)
 - SCD 2 support for lakehouse destination

## v0.10.5 (2024-11-29)
 - cross-lakehouse lakehouse bug fixed for merge operation
 - support for lakehouses with schemas

## v0.10.0 (2024-11-10)
 - JDBC source support
 - merge method for large tables

## v0.9.0 (2024-06-21)
 - Fabric pipeline execution via API

## v0.8.1 (2024-05-19)
 - fix handling CSV files with no data

## v0.8.0 (2024-05-09)
 - Data validation transformation
 - Generic N/A record option for table destinations
 - Current lakehouse metadata helper functions in fsUtils
 - Batch restore for delta tables in tableUtils

## v0.7.1 (2024-04-28)
 - Fixed bug in Warehouse table destination type mapping

## v0.7.0 (2024-04-27)
 - Fabric Warehouse table destination
 - Fabric Warehouse T-SQL source

## v0.6.1 (2024-04-11)
 - Fixed breaking bug in Configuration.py file name

## v0.6.0 (2024-04-01)
- Power BI dataset definition backup / restore using TMSL
- Lakehouse table utilities - add / rename / remove columns
- Table destination schema merge (support for adding columns)
- Excel data source
- SharePoint list data source

## v0.5.0 (2024-03-16)
- Source files archiving support
- Spark / Pandas DF source / destination
- Spark / Pandas DF transformation using functions
- Destination table config options
    - incremental load
        - append
        - merge
        - snapshot
    - identity column
    - watermark
    - timestamp column
    - fixing invalid column names

## v0.4.0 (2024-03-01)
- Cross-lakehouse data pipelines support
- Simplified initiation

## v0.3.0 (2024-02-09)
- Extended configuration system
- Added CSV and JSON source config options
- Logging support

## v0.2.0 (2023-12-31)
- First release of `bifabrik` - support for CSV / JSON / SQL sources, full load only, no logging, no recovery.
