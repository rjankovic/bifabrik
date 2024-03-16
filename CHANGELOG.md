# Changelog


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
