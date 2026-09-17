# SQL deployment scripts

These standalone SQL files are extracted from the version-controlled Azure Synapse SQL script artifacts under `synapse/sqlscript/`.

Run them in numeric order:

1. `01_external_objects.sql` creates the workspace identity credential, external data source, Parquet file format, and Gold-layer external tables.
2. `02_prediction_objects.sql` creates the prediction external table and warehouse fact table.
3. `03_dimensional_warehouse.sql` creates the dimensional serving model.
4. `04_sp_daily_refresh.sql` creates the daily warehouse refresh procedure.

Before execution, replace `<storage-account-name>` with the target ADLS Gen2 storage account name. No passwords, access keys, SAS tokens, or connection strings belong in these files.

`Login.json` is intentionally excluded because environment-specific database principals and grants should be managed separately from the portable data model.
