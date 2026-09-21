# Pending prediction: 2026-09-16 → 2026-09-17

## Cause and data flow

`BlobStreamPredict` archives predictions under `gold/prediction/streaming/`.
The archive contains the prediction session, forecast, model version and generation
time; it does not contain an actual volume. `BackfillRetrain` gets the target date
from Historical Gold and joins Yahoo volume on **next_trading_date**, not the
prediction date.

Previously, BackfillRetrain calculated `updated_gold_df` and wrote it to
`tweets_stock/historical_staging/`, but never published it to
`tweets_stock/historical/`. Its evaluated prediction dataframe was also only
in memory. A successful notebook run therefore did not persist the labels consumed
by the next notebook.

GoldAggregate rereads `tweets_stock/historical/`, joins the latest archived
prediction to its session/target mapping, and writes
`aggregates/prediction_history/`. A null actual becomes `Pending`.
`dbo.ext_PredictionHistory` reads that folder; `dbo.sp_DailyRefresh` truncates and
reloads `dbo.FactPrediction`, copying actual, accuracy and status unchanged.
Refreshing SQL alone cannot recover an actual missing from Gold.

The fix validates staging, publishes it to Historical Gold, checks the published
row/unlabeled counts, and rebinds training to the new files. Label publication does
not depend on the retraining threshold. Label joins use one row per target date,
preventing duplicate tweet rows. The obsolete fixed September 15 download is
removed; the market request covers older unresolved targets and excludes the
current day's unfinished bar before the evening cutoff. Missing/nonpositive
volumes are excluded. GoldAggregate rejects ambiguous session mappings and only
evaluates positive actuals.

## Deployment and recovery

1. Deploy/publish the changed BackfillRetrain and GoldAggregate notebooks to the
   existing Synapse workspace, applying the environment's storage account value.
   Repository notebook paths intentionally use `<storage-account-name>`.
2. Run `PL_BackfillRetrain` after the market close. Its existing success dependencies
   run BackfillRetrain → GoldAggregate → SQL resume → `sp_DailyRefresh`.
   Confirm `RefreshWarehouse` actually ran; the current pipeline skips it if the
   SQL pool is not online after its wait.
3. Verify both layers using `sql/05_verify_prediction_backfill.sql`. Expected for
   the reported Yahoo observation: target 2026-09-17, actual 94191300, Evaluated,
   accuracy `1 - ABS(94191300 - predicted_volume) / 94191300`.

No manual FactPrediction UPDATE is needed. It would be overwritten by the next
refresh. If the historical session/target mapping is absent, restore that mapping
from the trading calendar first; do not substitute the prediction date for the
target date or blindly add one calendar day.

Production writes use the existing overwrite convention and are not atomic.
Run a single backfill at a time and avoid concurrent readers during publication.
Staging remains available if a production write fails. Model staging/promotion is
outside this label-publication fix.

## Validation boundary

The regression executes notebook cells with Spark 3.5 against temporary Parquet
folders, using the user-reported 94,191,300 as a fixture. It checks publication
without retraining, row preservation, preservation of existing labels, pending
future targets, repeat-run idempotency, latest-prediction selection, accuracy, and
the repository's FactPrediction INSERT in a local SQL harness. This is not a live
Yahoo, Azure storage, or dedicated SQL pool verification.

Run with Python, PySpark 3.5 and Java 17:

```sh
python -m unittest discover -s tests
```
