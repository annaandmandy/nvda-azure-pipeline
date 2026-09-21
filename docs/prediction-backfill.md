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

## Follow-up: actual exists but prediction history remains Pending

The original GoldAggregate prediction-history cell (cell index 6) grouped labels
by `(prediction_session_date, next_trading_date)` and joined predictions only on
`prediction_session_date`. It did use `next_available_volume`, but restricted
actual lookup to the same historical session. A label for September 17 elsewhere
in production could not fill the September 16 prediction's null actual. It also
ignored a target date already present in an archived prediction.

The follow-up separates session-to-target mapping from the market-date label
lookup. Prediction `target_date` now joins historical `next_trading_date`, and
actual volume comes from `next_available_volume`. Neither `current_volume` nor
`prediction_session_date` is used as the target-market label. Positive finite
labels are deduplicated by target date; conflicting actuals fail explicitly.

There was also an independent source configuration in the final evaluation cell:
reconfiguring the first cell's GOLD_PATH did not affect its later hard-coded
HISTORICAL_GOLD_PATH. The notebook now has one STORAGE_ACCOUNT setting and reuses
GOLD_PATH and AGG_ROOT throughout, refreshes the production path before evaluation,
and prints its exact source/output paths plus the available target-date labels.
The output schema and ext_PredictionHistory/FactPrediction SQL remain unchanged.

### Live verification after deployment

Set GoldAggregate's STORAGE_ACCOUNT to the same account used by BackfillRetrain,
publish the updated notebook and pipeline in Synapse, then run PL_BackfillRetrain.
In RunGoldAggregate output, check:

1. `Prediction evaluation production source` matches BackfillRetrain's
   `Historical Gold` path exactly, including account and `historical/` suffix.
2. `Available production labels by target date` contains September 17 with
   94191300. Staging alone is not evidence that production has this label.
3. `Prediction evaluation results` contains September 16 → September 17 with
   actual 94191300, calculated accuracy and Evaluated status.
4. Run the existing SQL verification script after RefreshWarehouse completes.

If step 2 fails, inspect production directly in that notebook session:

```python
(spark.read.parquet(GOLD_PATH)
 .filter(F.to_date("next_trading_date") == F.lit("2026-09-17").cast("date"))
 .select("prediction_session_date", "next_trading_date", "next_available_volume")
 .distinct().show(truncate=False))
```

A missing production label remains Pending intentionally: evaluation must not
silently substitute staging or fabricate the reported actual. Check the deployed
BackfillRetrain publication and source account before rerunning aggregation.

The new regression explicitly asserts actual_volume == 94191300 and status !=
Pending for September 16 → 17, with the label on other historical rows, a distinct
current_volume and a different September 18 label to catch incorrect date joins.
It also tests configured production vs a stale staging path, archived targets
without session mappings, and staging-only actuals remaining Pending. The prior
code fails this fixture with `None != 94191300`; all four updated tests pass.
These tests run actual Spark transformations locally. Azure runtime/storage was
not directly accessible; the user confirmed the live notebook is synced to GitHub.
