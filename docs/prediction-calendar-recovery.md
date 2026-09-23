# Deploy the September recovery fixes together

## What this fixes

The saved September 17 prediction exists, but both ext_PredictionHistory and
FactPrediction have a null target date and Pending status. The original forecast
archive stores only its prediction session, while GoldAggregate previously got
both the target mapping and actual volume from historical tweet training rows.
Missing session rows therefore blocked evaluation even when Yahoo OHLCV existed.
The Function's subscription outage explains missing tweets, not this dependency.

GoldAggregate now derives the strictly following NYSE session using the same
pandas_market_calendars NYSE calendar used by historical training. It resolves
September 17 → 18 and Friday September 18 → Monday September 21 without any
historical tweet/session rows. Completed Yahoo current_volume is matched against
that target date. A missing target bar remains Pending at its proper date; a
later available bar never substitutes for it. Today's bar is eligible only after
18:00 Eastern, as before. The Yahoo download range includes the earliest saved
prediction as well as tweet dates and the last 30 days.

An archived non-null target is preserved when consistent with the calendar;
conflicting targets or invalid/non-trading session dates fail before output
writes. Old archives with missing/null target dates are resolved without rerunning
or changing their saved forecast values. Schema merging reads mixed old/new
archives. Both market_summary_per_day and prediction_history use the SAME
computed evaluation, so their actual and accuracy agree.

BlobStreamPredict saves target_date in new forecast archives. The pipeline's
blank default resolves the trigger's date in Eastern time, rather than the fixed
September 16 date. Manual YYYY-MM-DD overrides still work. The notebook skips
non-trading days before reading archives, rejects future dates and validates the
original archive observation timestamp against its session's 06:00 Eastern
cutoff. It checks tweet identities/session labels, filters post-cutoff tweets,
and deduplicates IDs. A prior-day tweet can legitimately inform the next morning's
forecast: absence of same-day tweets alone is not an error. Empty eligible input
fails without overwriting forecasts. Historical reruns are tagged replay in the
raw prediction archive; this does not establish a point-in-time model backtest.

## Deployment sequence

Do not rerun predictions merely to populate evaluation. The existing September
17 forecast will be evaluated from its unchanged archived value.

1. Finish active runs. Merge the historical tweet recovery PR (#4) and this PR.
   These are separate changes; this evaluation fix can run without the backfill
   endpoint, but both are needed for the requested full recovery.
2. In Synapse Studio → Manage → Apache Spark pools → sparkNVDA → Packages,
   add `pandas_market_calendars==5.4.0` to the pool's existing requirements and
   apply. Preserve other packages (including yfinance); do not replace the list
   with only this package. Wait for the pool package update to succeed, then use
   a new session. A package installed in another notebook's session is not enough
   for these scheduled pipeline runs. This uses the project's existing calendar
   library, newly required by GoldAggregate and BlobStreamPredict as well.
3. Reload Synapse's main collaboration branch and Publish all. Confirm
   GoldAggregate contains evaluate_predictions, BlobStreamPredict writes
   target_date, and PL_DailyPrediction has a blank p_session_date default.
   The daily trigger remains 06:00 Eastern; verify no live trigger override
   still supplies 2026-09-16. Existing SQL refresh definitions need no change.
4. Separately deploy the complete function-app folder from merged main, including
   tweet_backfill.py, to the existing Azure Function App. Synapse Publish does
   not deploy Functions. Confirm the restored RapidAPI subscription works there.
5. Since September 17 also lacks desired tweet coverage, start the recovery
   endpoint from **September 17**, not 18:

   ```json
   {"start_date":"2026-09-17","end_date":"2026-09-22","max_pages":3}
   ```

   Follow docs/historical-tweet-backfill.md from PR #4 for continuation and retry.
   Each batch uses up to three API calls. Finish all next_request batches before
   refreshing aggregates. The dates are inclusive and limited to completed days.
   Recovery is descriptive BI only; never copy recovery files into live session
   archive paths or rerun forecasts on today's recovered engagement values.
6. Run PL_BackfillRetrain once. Check both RunGoldAggregate and the nested
   RefreshWarehouse actually executed successfully. In GoldAggregate's output,
   expect `independent Yahoo completed OHLCV; NYSE calendar targets` and a non-null
   target date for the September 17 prediction. The independent evaluation does
   not need September 17 tweets to succeed.
7. Resume sqlNVDA if paused, then run the queries below. Once external and fact
   rows agree, refresh Power BI. Its September 17 social metrics can remain blank
   if the API returned no actual September 17 tweets, but its forecast evaluation
   should no longer depend on those metrics.

```sql
SELECT prediction_date, target_date, predicted_volume, actual_volume,
       prediction_accuracy, evaluation_status
FROM dbo.ext_PredictionHistory
WHERE prediction_date IN ('2026-09-16', '2026-09-17');

SELECT PredictionDateKey, TargetDateKey, PredictedVolume, ActualVolume,
       PredictionAccuracy, EvaluationStatus
FROM dbo.FactPrediction
WHERE PredictionDateKey IN (20260916, 20260917);

SELECT * FROM dbo.FactDailyTweet
WHERE DateKey BETWEEN 20260917 AND 20260922 ORDER BY DateKey;
```

September 16 → 17 should retain actual 94,191,300 / Evaluated. September 17 → 18
should get September 18's completed Yahoo volume / Evaluated when available.
Do not manually fill SQL nulls: the next refresh would overwrite them.

## Validation and limitations

Tests execute the production notebook helpers and writers with local Spark,
including target resolution without ANY historical tweet rows, the September
16→17 recovery, September 17→18, weekends and Labor Day, missing/invalid/unfinished
bars, matching market-summary evaluation, and the unchanged FactPrediction INSERT
in a local SQL harness. Other volume fixtures (including 190,290,000 for September
18) are synthetic; the screenshot only establishes a rounded 190.29M, not its
exact live volume. The corrected code reads the exact Yahoo bar at runtime.

Combined validation with PR #4: 23 tests passed in 37.639 seconds on Spark 3.5.3,
Java 17 and pandas_market_calendars 5.4.0. The changes to test_daily_gold.py also
merge cleanly with PR #4.

No live Azure deployment, pipeline expression execution, Yahoo call or Power BI
refresh was performed by local tests. Calendar packages must stay current for
exchange calendar changes. This is not a historical model/feature snapshot system;
new replay runs still load the currently deployed model. The existing top-user
rankings remain all-period summaries, separate from daily date filtering.
