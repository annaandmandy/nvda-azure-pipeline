# Daily BI data: historical-only tables after September 15

## Cause

BlobStreamPredict reads the Function App's `gold/stream/YYYY/MM/DD/tweets_gold.json`
archives and writes session-level predictions. It does not append tweet details to
Historical Gold. BackfillRetrain only updates `next_available_volume` on existing
historical rows. GoldAggregate previously read only Historical Gold for every BI
table and derived the market table by grouping those tweets. Consequently:

- Prediction history could contain September 16 → 17 and its actual while daily
  sentiment, interaction, account categories and viral statistics had no September
  16 rows.
- Current OHLCV could be missing for a valid trading date just because the tweet
  dataset had no tweets that day.
- SQL refresh and Power BI refresh cannot invent missing external-table rows.

## Change

GoldAggregate now merges all saved Function App stream archives with Historical
Gold **for BI only**. It keeps one row per incoming tweet ID using the latest
archive generation time and source filename, combines incoming non-null features
with historical data, and preserves historical prediction sessions and market
labels. It computes Eastern tweet timestamps from UTC timestamps. A tweet posted
September 15 ET stays in September 15 statistics even if it was collected for a
September 16 or later prediction session.

The merged view drives all eight existing BI aggregates. It does not overwrite
Historical Gold, alter training input or change the prediction-history evaluation
from PR #2. Replay and overlapping collection windows do not multiply tweets.
Missing or unreadable archives fail visibly instead of silently rebuilding only
old data. Empty tweet arrays are allowed; malformed identity/timestamps fail.

Yahoo completed OHLCV bars form the market-date rows independently of tweet
coverage. The query starts at the earlier of the first available tweet date or
30 days ago and ends before today until 18:00 ET, or includes today after 18:00 ET.
No valid bars causes failure before writing aggregates. Zero/negative, nonfinite
and unfinished bars are excluded. A market day without collected tweets has
current prices/volume, tweet_count=0 and null sentiment, not fabricated social
metrics. Yahoo is already required by BackfillRetrain.

Market-summary forecasts are joined by their prediction session, together with
that session's historical next-volume label; they are no longer shifted to the
target day and then compared with that day's next-volume label. Latest generated
forecasts win on reruns. SQL schemas and paths remain compatible.

The standalone and native SQL refresh definitions also include prediction and
target dates in DimDate, matching the live ALTER repair supplied during debugging.

## Deploy and verify

1. Review/merge the PR, then publish GoldAggregate from the Synapse collaboration
   branch. Confirm STORAGE_ACCOUNT is the deployment account. Git commit alone is
   not publication.
2. Ensure the live sp_DailyRefresh contains FactPrediction's truncate/insert and
   both prediction date sources in DimDate. For an existing procedure, execute
   its updated definition with ALTER PROCEDURE in sqlNVDA; publishing a SQL script
   does not execute its DDL. If the earlier live ALTER repair succeeded, no further
   SQL definition change is needed for this notebook fix.
3. Run PL_BackfillRetrain once. GoldAggregate prints `BI daily source` and tweet
   counts by actual Eastern date. Check whether September 16 was actually captured.
4. Verify the external tables, then RefreshWarehouse and corresponding fact tables:

```sql
SELECT * FROM dbo.ext_DailyTweetSummary
WHERE tweet_created_at_date = '2026-09-16';
SELECT * FROM dbo.ext_MarketSummaryPerDay
WHERE tweet_created_at_date = '2026-09-16';
SELECT * FROM dbo.FactDailyTweet WHERE DateKey = 20260916;
SELECT * FROM dbo.FactMarketSummaryPerDay WHERE DateKey = 20260916;
```

5. Resume sqlNVDA if the pipeline paused it, then refresh Power BI. The prediction
   should remain 94191300 / Evaluated for September 16 → 17.

If there are **no archived September 16 tweets**, September 16 social metrics
correctly remain absent. Recover genuine historical tweets through the existing
historical ingestion process; changing dates or copying another day's statistics
is not a backfill. The API uses a small Top-tweets sample, not complete daily X
coverage. The current daily prediction pipeline separately defaults p_session_date
to September 16; this PR does not change inference scheduling or trading-calendar
mapping. The Top Users tables remain all-period summaries without a date key.

## Validation

Seven local regression tests pass on Spark 3.5.3 / Java 17. The new integration
fixture starts with only September 15 historical tweets and supplies stream
archives containing a real September 16 tweet, a UTC-September-16 tweet that belongs
to September 15 ET, and overlapping snapshots. Actual notebook helpers and all eight
Parquet writers run. Assertions cover September 16 sentiment, interaction, followers,
credibility, account categories, viral metrics, deduplication, replay and preserved
ML labels. A September 17 bar with no tweets still produces current volume but no
sentiment. OHLCV values other than the known September 17 volume are synthetic test
fixtures, not claimed live Yahoo observations. Existing prediction-to-SQL tests pass.
Live Azure storage, Yahoo responses and Power BI were not directly accessible.
