# Recover missing daily tweet statistics after the API subscription interruption

The restored API can search historical dates, but the timer always fetches a recent
window. PL_BackfillRetrain does not fetch tweets. This change adds a FUNCTION-key
protected HTTP endpoint `POST /api/backfill-nvda-tweets` to the existing Python
Function App. Deploy the **whole function-app directory**, including the new
`tweet_backfill.py`, with the same method used for the existing Function App.
Merging GitHub or publishing Synapse alone does **not** deploy Azure Functions.
The existing requirements and Azure settings remain the same. Do not deploy only
function_app.py; its new local module is required.

## Operate from Azure Portal

1. Wait for the active prediction/backfill pipeline to finish before deployment
   or starting recovery. Confirm RapidAPI subscription and the Azure App's Key.
2. After deploying the Function App, open Functions → backfill_nvda_tweets →
   Code + Test → Test/Run. Select POST and the function key (if prompted).
3. Use this JSON body for September 18 through September 22 inclusive. This also
   recovers weekend tweets for BI; it does not invent weekend stock trading days.

   ```json
   {"start_date":"2026-09-18","end_date":"2026-09-22","max_pages":3}
   ```

4. A `202` with `status: partial` means this batch reached its API-call budget.
   Copy the **entire next_request object** into the next request body and run
   again. Keep doing this while next_request is non-null. Do not restart at the
   initial body each time. Each request uses at most three provider API calls;
   review the returned counts and your subscription quota before continuing.
5. `200`, `status: finished`, and `next_request: null` means the provider returned
   no further cursor for the range. It is not a guarantee of complete X coverage.
   `written_pages` lists paths and matching tweet counts. An empty date produces
   no fabricated tweet or empty schema file. Dates with no matches can stay blank.
6. If the endpoint fails, inspect Function logs and retry the same request body.
   Some earlier pages may already exist. Page paths are deterministic, so retrying
   replaces that page and GoldAggregate deduplicates tweet IDs across pages.
   A repeated cursor or unexpected payload stops visibly rather than reporting
   success. If the provider ignores cursor, stop and inspect its response.
7. Verify saved files under
   `gold/stream/backfill/2026/09/<day>/<page-hash>/tweets_gold.json`.
   The existing recursive GoldAggregate reader includes these automatically.
8. After completing recovery, run PL_BackfillRetrain once. Confirm
   RunGoldAggregate and the nested RefreshWarehouse both actually succeeded.
   Resume sqlNVDA if the pipeline paused it, query the facts below, then refresh
   Power BI. No SQL definition change is needed for this feature.

   ```sql
   SELECT * FROM dbo.FactDailyTweet
   WHERE DateKey BETWEEN 20260918 AND 20260922
   ORDER BY DateKey;

   SELECT * FROM dbo.FactMarketSummaryPerDay
   WHERE DateKey BETWEEN 20260918 AND 20260922
   ORDER BY DateKey;
   ```

If Test/Run is unavailable for the hosting plan, send the same JSON with an
HTTP client to the Function URL and put the function key in `x-functions-key`.
Never paste the key into GitHub, screenshots, or chat.

## Data meaning and safety

- Date bounds are inclusive Eastern calendar days, at most seven completed days
  per range. Search uses Latest and a padded UTC date window, then filters by the
  actual UTC timestamp converted to America/New_York. Late evening tweets remain
  on the correct Eastern day. Today's unfinished date is rejected.
- Existing feature engineering is reused. Engagement and user profiles are
  observed **at recovery time**, not necessarily their original historical values.
  The envelope records the real recovery timestamp and `historical_bi_backfill`.
- Files live under stream/backfill, never stream/YYYY/MM/DD, and have no candidate
  prediction session. This restores descriptive BI metrics, not historical model
  forecasts or training data. Saved live archives and predictions are untouched.
- GoldAggregate's existing latest-observation rule can update BI features of an
  overlapping tweet, while preserving historical ML labels. Recovery pages are
  available to BI as they are saved: finish the whole run before refreshing.
- Do not point BlobStreamPredict at these files or relabel them as live forecasts.
  Historical forecasts require a separate backtest with point-in-time features.
- Pagination supports the existing result.timeline.instructions envelope, Bottom
  timeline cursors and the provider's outer cursor.bottom form. Other payload
  shapes fail or need an adapter; local fixtures do not prove every live response.
- Daily prediction's fixed default p_session_date remains a separate issue; this
  feature does not fix inference scheduling. The daily ingestion timer is unchanged.

## Validation

Run `python -m unittest discover -s tests` with the existing Spark test environment.
Recovery unit tests cover Eastern boundaries, duplicate IDs, retry idempotency,
page limits and continuation, empty days/pages, invalid payloads, cursor loops,
provider failure after a saved page, wrapped tweets, and request bounds. A Spark
integration test reads real recovery JSON files using GoldAggregate's recursive
reader, verifies September 18 rows after a retry and preserves the known
September 16 → 17 actual label 94,191,300. Existing prediction/daily BI tests remain.
No live Azure deployment or paid API requests are performed by these tests.

## Diagnose batches with no written pages

The HTTP response and existing Historical BI recovery log now include
`page_diagnostics`. Each page reports the target Eastern date, search bounds,
parsed tweet count, earliest/latest Eastern timestamps, exclusions before/after
the target day, duplicate matching IDs, written count and whether a cursor exists.
No tweet text, user identifiers or cursor values are included in this diagnostic
object. The existing `next_request` still contains the continuation cursor.

- `all_outside_target_date`: tweets were parsed but all were outside the Eastern
  target day. Latest search can return the newer padded day's tweets first.
- `no_parsed_tweets`: the parser extracted no tweets. This alone cannot establish
  that the provider has no tweets; an unrecognized response layout is also possible.
- `written`: the page was saved with the reported number of unique matching IDs.

After deploying diagnostics, resume with the most recent `next_request`, changing
`max_pages` to 1 for a single-call diagnostic run. Preserve its date range and
cursor. Inspect `page_diagnostics` before spending quota on more pages. Do not
interpret `partial` or an Azure execution status of Succeeded as recovered data.
