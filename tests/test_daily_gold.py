"""Daily BI inputs must add real tweet dates and market rows independently."""
import json
from pathlib import Path
from datetime import date, datetime
import tempfile
import unittest

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType,
                               DateType, LongType, DoubleType, BooleanType)

ROOT = Path(__file__).resolve().parents[1]


def cell(i):
    return ''.join(json.loads((ROOT / 'synapse/notebook/GoldAggregate.json').read_text())
                   ['properties']['cells'][i]['source'])


class DailyGoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('daily-gold-regression')
                     .config('spark.sql.shuffle.partitions', '2')
                     .config('spark.sql.session.timeZone', 'UTC')
                     .config('spark.ui.enabled', 'false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.env = dict(spark=cls.spark, display=lambda *_: None)
        # Execute the actual notebook helper definitions, excluding cloud I/O.
        exec(cell(0).split('# Read Historical Gold and archived daily tweets for BI')[0], cls.env)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def historical_fixture(self):
        types = {}
        for c in ['tweet_id', 'user_id', 'tweet_created_at', 'full_text', 'clean_text']:
            types[c] = StringType()
        for c in ['tweet_timestamp_utc', 'tweet_timestamp_et']:
            types[c] = TimestampType()
        for c in ['account_created_date', 'prediction_session_date', 'next_trading_date']:
            types[c] = DateType()
        for c in ['view_count', 'retweet_count', 'reply_count', 'quote_count', 'favorite_count',
                  'followers_count', 'friends_count', 'media_count', 'account_age_days',
                  'current_volume', 'next_available_volume']:
            types[c] = LongType()
        for c in ['sentiment_score', 'interaction_score', 'favorite_ratio', 'reply_ratio',
                  'credibility_score', 'current_open', 'current_close', 'current_high', 'current_low']:
            types[c] = DoubleType()
        for c in ['is_viral', 'is_blue_verified', 'is_new_account', 'is_influencer']:
            types[c] = BooleanType()
        values = {c: None for c in types}
        values.update(tweet_id='old', user_id='user-old', full_text='historic tweet',
                      tweet_timestamp_utc=datetime(2026, 9, 15, 12),
                      tweet_timestamp_et=datetime(2026, 9, 15, 8),
                      prediction_session_date=date(2026, 9, 16), next_trading_date=date(2026, 9, 17),
                      next_available_volume=94191300, current_volume=85130000,
                      sentiment_score=.1, interaction_score=10., favorite_ratio=.01,
                      reply_ratio=.02, credibility_score=1., followers_count=100,
                      friends_count=5, media_count=2, account_age_days=100,
                      is_viral=False, is_blue_verified=False, is_new_account=True, is_influencer=False)
        return self.spark.createDataFrame([values], StructType([StructField(c, t, True) for c, t in types.items()]))

    def stream_tweet(self, tweet_id, timestamp, **kw):
        values = dict(tweet_id=tweet_id, user_id='daily-user', tweet_timestamp_utc=timestamp,
                      tweet_timestamp_et='2026-09-23T00:00:00-04:00',  # untrusted date: recompute from UTC
                      candidate_prediction_session_date='2026-09-23',
                      full_text='NVDA test', account_created_date='2020-01-01',
                      sentiment_score=.6, interaction_score=3000., favorite_ratio=.2,
                      reply_ratio=.1, credibility_score=4., followers_count=500,
                      friends_count=5, media_count=2, account_age_days=2000,
                      favorite_count=3, retweet_count=2, reply_count=1, quote_count=0, view_count=50,
                      is_viral=True, is_blue_verified=True, is_new_account=False, is_influencer=True)
        values.update(kw)
        return values

    def market_fixture(self):
        pdf = pd.DataFrame(dict(Open=[100., 101., 102.], Close=[101., 102., 103.],
                                High=[102., 103., 104.], Low=[99., 100., 101.],
                                Volume=[85130000, 90234567, 94191300]),
                           index=pd.to_datetime(['2026-09-15', '2026-09-16', '2026-09-17']))
        pdf.index.name = 'Date'
        return pdf

    def test_daily_tweet_and_market_outputs_reach_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = self.historical_fixture()
            tweets = [self.stream_tweet('new16', '2026-09-16T15:00:00+00:00'),
                      self.stream_tweet('late15', '2026-09-16T02:00:00+00:00'),
                      self.stream_tweet('old', '2026-09-15T12:00:00+00:00', interaction_score=20.)]
            for name, observed, rows in [
                ('first', '2026-09-17T09:00:00+00:00', tweets),
                ('later', '2026-09-18T09:00:00+00:00', [dict(tweets[0], interaction_score=4000.)])
            ]:
                path = Path(tmp) / name
                path.mkdir()
                (path / 'tweets_gold.json').write_text(json.dumps(dict(generated_at_utc=observed, tweets=rows)))
            envelopes = (self.spark.read.option('multiline', 'true').option('recursiveFileLookup', 'true')
                         .json(tmp).withColumn('_source_file', F.input_file_name()))
            merged = self.env['merge_daily_tweets'](h, envelopes)
            rows = {r.tweet_id: r for r in merged.collect()}
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows['new16'].interaction_score, 4000.)
            self.assertEqual(rows['late15'].tweet_timestamp_et.date(), date(2026, 9, 15))
            self.assertEqual(rows['old'].next_available_volume, 94191300)
            self.assertEqual(rows['old'].prediction_session_date, date(2026, 9, 16))
            self.assertIsNone(rows['new16'].prediction_session_date)  # never invent training sessions
            rerun = self.env['merge_daily_tweets'](merged, envelopes)
            self.assertEqual(sorted(rerun.collect()), sorted(merged.collect()))
            self.assertEqual(h.count(), 1)  # no mutation of the training input

            env = dict(self.env, df=merged.withColumn('tweet_created_at_date', F.to_date('tweet_timestamp_et')),
                       historical_bi_df=h, predicted_df=None,
                       market_daily_df=self.env['completed_market_frame'](self.market_fixture(), date(2026, 9, 18)),
                       AGG_ROOT=tmp + '/out/')
            exec(cell(1), env)
            # Use the production writer against temporary Parquet directories.
            env['aggregate_tables'] = {name: env[var] for name, var in [
                ('user_profile', 'user_profile_df'), ('daily_tweet_summary', 'daily_tweet_summary'),
                ('market_summary_per_day', 'market_summary_per_day'), ('viral_tweet_log', 'viral_tweet_log'),
                ('user_type_distribution_per_day', 'user_type_distribution_per_day'),
                ('top_users_by_tweet_count', 'top_users_by_tweet_count'),
                ('top_users_by_viral_tweets', 'top_users_by_viral_tweets'),
                ('influencer_monthly_engagement', 'influencer_monthly_engagement')]}
            exec(cell(3), env)
            daily = self.spark.read.parquet(tmp + '/out/daily_tweet_summary').filter("tweet_created_at_date = '2026-09-16'").first()
            self.assertEqual(daily.total_tweets, 1)
            self.assertEqual(daily.avg_sentiment, .6)
            self.assertEqual(daily.avg_interaction_score, 4000.)
            self.assertEqual(daily.avg_followers, 500.)
            self.assertEqual(daily.avg_credibility_score, 4.)
            account = self.spark.read.parquet(tmp + '/out/user_type_distribution_per_day').filter("tweet_created_at_date = '2026-09-16'").first()
            self.assertEqual(account.influencer_tweet_count, 1)
            self.assertEqual(account.blue_verified_tweet_count, 1)
            market = {r.tweet_created_at_date: r for r in self.spark.read.parquet(tmp + '/out/market_summary_per_day').collect()}
            self.assertEqual(market[date(2026, 9, 16)].current_volume, 90234567.)
            self.assertEqual(market[date(2026, 9, 16)].actual_volume, 94191300.)
            self.assertEqual(market[date(2026, 9, 17)].current_volume, 94191300.)
            self.assertEqual(market[date(2026, 9, 17)].tweet_count, 0)
            self.assertIsNone(market[date(2026, 9, 17)].avg_sentiment)
            self.assertNotIn(date(2026, 9, 23), market)
            self.assertEqual(self.spark.read.parquet(tmp + '/out/viral_tweet_log').filter("tweet_created_at_date = '2026-09-16'").count(), 1)

    def test_historical_recovery_pages_are_read_by_gold(self):
        from test_tweet_backfill import m, page, tweet, env as features, NOW
        with tempfile.TemporaryDirectory() as tmp:
            def persist(path, envelope):
                target = Path(tmp) / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(envelope))
            args = dict(fetch_page=lambda *_: page([tweet(), tweet(),
                        tweet('late18', 'Sat Sep 19 03:00:00 +0000 2026')]),
                        build_row=features['build_feature_row'], write_page=persist,
                        query='$NVDA lang:en', now=NOW)
            body = {'start_date': '2026-09-18', 'end_date': '2026-09-18'}
            m.run_backfill(body, **args)
            m.run_backfill(body, **args)
            envelopes = (self.spark.read.option('multiline', 'true')
                         .option('recursiveFileLookup', 'true')
                         .option('pathGlobFilter', 'tweets_gold.json').json(tmp + '/stream')
                         .withColumn('_source_file', F.input_file_name()))
            merged = self.env['merge_daily_tweets'](self.historical_fixture(), envelopes)
            rows = merged.filter("to_date(tweet_timestamp_et) = '2026-09-18'").collect()
            self.assertEqual({r.tweet_id for r in rows}, {'one', 'late18'})
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(r.prediction_session_date is None for r in rows))
            self.assertTrue(all(r.sentiment_score == .25 for r in rows))
            self.assertEqual(merged.filter("tweet_id = 'old'").first().next_available_volume, 94191300)

    def test_market_rejects_unfinished_and_invalid_bars(self):
        pdf = self.market_fixture()
        pdf.columns = pd.MultiIndex.from_tuples([(c, 'NVDA') for c in pdf.columns])
        rows = self.env['completed_market_frame'](pdf, date(2026, 9, 17)).collect()
        self.assertEqual({r.tweet_created_at_date for r in rows}, {date(2026, 9, 15), date(2026, 9, 16)})
        pdf[('Volume', 'NVDA')] = float('nan')
        with self.assertRaisesRegex(ValueError, 'No completed market bars'):
            self.env['completed_market_frame'](pdf, date(2026, 9, 18))

    def test_sql_date_dimension_includes_prediction_dates(self):
        sql = (ROOT / 'sql/04_sp_daily_refresh.sql').read_text()
        native = json.loads((ROOT / 'synapse/sqlscript/04_sp_DailyRefresh.json').read_text())['properties']['content']['query']
        self.assertEqual(sql, native)
        dim = sql.split(') dates', 1)[0]
        self.assertIn('SELECT prediction_date\n        FROM dbo.ext_PredictionHistory', dim)
        self.assertIn('SELECT target_date\n        FROM dbo.ext_PredictionHistory', dim)


if __name__ == '__main__':
    unittest.main()
