"""Run with Python + PySpark 3.5 and Java 17: python -m unittest discover -s tests."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import pandas as pd

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]


def cell(name, index):
    return ''.join(json.loads((ROOT / 'synapse/notebook' / (name + '.json')).read_text())
                   ['properties']['cells'][index]['source'])


class PredictionBackfillTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('prediction-regression')
                     .config('spark.sql.shuffle.partitions', '2')
                     .config('spark.ui.enabled', 'false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def evaluation_env(self, pred, bars, end=date(2026, 9, 23)):
        env = dict(spark=self.spark, F=F, display=lambda *_: None)
        exec(cell('GoldAggregate', 0).split('# Read Historical Gold and archived daily tweets for BI')[0], env)
        market = self.spark.createDataFrame(bars, 'tweet_created_at_date date, current_volume double')
        return env['evaluate_predictions'](pred, market, end), env

    def forecasts(self, days):
        return self.spark.createDataFrame([
            (d, 128255294.133, 'v1', datetime(2026, 9, 23, 15), 20, .1, .2) for d in days
        ], 'prediction_session_date date, predicted_next_volume double, model_version string, prediction_generated_at_utc timestamp, tweet_count long, avg_sentiment_score double, avg_interaction_score double')

    def test_missing_historical_tweets_do_not_block_target_or_actual(self):
        pred = self.forecasts([date(2026, 9, 16), date(2026, 9, 17), date(2026, 9, 18)])
        history, _ = self.evaluation_env(pred, [(date(2026, 9, 17), 94191300.),
                                              (date(2026, 9, 18), 190290000.),
                                              (date(2026, 9, 21), 109810000.)])
        rows = {r.prediction_date: r for r in history.collect()}
        for session, target, volume in [(16, 17, 94191300.), (17, 18, 190290000.), (18, 21, 109810000.)]:
            row = rows[date(2026, 9, session)]
            self.assertEqual((row.target_date, row.actual_volume, row.evaluation_status),
                             (date(2026, 9, target), volume, 'Evaluated'))
        # There is no historical dataframe at all: this path must remain independent.
        self.assertAlmostEqual(rows[date(2026, 9, 17)].prediction_accuracy,
                               1 - abs(190290000 - 128255294.133) / 190290000)

    def test_missing_bar_does_not_shift_target_to_next_available_bar(self):
        history, _ = self.evaluation_env(self.forecasts([date(2026, 9, 17)]),
                                          [(date(2026, 9, 21), 109810000.)])
        row = history.first()
        self.assertEqual(row.target_date, date(2026, 9, 18))
        self.assertIsNone(row.actual_volume)
        self.assertEqual(row.evaluation_status, 'Pending')

    def test_archived_target_checked_and_holiday_resolved(self):
        pred = self.forecasts([date(2026, 9, 4)]).withColumn('target_date', F.lit('2026-09-08'))
        history, _ = self.evaluation_env(pred, [(date(2026, 9, 8), 123456.)])
        self.assertEqual(history.first().target_date, date(2026, 9, 8))
        with self.assertRaisesRegex(ValueError, 'conflicts'):
            self.evaluation_env(pred.withColumn('target_date', F.lit('2026-09-07')), [])

    def test_unfinished_and_invalid_volumes_stay_pending(self):
        for volume in [94191300., 0., -1., float('nan'), float('inf')]:
            history, _ = self.evaluation_env(self.forecasts([date(2026, 9, 16)]),
                    [(date(2026, 9, 17), volume)], end=date(2026, 9, 17))
            self.assertEqual(history.first().evaluation_status, 'Pending')
        for volume in [0., -1., float('nan'), float('inf')]:
            history, _ = self.evaluation_env(self.forecasts([date(2026, 9, 16)]),
                    [(date(2026, 9, 17), volume)])
            self.assertIsNone(history.first().actual_volume)

    def test_market_download_range_and_invalid_bars(self):
        historical = self.spark.createDataFrame([
            (date(2026, 7, 1), None)
        ], 'next_trading_date date, next_available_volume long')
        for hour, expected_end in [(12, '2026-09-17'), (18, '2026-09-18')]:
            calls = []
            def download(*args, **kwargs):
                calls.append(kwargs)
                frame = pd.DataFrame(
                    [[94191300.], [0.], [float('nan')], [-1.]],
                    index=pd.to_datetime(['2026-09-17', '2026-09-16', '2026-09-15', '2026-09-14']),
                    columns=pd.MultiIndex.from_tuples([('Volume', 'NVDA')]))
                frame.index.name = 'Date'
                return frame
            env = dict(F=F, historical_gold_df=historical, timedelta=timedelta,
                       ET=ZoneInfo('America/New_York'),
                       datetime=SimpleNamespace(now=lambda _: datetime(2026, 9, 17, hour)),
                       yf=SimpleNamespace(download=download))
            exec(cell('BackfillRetrain', 5), env)
            self.assertEqual(calls[0]['start'], '2026-07-01')
            self.assertEqual(calls[0]['end'], expected_end)
            self.assertEqual(env['stock_lookup'], {} if hour == 12 else {'2026-09-17': 94191300})

    def test_published_actual_reaches_prediction_history_and_sql(self):
        with tempfile.TemporaryDirectory() as tmp:
            production, staging, aggregate = [str(Path(tmp) / x) for x in ('historical', 'staging', 'aggregate')]
            # Two sessions share a target: the label join must not multiply tweets.
            historical = self.spark.createDataFrame([
                ('a', date(2026, 9, 16), date(2026, 9, 17), None),
                ('b', date(2026, 9, 16), date(2026, 9, 17), None),
                ('c', date(2026, 9, 15), date(2026, 9, 17), None),
                ('d', date(2026, 9, 17), date(2026, 9, 18), None),
                ('e', date(2026, 9, 14), date(2026, 9, 15), 123456),
            ], 'tweet_id string, prediction_session_date date, next_trading_date date, next_available_volume long')
            historical.write.parquet(production)
            env = dict(spark=self.spark, F=F, display=lambda *_: None,
                       HISTORICAL_GOLD_PATH=production, HISTORICAL_GOLD_STAGING_PATH=staging,
                       historical_gold_df=self.spark.read.parquet(production),
                       stock_lookup={'2026-09-17': 94191300}, retrain_required=False)
            for i in (6, 7, 8, 9, 10, 17):
                exec(cell('BackfillRetrain', i), env)
            published = self.spark.read.parquet(production)
            rows = {r.tweet_id: r.next_available_volume for r in published.collect()}
            self.assertEqual(rows, {'a': 94191300, 'b': 94191300, 'c': 94191300, 'd': None, 'e': 123456})
            self.assertFalse(env['retrain_required'])
            # Training is rebound to valid files after production overwrite.
            self.assertEqual(env['updated_gold_df'].count(), 5)
            # A second run has no new labels and leaves production unchanged.
            for i in (6, 8, 9, 10, 17):
                exec(cell('BackfillRetrain', i), env)
            self.assertEqual(env['backfill_count'], 0)
            self.assertEqual(self.spark.read.parquet(production).count(), 5)

            pred = self.spark.createDataFrame([
                (date(2026, 9, 16), 90000000., 'old', datetime(2026, 9, 16, 9), 2, .1, .2),
                (date(2026, 9, 16), 95000000., 'latest', datetime(2026, 9, 16, 10), 2, .1, .2),
                (date(2026, 9, 17), 96000000., 'latest', datetime(2026, 9, 17, 10), 1, .1, .2),
            ], 'prediction_session_date date, predicted_next_volume double, model_version string, prediction_generated_at_utc timestamp, tweet_count long, avg_sentiment_score double, avg_interaction_score double')
            env.update(pred=pred, GOLD_PATH=production, AGG_ROOT=tmp + '/')
            history_df, _ = self.evaluation_env(pred, [(date(2026, 9, 17), 94191300.), (date(2026, 9, 18), 190290000.)])
            env.update(prediction_history=history_df, PREDICTION_AGG_PATH=tmp + '/prediction_history')
            exec(cell('GoldAggregate', 6), env)
            aggregate = str(Path(tmp) / 'prediction_history')
            history = self.spark.read.parquet(aggregate).orderBy('prediction_date').collect()
            self.assertEqual(len(history), 2)
            row = history[0]
            self.assertEqual((row.target_date, row.actual_volume, row.evaluation_status, row.model_version),
                             (date(2026, 9, 17), 94191300., 'Evaluated', 'latest'))
            self.assertAlmostEqual(row.prediction_accuracy, 1 - abs(94191300 - 95000000) / 94191300)
            self.assertEqual(history[1].evaluation_status, 'Evaluated')
            self.assertEqual(history[1].target_date, date(2026, 9, 18))
            self.assertEqual(history[1].actual_volume, 190290000.)

            # Execute the repository's FactPrediction INSERT unchanged against a
            # local SQL harness. This verifies projection, not Synapse connectivity.
            conn = sqlite3.connect(':memory:')
            for name, index in [('YEAR', 0), ('MONTH', 1), ('DAY', 2)]:
                conn.create_function(name, 1, lambda x, i=index: int(str(x).split('-')[i]))
            conn.execute("ATTACH DATABASE ':memory:' AS dbo")
            names = env['prediction_history'].columns
            conn.execute('CREATE TABLE dbo.ext_PredictionHistory (' + ','.join(names) + ')')
            for r in history:
                conn.execute('INSERT INTO dbo.ext_PredictionHistory VALUES (' + ','.join('?' for _ in names) + ')',
                             [str(v) if isinstance(v, (date, datetime)) else v for v in r])
            columns = 'PredictionDateKey,TargetDateKey,PredictedVolume,ActualVolume,PredictionAccuracy,EvaluationStatus,ModelVersion,PredictionGeneratedAtUTC,TweetCount,AvgSentimentScore,AvgInteractionScore'
            conn.execute('CREATE TABLE dbo.FactPrediction (' + columns + ')')
            sql = (ROOT / 'sql/04_sp_daily_refresh.sql').read_text()
            insert = 'INSERT INTO dbo.FactPrediction' + sql.split('INSERT INTO dbo.FactPrediction', 1)[1].split('END;', 1)[0]
            conn.execute(insert)
            actual = conn.execute('SELECT TargetDateKey,ActualVolume,EvaluationStatus FROM dbo.FactPrediction WHERE PredictionDateKey=20260916').fetchone()
            self.assertEqual(actual, (20260917, 94191300., 'Evaluated'))
            actual17 = conn.execute('SELECT TargetDateKey,ActualVolume,EvaluationStatus FROM dbo.FactPrediction WHERE PredictionDateKey=20260917').fetchone()
            self.assertEqual(actual17, (20260918, 190290000., 'Evaluated'))
            conn.close()


if __name__ == '__main__':
    unittest.main()
