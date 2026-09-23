"""Calendar, input cutoff and archive metadata in the actual prediction notebook."""
import json
import unittest
from pathlib import Path
from datetime import date, datetime, timezone
from types import SimpleNamespace
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]

def cell(i):
    p = json.loads((ROOT / 'synapse/notebook/BlobStreamPredict.json').read_text())
    return ''.join(p['properties']['cells'][i]['source'])


class PredictionSessionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('session-test')
                     .config('spark.sql.shuffle.partitions', '2')
                     .config('spark.sql.session.timeZone', 'UTC')
                     .config('spark.ui.enabled', 'false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.env = dict(spark=cls.spark, display=lambda *_: None)
        exec(cell(0), cls.env)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_eastern_default_and_weekend_holiday_targets(self):
        resolve = self.env['resolve_prediction_session']
        # UTC September 18 is still September 17 in New York.
        now = datetime(2026, 9, 18, 1, tzinfo=timezone.utc)
        self.assertEqual(resolve('', now), (date(2026, 9, 17), date(2026, 9, 18)))
        now = datetime(2026, 9, 23, 15, tzinfo=timezone.utc)
        self.assertEqual(resolve('2026-09-18', now)[1], date(2026, 9, 21))
        self.assertEqual(resolve('2026-09-04', now)[1], date(2026, 9, 8))
        self.assertIsNone(resolve('2026-09-19', now)[1])
        with self.assertRaises(ValueError):
            resolve('2026-09-24', now)
        pipeline = json.loads((ROOT / 'synapse/pipeline/PL_DailyPrediction.json').read_text())['properties']
        self.assertEqual(pipeline['parameters']['p_session_date']['defaultValue'], '')
        expression = pipeline['activities'][0]['typeProperties']['parameters']['TEST_SESSION_DATE']['value']['value']
        self.assertIn('pipeline().TriggerTime', expression)
        self.assertIn('Eastern Standard Time', expression)

    def test_no_same_day_tweets_required_but_cutoff_enforced(self):
        # A morning prediction can validly use the previous day's archived tweets.
        rows = [('old', '2026-09-16T18:00:00Z', '2026-09-17'),
                ('old', '2026-09-16T18:00:00Z', '2026-09-17'),
                ('late', '2026-09-17T10:00:00Z', '2026-09-17')]
        env = dict(self.env, session_day=date(2026, 9, 17),
                   cutoff_utc=datetime(2026, 9, 17, 10, tzinfo=timezone.utc))
        env['streaming_df'] = self.spark.createDataFrame(rows,
            'tweet_id string, tweet_timestamp_utc string, candidate_prediction_session_date string')
        exec(cell(4), env)
        eligible = env['stream_features_df'].collect()
        self.assertEqual([r.tweet_id for r in eligible], ['old'])
        self.assertEqual(eligible[0].prediction_session_date, date(2026, 9, 17))
        env['streaming_df'] = env['streaming_df'].filter("tweet_id = 'late'")
        with self.assertRaisesRegex(ValueError, 'No tweets eligible'):
            exec(cell(4), env)

    def test_saved_target_and_late_observation_rejection(self):
        env = dict(self.env, session_day=date(2026, 9, 17), target_day=date(2026, 9, 18))
        env['daily_prediction_df'] = self.spark.createDataFrame([(128255294.133,)], 'predicted_next_volume double')
        env['datetime'] = SimpleNamespace(now=lambda _: datetime(2026, 9, 23, 15, tzinfo=timezone.utc))
        exec(cell(12), env)
        env['datetime'] = datetime
        row = env['daily_prediction_df'].first()
        self.assertEqual(row.target_date, date(2026, 9, 18))
        self.assertEqual(row.prediction_run_type, 'replay')
        validation = 'from datetime import time' + cell(2).split('from datetime import time', 1)[1]
        env['raw_stream_df'] = self.spark.createDataFrame([('2026-09-23T09:00:00Z',)], 'generated_at_utc string')
        with self.assertRaisesRegex(ValueError, 'after the session cutoff'):
            exec(validation, env)

    def test_calendar_implementation_matches_evaluator(self):
        source = ''.join(json.loads((ROOT / 'synapse/notebook/GoldAggregate.json').read_text())
                         ['properties']['cells'][0]['source'])
        helper = cell(0).split('def nyse_target_pairs', 1)[1].split('def resolve_prediction_session', 1)[0].strip()
        other = source.split('def nyse_target_pairs', 1)[1].split('def evaluate_predictions', 1)[0].strip()
        self.assertEqual(helper, other)


if __name__ == '__main__':
    unittest.main()
