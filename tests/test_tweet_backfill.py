"""No credentials or API calls: exercise the production recovery engine."""
import ast
import importlib.util
import json
from datetime import datetime, timezone, date
from pathlib import Path
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('tweet_backfill', ROOT / 'function-app/tweet_backfill.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
NOW = datetime(2026, 9, 23, 15, tzinfo=timezone.utc)


def tweet(id='one', stamp='Fri Sep 18 15:00:00 +0000 2026'):
    return {'rest_id': id, 'legacy': {'created_at': stamp, 'full_text': '$NVDA'},
            'core': {'user_results': {'result': {'rest_id': 'user-1'}}}}


def page(tweets=(), cursor=None):
    entries = [{'content': {'itemContent': {'tweet_results': {'result': t}}}} for t in tweets]
    if cursor:
        entries.append({'content': {'cursorType': 'Bottom', 'value': cursor}})
    return {'result': {'timeline': {'instructions': [{'entries': entries}]}}}


# Use the actual feature builder without importing the Azure host or downloading VADER.
source = ast.parse((ROOT / 'function-app/function_app.py').read_text())
names = {'safe_int', 'clean_text', 'parse_twitter_datetime', 'build_feature_row'}
env = dict(datetime=datetime, ET=m.ET, re=__import__('re'),
           SIA=Mock(polarity_scores=Mock(return_value={'compound': .25})))
exec(compile(ast.Module(body=[n for n in source.body if isinstance(n, ast.FunctionDef) and n.name in names],
                        type_ignores=[]), '<feature-builder>', 'exec'), env)


class TweetBackfillTest(unittest.TestCase):
    def run_recovery(self, responses, body=None, saved=None):
        saved = {} if saved is None else saved
        fetch = Mock(side_effect=responses)
        result = m.run_backfill(body or {'start_date': '2026-09-18', 'end_date': '2026-09-18'},
                                fetch_page=fetch, build_row=env['build_feature_row'],
                                write_page=lambda p, e: saved.__setitem__(p, e), query='$NVDA lang:en', now=NOW)
        return result, saved, fetch

    def test_et_bounds_duplicates_and_safe_replay(self):
        tweets = [tweet(), tweet(), tweet('late', 'Sat Sep 19 03:59:00 +0000 2026'),
                  tweet('early', 'Fri Sep 18 03:59:00 +0000 2026'),
                  tweet('next', 'Sat Sep 19 04:00:00 +0000 2026')]
        result, saved, fetch = self.run_recovery([page(tweets)])
        self.assertEqual(result['status'], 'finished')
        self.assertEqual(len(saved), 1)
        path, output = next(iter(saved.items()))
        self.assertTrue(path.startswith('stream/backfill/2026/09/18/'))
        self.assertEqual({r['tweet_id'] for r in output['tweets']}, {'one', 'late'})
        self.assertNotIn('candidate_prediction_session_date', output)
        self.assertEqual(output['source_kind'], 'historical_bi_backfill')
        self.assertIn('until:2026-09-20', fetch.call_args.args[0])
        self.run_recovery([page(tweets)], saved=saved)
        self.assertEqual(len(saved), 1)

    def test_page_limit_and_resume(self):
        body = {'start_date': '2026-09-18', 'end_date': '2026-09-18', 'max_pages': 1}
        result, saved, _ = self.run_recovery([page([tweet()], 'NEXT')], body)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['next_request']['cursor'], 'NEXT')
        result, saved, fetch = self.run_recovery([page([tweet('second')])], result['next_request'], saved)
        self.assertIsNone(result['next_request'])
        self.assertEqual(len(saved), 2)
        self.assertEqual(fetch.call_args.args[1], 'NEXT')

    def test_range_advances_and_empty_day_does_not_fabricate_rows(self):
        result, saved, _ = self.run_recovery([page(), page(), page()],
                            {'start_date': '2026-09-18', 'end_date': '2026-09-21'})
        self.assertEqual(saved, {})
        self.assertEqual(result['provider_exhausted_dates'], ['2026-09-18', '2026-09-19', '2026-09-20'])
        self.assertEqual(result['next_request']['start_date'], '2026-09-21')
        self.assertNotIn('cursor', result['next_request'])

    def test_empty_cursor_page_continues_and_loop_fails(self):
        result, _, fetch = self.run_recovery([page(cursor='A'), page([tweet()])])
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(result['status'], 'finished')
        with self.assertRaisesRegex(m.ProviderPayloadError, 'Repeated cursor'):
            self.run_recovery([page([tweet()], 'A'), page([tweet()], 'A')])

    def test_malformed_payload_or_tweet_does_not_publish(self):
        for payload in [{}, {'error': 'not subscribed'}, page([tweet(stamp='bad')]),
                        page([{'rest_id': 'unknown'}])]:
            saved = {}
            with self.assertRaises(m.ProviderPayloadError):
                self.run_recovery([payload], saved=saved)
            self.assertEqual(saved, {})

    def test_failure_after_page_can_be_retried(self):
        saved = {}
        with self.assertRaisesRegex(RuntimeError, '403'):
            self.run_recovery([page([tweet()], 'A'), RuntimeError('403')], saved=saved)
        self.assertEqual(len(saved), 1)
        self.run_recovery([page([tweet()], 'A'), page([tweet('second')])], saved=saved)
        self.assertEqual(len(saved), 2)

    def test_diagnostics_september17_excludes_newer_results(self):
        result, saved, _ = self.run_recovery([page([tweet()], 'NEXT')],
            {'start_date': '2026-09-17', 'end_date': '2026-09-22', 'max_pages': 1})
        self.assertEqual(saved, {})
        d = result['page_diagnostics'][0]
        self.assertEqual(d['target_date_et'], '2026-09-17')
        self.assertEqual(d['parsed_tweets'], 1)
        self.assertEqual(d['excluded_after_target_date'], 1)
        self.assertEqual(d['earliest_tweet_et'], '2026-09-18T11:00:00-04:00')
        self.assertEqual(d['outcome'], 'all_outside_target_date')
        self.assertTrue(d['has_next_cursor'])
        self.assertEqual(result['next_request']['cursor'], 'NEXT')

    def test_diagnostics_empty_provider_page_is_distinct(self):
        result, saved, _ = self.run_recovery([page(cursor='NEXT')],
            {'start_date': '2026-09-17', 'end_date': '2026-09-22', 'max_pages': 1})
        d = result['page_diagnostics'][0]
        self.assertEqual(d['outcome'], 'no_parsed_tweets')
        self.assertEqual(d['parsed_tweets'], 0)
        self.assertIsNone(d['earliest_tweet_et'])
        self.assertIsNone(d['latest_tweet_et'])
        self.assertEqual(saved, {})
        self.assertNotIn('NEXT', json.dumps(d))

    def test_diagnostics_counts_et_boundaries_and_duplicates(self):
        result, saved, _ = self.run_recovery([page([
            tweet(), tweet(),
            tweet('early', 'Fri Sep 18 03:59:00 +0000 2026'),
            tweet('late', 'Sat Sep 19 03:59:00 +0000 2026'),
            tweet('next', 'Sat Sep 19 04:00:00 +0000 2026')])])
        d = result['page_diagnostics'][0]
        self.assertEqual(d['parsed_tweets'], 5)
        self.assertEqual(d['excluded_before_target_date'], 1)
        self.assertEqual(d['excluded_after_target_date'], 1)
        self.assertEqual(d['duplicate_matching_ids'], 1)
        self.assertEqual(d['written_tweets'], 2)
        self.assertEqual(d['outcome'], 'written')
        self.assertFalse(d['has_next_cursor'])
        self.assertEqual(next(iter(saved.values()))['tweet_count'], 2)

    def test_validation_rejects_future_reversed_long_or_unbounded_requests(self):
        for body in [None, {}, {'start_date': 'bad', 'end_date': '2026-09-18'},
                     {'start_date': '2026-09-18', 'end_date': '2026-09-23'},
                     {'start_date': '2026-09-18', 'end_date': '2026-09-17'},
                     {'start_date': '2026-09-01', 'end_date': '2026-09-18'},
                     {'start_date': '2026-09-18', 'end_date': '2026-09-18', 'max_pages': 100},
                     {'start_date': '2026-09-18', 'end_date': '2026-09-18', 'max_pages': True}]:
            with self.assertRaises(ValueError):
                m.validate_request(body, date(2026, 9, 23))

    def test_visibility_wrapper_and_outer_cursor(self):
        payload = page([{'__typename': 'TweetWithVisibilityResults', 'tweet': tweet()}])
        payload['cursor'] = {'bottom': 'NEXT'}
        tweets, cursor = m.parse_search_page(payload)
        self.assertEqual(tweets[0]['rest_id'], 'one')
        self.assertEqual(cursor, 'NEXT')


if __name__ == '__main__':
    unittest.main()
