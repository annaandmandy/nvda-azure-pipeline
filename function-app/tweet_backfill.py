"""Resumable BI-only recovery of provider search results by Eastern date."""
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')


class ProviderPayloadError(RuntimeError):
    pass


def validate_request(body, today=None):
    if not isinstance(body, dict):
        raise ValueError('JSON body must be an object')
    allowed = {'start_date', 'end_date', 'cursor', 'max_pages'}
    if set(body) - allowed:
        raise ValueError('Unknown parameters; use start_date, end_date, cursor, max_pages')
    try:
        start = date.fromisoformat(body['start_date'])
        end = date.fromisoformat(body['end_date'])
    except (KeyError, ValueError, TypeError):
        raise ValueError('start_date and end_date must be YYYY-MM-DD') from None
    today = today or datetime.now(ET).date()
    if start > end or (end - start).days >= 7 or end >= today:
        raise ValueError('Use 1–7 completed Eastern calendar days, with an inclusive end_date')
    pages = body.get('max_pages', 3)
    if type(pages) is not int or not 1 <= pages <= 3:
        raise ValueError('max_pages must be an integer from 1 to 3')
    cursor = body.get('cursor')
    if cursor is not None and (not isinstance(cursor, str) or not cursor or len(cursor) > 8192):
        raise ValueError('cursor must be a nonempty string returned by this endpoint')
    return start, end, cursor, pages


def parse_search_page(payload):
    # Match the timeline envelope consumed by the production ingestion function.
    try:
        instructions = payload['result']['timeline']['instructions']
    except (KeyError, TypeError):
        raise ProviderPayloadError('Missing result.timeline.instructions; no recovery page written') from None
    if not isinstance(instructions, list) or payload.get('errors') or payload.get('error'):
        raise ProviderPayloadError('Invalid provider response; no recovery page written')
    tweets, cursors = [], []

    def walk(node):
        if isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, dict):
            if str(node.get('cursorType', '')).lower() == 'bottom':
                value = node.get('value')
                if not isinstance(value, str) or not value:
                    raise ProviderPayloadError('Invalid bottom cursor')
                cursors.append(value)
            if 'tweet_results' in node:
                result = node['tweet_results'].get('result', {})
                if result.get('__typename') == 'TweetWithVisibilityResults':
                    result = result.get('tweet', {})
                if result.get('legacy'):
                    tweets.append(result)
                elif result.get('__typename') not in ('TweetUnavailable', 'TweetTombstone'):
                    raise ProviderPayloadError('Unrecognized tweet result; refusing silent data loss')
            else:
                for value in node.values():
                    walk(value)

    walk(instructions)
    # Some twitter241 responses expose the same continuation beside result.
    outer = payload.get('cursor')
    if isinstance(outer, dict) and outer.get('bottom'):
        cursors.append(outer['bottom'])
    unique = set(cursors)
    if len(unique) > 1:
        raise ProviderPayloadError('Ambiguous pagination cursors')
    return tweets, next(iter(unique), None)


def run_backfill(body, *, fetch_page, build_row, write_page, query, now=None):
    """At most three API requests; next_request resumes the same inclusive range.

    fetch_page(query, cursor) -> provider JSON; write_page(path, envelope) persists
    one deterministic page. Retries replace that page, never the live archives.
    """
    now = now or datetime.now(timezone.utc)
    day, end, cursor, limit = validate_request(body, now.astimezone(ET).date())
    pages, written, days = 0, [], []
    seen_cursors = {cursor} if cursor else set()
    while day <= end and pages < limit:
        # Date-only search bounds may be interpreted as UTC. Include the next UTC
        # day and filter exact Eastern dates locally, including the late evening.
        search = f'{query} since:{day.isoformat()} until:{(day + timedelta(days=2)).isoformat()}'
        tweets, next_cursor = parse_search_page(fetch_page(search, cursor))
        pages += 1
        if next_cursor and next_cursor in seen_cursors:
            raise ProviderPayloadError('Repeated cursor; resume safely after checking provider pagination')
        rows = {}
        for tweet in tweets:
            row = build_row(tweet)
            if not row or not row.get('tweet_id') or not row.get('user_id'):
                raise ProviderPayloadError('Missing tweet identity/timestamp; page not written')
            stamp = datetime.fromisoformat(row['tweet_timestamp_utc'])
            if stamp.tzinfo is None:
                raise ProviderPayloadError('Tweet timestamp has no timezone')
            if stamp.astimezone(ET).date() != day:
                continue
            row = dict(row)
            row.pop('candidate_prediction_session_date', None)
            rows[row['tweet_id']] = row
        if rows:
            page_id = hashlib.sha256(json.dumps([search, cursor], ensure_ascii=False).encode()).hexdigest()
            path = f'stream/backfill/{day:%Y/%m/%d}/{page_id}/tweets_gold.json'
            envelope = {
                'generated_at_utc': now.isoformat(),
                'generated_at_et': now.astimezone(ET).isoformat(),
                'source_kind': 'historical_bi_backfill',
                'backfill_date_et': day.isoformat(),
                'coverage': 'provider_search_results_only',
                'tweet_count': len(rows),
                'tweets': list(rows.values()),
            }
            write_page(path, envelope)
            written.append({'blob': path, 'tweet_count': len(rows)})
        # A cursor-bearing empty page can be a provider gap. Continue to its
        # cursor rather than declaring exhaustion from tweet count alone.
        if next_cursor:
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            days.append(day.isoformat())
            day += timedelta(days=1)
            cursor = None
            seen_cursors = set()
    continuation = None
    if day <= end:
        continuation = {'start_date': day.isoformat(), 'end_date': end.isoformat(), 'max_pages': limit}
        if cursor:
            continuation['cursor'] = cursor
    return {
        'status': 'partial' if continuation else 'finished',
        'pages_fetched': pages,
        'written_pages': written,
        'provider_exhausted_dates': days,
        'coverage': 'provider_search_results_only; not guaranteed complete X coverage',
        'next_request': continuation,
    }
