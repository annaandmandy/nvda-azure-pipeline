import azure.functions as func

import os
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import nltk

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()

ET = ZoneInfo("America/New_York")

RAPIDAPI_HOST = os.environ["RAPIDAPI_HOST"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]

STORAGE_ACCOUNT_NAME = os.environ["STORAGE_ACCOUNT_NAME"]
GOLD_CONTAINER = os.environ.get("GOLD_CONTAINER", "gold")

TWITTER_QUERY = os.environ.get(
    "TWITTER_QUERY",
    "$NVDA lang:en"
)

TWITTER_COUNT = int(
    os.environ.get("TWITTER_COUNT", "20")
)

PREDICTION_CUTOFF_HOUR_ET = int(
    os.environ.get(
        "PREDICTION_CUTOFF_HOUR_ET",
        "6"
    )
)


# ============================================================
# VADER
# ============================================================

try:
    nltk.data.find(
        "sentiment/vader_lexicon.zip"
    )
except LookupError:
    nltk.download(
        "vader_lexicon",
        quiet=True
    )

SIA = SentimentIntensityAnalyzer()


# ============================================================
# STORAGE
# ============================================================

def get_blob_service_client():

    credential = DefaultAzureCredential()

    account_url = (
        f"https://{STORAGE_ACCOUNT_NAME}"
        ".blob.core.windows.net"
    )

    return BlobServiceClient(
        account_url=account_url,
        credential=credential
    )


# ============================================================
# HELPERS
# ============================================================

def safe_int(value, default=0):

    try:
        if value is None:
            return default

        return int(value)

    except (ValueError, TypeError):
        return default


def clean_text(text):

    if not text:
        return ""

    text = re.sub(
        r"http\S+|www\.\S+",
        "",
        text
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


def parse_twitter_datetime(value):

    if not value:
        return None

    try:
        return datetime.strptime(
            value,
            "%a %b %d %H:%M:%S %z %Y"
        )

    except Exception:
        return None


# ============================================================
# RAPIDAPI
# ============================================================

def fetch_nvda_tweets():

    now_utc = datetime.now(timezone.utc)

    # Fetch a small recent window.
    # Session eligibility is enforced later using ET cutoff.
    since_date = (
        now_utc - timedelta(days=2)
    ).date().isoformat()

    until_date = (
        now_utc + timedelta(days=1)
    ).date().isoformat()

    query = (
        f"{TWITTER_QUERY} "
        f"since:{since_date} "
        f"until:{until_date}"
    )

    url = (
        f"https://{RAPIDAPI_HOST}/search-v2"
    )

    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY
    }

    params = {
        "type": "Top",
        "count": TWITTER_COUNT,
        "query": query
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# EXTRACT TWEET OBJECTS
# ============================================================

def extract_tweet_results(payload):

    results = []

    instructions = (
        payload
        .get("result", {})
        .get("timeline", {})
        .get("instructions", [])
    )

    for instruction in instructions:

        entries = instruction.get(
            "entries",
            []
        )

        for entry in entries:

            try:

                result = (
                    entry
                    ["content"]
                    ["itemContent"]
                    ["tweet_results"]
                    ["result"]
                )

                if result:
                    results.append(result)

            except (
                KeyError,
                TypeError
            ):
                continue

    return results


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def build_feature_row(tweet):

    legacy = tweet.get(
        "legacy",
        {}
    ) or {}

    core = tweet.get(
        "core",
        {}
    ) or {}

    user = (
        core
        .get("user_results", {})
        .get("result", {})
    ) or {}

    user_core = user.get(
        "core",
        {}
    ) or {}

    relationship = user.get(
        "relationship_counts",
        {}
    ) or {}

    tweet_counts = user.get(
        "tweet_counts",
        {}
    ) or {}

    verification = user.get(
        "verification",
        {}
    ) or {}

    views = tweet.get(
        "views",
        {}
    ) or {}

    # -----------------------------
    # Tweet timestamp
    # -----------------------------

    created_at_raw = legacy.get(
        "created_at"
    )

    tweet_dt_utc = parse_twitter_datetime(
        created_at_raw
    )

    if tweet_dt_utc is None:
        return None

    tweet_dt_et = (
        tweet_dt_utc.astimezone(ET)
    )

    # -----------------------------
    # Raw engagement
    # -----------------------------

    view_count = safe_int(
        views.get("count")
    )

    retweet_count = safe_int(
        legacy.get("retweet_count")
    )

    reply_count = safe_int(
        legacy.get("reply_count")
    )

    quote_count = safe_int(
        legacy.get("quote_count")
    )

    favorite_count = safe_int(
        legacy.get("favorite_count")
    )

    # -----------------------------
    # Text + VADER
    # -----------------------------

    full_text = (
        legacy.get("full_text")
        or ""
    )

    cleaned = clean_text(
        full_text
    )

    sentiment_score = float(
        SIA.polarity_scores(
            cleaned
        )["compound"]
    )

    # -----------------------------
    # Same formulas as historical
    # -----------------------------

    interaction_score = (
        view_count * 0.1
        + retweet_count * 2.0
        + reply_count * 1.5
        + quote_count * 1.2
        + favorite_count
    )

    favorite_ratio = (
        favorite_count
        / (view_count + 1.0)
    )

    reply_ratio = (
        reply_count
        / (retweet_count + 1.0)
    )

    is_viral = (
        interaction_score > 2000
    )

    # -----------------------------
    # User
    # -----------------------------

    followers_count = safe_int(
        relationship.get(
            "followers"
        )
    )

    friends_count = safe_int(
        relationship.get(
            "following"
        )
    )

    media_count = safe_int(
        tweet_counts.get(
            "media_tweets"
        )
    )

    is_blue_verified = bool(
        user.get(
            "is_blue_verified",
            False
        )
    )

    account_created_raw = (
        user_core.get(
            "created_at"
        )
    )

    account_dt = (
        parse_twitter_datetime(
            account_created_raw
        )
    )

    account_created_date = None
    account_age_days = 0

    if account_dt:

        account_created_date = (
            account_dt.date()
        )

        account_age_days = max(
            (
                tweet_dt_et.date()
                - account_created_date
            ).days,
            0
        )

    import math

    credibility_score = (
        (2.0 if is_blue_verified else 0.0)
        + math.log1p(
            followers_count
        )
        + (
            account_age_days
            / 365.0
        )
    )

    is_new_account = (
        account_age_days < 365
    )

    is_influencer = (
        followers_count > 130000
        or is_blue_verified
    )

    return {
        "tweet_id":
            str(tweet.get("rest_id", "")),

        "user_id":
            str(user.get("rest_id", "")),

        "tweet_created_at":
            created_at_raw,

        "tweet_timestamp_utc":
            tweet_dt_utc.isoformat(),

        "tweet_timestamp_et":
            tweet_dt_et.isoformat(),

        "tweet_date_et":
            tweet_dt_et.date().isoformat(),

        "full_text":
            full_text,

        "clean_text":
            cleaned,

        "view_count":
            view_count,

        "retweet_count":
            retweet_count,

        "reply_count":
            reply_count,

        "quote_count":
            quote_count,

        "favorite_count":
            favorite_count,

        "sentiment_score":
            sentiment_score,

        "interaction_score":
            interaction_score,

        "favorite_ratio":
            favorite_ratio,

        "reply_ratio":
            reply_ratio,

        "is_viral":
            is_viral,

        "is_blue_verified":
            is_blue_verified,

        "followers_count":
            followers_count,

        "friends_count":
            friends_count,

        "media_count":
            media_count,

        "account_created_date":
            (
                account_created_date.isoformat()
                if account_created_date
                else None
            ),

        "account_age_days":
            account_age_days,

        "credibility_score":
            credibility_score,

        "is_new_account":
            is_new_account,

        "is_influencer":
            is_influencer
    }


# ============================================================
# PREDICTION SESSION
# ============================================================

def current_prediction_session():

    now_et = datetime.now(ET)

    # Function is designed to run around 5 AM ET.
    # It prepares features for the upcoming 6 AM
    # prediction session.
    if (
        now_et.hour
        < PREDICTION_CUTOFF_HOUR_ET
    ):
        return now_et.date()

    # Manual execution after cutoff is treated as
    # preparing the next calendar session.
    return (
        now_et + timedelta(days=1)
    ).date()


# ============================================================
# PROCESS
# ============================================================

def run_ingestion():

    payload = fetch_nvda_tweets()

    tweets = extract_tweet_results(
        payload
    )

    session_date = (
        current_prediction_session()
    )

    rows = []

    seen_ids = set()

    for tweet in tweets:

        row = build_feature_row(
            tweet
        )

        if not row:
            continue

        tweet_id = row["tweet_id"]

        if (
            not tweet_id
            or tweet_id in seen_ids
        ):
            continue

        seen_ids.add(tweet_id)

        # Gold/Stream receives the intended
        # prediction session. Synapse will apply
        # authoritative NYSE trading-day mapping.
        row[
            "candidate_prediction_session_date"
        ] = session_date.isoformat()

        rows.append(row)

    now_et = datetime.now(ET)

    output = {
        "generated_at_utc":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "generated_at_et":
            now_et.isoformat(),

        "candidate_prediction_session_date":
            session_date.isoformat(),

        "tweet_count":
            len(rows),

        "tweets":
            rows
    }

    blob_name = (
        "stream/"
        f"{session_date:%Y/%m/%d}/"
        "tweets_gold.json"
    )

    blob_service = (
        get_blob_service_client()
    )

    blob_client = (
        blob_service
        .get_blob_client(
            container=GOLD_CONTAINER,
            blob=blob_name
        )
    )

    blob_client.upload_blob(
        json.dumps(
            output,
            ensure_ascii=False,
            indent=2
        ),
        overwrite=True
    )

    logging.info(
        "Wrote %s tweets to %s",
        len(rows),
        blob_name
    )

    return {
        "status": "success",
        "tweet_count": len(rows),
        "candidate_prediction_session_date":
            session_date.isoformat(),
        "blob":
            (
                f"{GOLD_CONTAINER}/"
                f"{blob_name}"
            )
    }


# ============================================================
# TIMER
# 5 AM ET ≈ 09:00 UTC during EDT,
# 10:00 UTC during EST.
#
# Azure timer CRON is UTC. DST makes a fixed UTC
# expression unsuitable for exact 5 AM ET year-round.
#
# Trigger hourly at :05 around the relevant window;
# ET guard decides whether to execute.
# ============================================================

@app.timer_trigger(
    schedule="0 5 9,10 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def daily_nvda_ingestion(
    timer: func.TimerRequest
):

    now_et = datetime.now(ET)

    if now_et.hour != 5:
        logging.info(
            "Skipping timer execution. "
            "Current ET time: %s",
            now_et.isoformat()
        )
        return

    logging.info(
        "Starting daily NVDA ingestion."
    )

    result = run_ingestion()

    logging.info(
        "Completed: %s",
        result
    )


# ============================================================
# HTTP TEST ENDPOINT
# ============================================================

@app.route(
    route="run-nvda-ingestion",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def manual_nvda_ingestion(
    req: func.HttpRequest
) -> func.HttpResponse:

    try:

        result = run_ingestion()

        return func.HttpResponse(
            json.dumps(
                result,
                indent=2
            ),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as exc:

        logging.exception(
            "NVDA ingestion failed."
        )

        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "error": str(exc)
            }),
            status_code=500,
            mimetype="application/json"
        )