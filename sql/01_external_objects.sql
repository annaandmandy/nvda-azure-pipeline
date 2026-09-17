SELECT DB_NAME() AS current_database;

-- ============================================================
-- NVDA Power BI Serving Layer
-- Step 1: External storage objects
-- ============================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_scoped_credentials
    WHERE name = 'NVDAWorkspaceIdentity'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL NVDAWorkspaceIdentity
    WITH IDENTITY = 'Managed Identity';
END;
GO


IF NOT EXISTS (
    SELECT 1
    FROM sys.external_data_sources
    WHERE name = 'NVDA_Gold'
)
BEGIN
    CREATE EXTERNAL DATA SOURCE NVDA_Gold
    WITH (
        LOCATION =
        'abfss://gold@<storage-account-name>.dfs.core.windows.net',
        CREDENTIAL = NVDAWorkspaceIdentity
    );
END;
GO


IF NOT EXISTS (
    SELECT 1
    FROM sys.external_file_formats
    WHERE name = 'ParquetFormat'
)
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
        FORMAT_TYPE = PARQUET
    );
END;
GO

IF OBJECT_ID('dbo.TestDailyTweetSummary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.TestDailyTweetSummary;
GO

CREATE EXTERNAL TABLE dbo.TestDailyTweetSummary
(
    tweet_created_at_date DATE,
    total_tweets BIGINT,
    avg_sentiment FLOAT,
    avg_interaction_score FLOAT,
    avg_favorite_ratio FLOAT,
    avg_reply_ratio FLOAT,
    total_viral_tweet BIGINT,
    avg_credibility_score FLOAT,
    avg_followers FLOAT
)
WITH
(
    LOCATION = 'aggregates/daily_tweet_summary/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO

SELECT TOP 5 *
FROM dbo.TestDailyTweetSummary;
GO


/* ============================================================
   NVDA SENTIMENT PROJECT
   External Tables for all 8 Gold Aggregate datasets
   Dedicated SQL Pool: sqlNVDA
   ============================================================ */


/* ------------------------------------------------------------
   Cleanup test table
   ------------------------------------------------------------ */

IF OBJECT_ID('dbo.TestDailyTweetSummary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.TestDailyTweetSummary;
GO


/* ============================================================
   1. USER PROFILE
   ============================================================ */

IF OBJECT_ID('dbo.ext_UserProfile', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_UserProfile;
GO

CREATE EXTERNAL TABLE dbo.ext_UserProfile
(
    user_id                 VARCHAR(100),
    is_blue_verified        BIT,
    account_created_at      DATE,
    account_age_days        INT,
    is_new_account          BIT,
    is_influencer           BIT,
    followers_count         BIGINT,
    friends_count           BIGINT,
    media_count             BIGINT,
    credibility_score       FLOAT
)
WITH
(
    LOCATION = 'aggregates/user_profile/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   2. DAILY TWEET SUMMARY
   ============================================================ */

IF OBJECT_ID('dbo.ext_DailyTweetSummary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_DailyTweetSummary;
GO

CREATE EXTERNAL TABLE dbo.ext_DailyTweetSummary
(
    tweet_created_at_date   DATE,
    total_tweets            BIGINT,
    avg_sentiment           FLOAT,
    avg_interaction_score   FLOAT,
    avg_favorite_ratio      FLOAT,
    avg_reply_ratio         FLOAT,
    total_viral_tweet       BIGINT,
    avg_credibility_score   FLOAT,
    avg_followers           FLOAT
)
WITH
(
    LOCATION = 'aggregates/daily_tweet_summary/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   3. MARKET SUMMARY PER DAY
   ============================================================ */

IF OBJECT_ID('dbo.ext_MarketSummaryPerDay', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_MarketSummaryPerDay;
GO

CREATE EXTERNAL TABLE dbo.ext_MarketSummaryPerDay
(
    tweet_created_at_date   DATE,
    current_open            FLOAT,
    current_close           FLOAT,
    current_high            FLOAT,
    current_low             FLOAT,
    current_volume          FLOAT,
    actual_volume           FLOAT,
    avg_sentiment           FLOAT,
    tweet_count             BIGINT,
    predicted_volume        FLOAT,
    prediction_accuracy     FLOAT
)
WITH
(
    LOCATION = 'aggregates/market_summary_per_day/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   4. VIRAL TWEET LOG
   ============================================================ */

IF OBJECT_ID('dbo.ext_ViralTweetLog', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_ViralTweetLog;
GO

CREATE EXTERNAL TABLE dbo.ext_ViralTweetLog
(
    tweet_id                VARCHAR(100),
    user_id                 VARCHAR(100),
    tweet_created_at_date   DATE,
    full_text               VARCHAR(4000),
    sentiment_score         FLOAT,
    interaction_score       FLOAT,
    followers_count         BIGINT,
    favorite_count          BIGINT,
    retweet_count           BIGINT,
    reply_count             BIGINT,
    view_count              BIGINT,
    credibility_score       FLOAT
)
WITH
(
    LOCATION = 'aggregates/viral_tweet_log/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   5. USER TYPE DISTRIBUTION PER DAY
   ============================================================ */

IF OBJECT_ID('dbo.ext_UserTypeDistributionPerDay', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_UserTypeDistributionPerDay;
GO

CREATE EXTERNAL TABLE dbo.ext_UserTypeDistributionPerDay
(
    tweet_created_at_date            DATE,
    influencer_tweet_count           BIGINT,
    new_account_tweet_count          BIGINT,
    blue_verified_tweet_count        BIGINT,
    total_tweets                     BIGINT,
    distinct_users                   BIGINT,
    avg_account_age                  FLOAT,
    general_public_tweet_count       BIGINT,
    established_account_tweet_count  BIGINT,
    unverified_tweet_count           BIGINT
)
WITH
(
    LOCATION = 'aggregates/user_type_distribution_per_day/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   6. TOP USERS BY TWEET COUNT
   ============================================================ */

IF OBJECT_ID('dbo.ext_TopUsersByTweetCount', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_TopUsersByTweetCount;
GO

CREATE EXTERNAL TABLE dbo.ext_TopUsersByTweetCount
(
    user_id                 VARCHAR(100),
    tweet_count             BIGINT,
    followers_count         BIGINT,
    credibility_score       FLOAT,
    is_influencer           BIT
)
WITH
(
    LOCATION = 'aggregates/top_users_by_tweet_count/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   7. TOP USERS BY VIRAL TWEETS
   ============================================================ */

IF OBJECT_ID('dbo.ext_TopUsersByViralTweets', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_TopUsersByViralTweets;
GO

CREATE EXTERNAL TABLE dbo.ext_TopUsersByViralTweets
(
    user_id                 VARCHAR(100),
    viral_tweet_count       BIGINT,
    avg_interaction_score   FLOAT,
    followers_count         BIGINT,
    credibility_score       FLOAT
)
WITH
(
    LOCATION = 'aggregates/top_users_by_viral_tweets/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   8. INFLUENCER MONTHLY ENGAGEMENT
   ============================================================ */

IF OBJECT_ID('dbo.ext_InfluencerMonthlyEngagement', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.ext_InfluencerMonthlyEngagement;
GO

CREATE EXTERNAL TABLE dbo.ext_InfluencerMonthlyEngagement
(
    year                    INT,
    month                   INT,
    influencer_tweet_count  BIGINT,
    avg_sentiment_score     FLOAT,
    avg_interaction         FLOAT,
    distinct_influencers    BIGINT,
    total_favorites         BIGINT,
    total_retweets          BIGINT,
    total_replies           BIGINT,
    year_month              DATE
)
WITH
(
    LOCATION = 'aggregates/influencer_monthly_engagement/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO


/* ============================================================
   Validation
   ============================================================ */

SELECT 'UserProfile' AS table_name, COUNT(*) AS row_count
FROM dbo.ext_UserProfile

UNION ALL

SELECT 'DailyTweetSummary', COUNT(*)
FROM dbo.ext_DailyTweetSummary

UNION ALL

SELECT 'MarketSummaryPerDay', COUNT(*)
FROM dbo.ext_MarketSummaryPerDay

UNION ALL

SELECT 'ViralTweetLog', COUNT(*)
FROM dbo.ext_ViralTweetLog

UNION ALL

SELECT 'UserTypeDistributionPerDay', COUNT(*)
FROM dbo.ext_UserTypeDistributionPerDay

UNION ALL

SELECT 'TopUsersByTweetCount', COUNT(*)
FROM dbo.ext_TopUsersByTweetCount

UNION ALL

SELECT 'TopUsersByViralTweets', COUNT(*)
FROM dbo.ext_TopUsersByViralTweets

UNION ALL

SELECT 'InfluencerMonthlyEngagement', COUNT(*)
FROM dbo.ext_InfluencerMonthlyEngagement;
GO


