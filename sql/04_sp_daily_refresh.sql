CREATE PROCEDURE dbo.sp_DailyRefresh
AS
BEGIN

    /* =========================================================
       1. CLEAR WAREHOUSE
       ========================================================= */

    TRUNCATE TABLE dbo.FactViralTweetLog;
    TRUNCATE TABLE dbo.FactDailyTweet;
    TRUNCATE TABLE dbo.FactMarketSummaryPerDay;
    TRUNCATE TABLE dbo.FactUserTypeDistribution;
    TRUNCATE TABLE dbo.FactTopUsersByTweetCount;
    TRUNCATE TABLE dbo.FactTopUsersByViralTweets;
    TRUNCATE TABLE dbo.FactInfluencerMonthlyEngagement;
    TRUNCATE TABLE dbo.FactPrediction;

    TRUNCATE TABLE dbo.DimUser;
    TRUNCATE TABLE dbo.DimDate;


    /* =========================================================
       2. DIM DATE
       Conformed date dimension
       ========================================================= */

    INSERT INTO dbo.DimDate
    (
        DateKey,
        FullDate,
        CalendarYear,
        CalendarMonth,
        MonthName,
        DayOfMonth,
        DayOfWeekNumber,
        DayName,
        YearMonth
    )
    SELECT DISTINCT
        YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d) AS DateKey,
        d AS FullDate,
        YEAR(d) AS CalendarYear,
        MONTH(d) AS CalendarMonth,
        DATENAME(MONTH, d) AS MonthName,
        DAY(d) AS DayOfMonth,
        DATEPART(WEEKDAY, d) AS DayOfWeekNumber,
        DATENAME(WEEKDAY, d) AS DayName,
        CONCAT(
            YEAR(d),
            '-',
            RIGHT('0' + CAST(MONTH(d) AS VARCHAR(2)), 2)
        ) AS YearMonth
    FROM
    (
        SELECT tweet_created_at_date AS d
        FROM dbo.ext_DailyTweetSummary

        UNION

        SELECT tweet_created_at_date
        FROM dbo.ext_MarketSummaryPerDay

        UNION

        SELECT tweet_created_at_date
        FROM dbo.ext_ViralTweetLog

        UNION

        SELECT tweet_created_at_date
        FROM dbo.ext_UserTypeDistributionPerDay

        UNION

        SELECT year_month
        FROM dbo.ext_InfluencerMonthlyEngagement
    ) dates
    WHERE d IS NOT NULL;


    /* =========================================================
       3. DIM USER
       Natural key: UserID
       Surrogate key: UserKey
       ========================================================= */

    INSERT INTO dbo.DimUser
    (
        UserKey,
        UserID,
        IsBlueVerified,
        AccountCreatedAt,
        AccountAgeDays,
        IsNewAccount,
        IsInfluencer,
        FollowersCount,
        FriendsCount,
        MediaCount,
        CredibilityScore
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY user_id) AS UserKey,
        user_id,
        is_blue_verified,
        account_created_at,
        account_age_days,
        is_new_account,
        is_influencer,
        followers_count,
        friends_count,
        media_count,
        credibility_score
    FROM dbo.ext_UserProfile;


    /* =========================================================
       4. FACT DAILY TWEET
       Grain: 1 row / date
       ========================================================= */

    INSERT INTO dbo.FactDailyTweet
    (
        DateKey,
        TotalTweets,
        AvgSentiment,
        AvgInteractionScore,
        AvgFavoriteRatio,
        AvgReplyRatio,
        TotalViralTweet,
        AvgCredibilityScore,
        AvgFollowers
    )
    SELECT
        YEAR(tweet_created_at_date) * 10000
            + MONTH(tweet_created_at_date) * 100
            + DAY(tweet_created_at_date),

        total_tweets,
        avg_sentiment,
        avg_interaction_score,
        avg_favorite_ratio,
        avg_reply_ratio,
        total_viral_tweet,
        avg_credibility_score,
        avg_followers

    FROM dbo.ext_DailyTweetSummary
    WHERE tweet_created_at_date IS NOT NULL;


    /* =========================================================
       5. FACT MARKET SUMMARY
       Grain: 1 row / market date
       ========================================================= */

    INSERT INTO dbo.FactMarketSummaryPerDay
    (
        DateKey,
        CurrentOpen,
        CurrentClose,
        CurrentHigh,
        CurrentLow,
        CurrentVolume,
        ActualNextVolume,
        AvgSentiment,
        TweetCount,
        PredictedNextVolume,
        PredictionAccuracy
    )
    SELECT
        YEAR(tweet_created_at_date) * 10000
            + MONTH(tweet_created_at_date) * 100
            + DAY(tweet_created_at_date),

        current_open,
        current_close,
        current_high,
        current_low,
        current_volume,
        actual_volume,
        avg_sentiment,
        tweet_count,
        predicted_volume,
        prediction_accuracy

    FROM dbo.ext_MarketSummaryPerDay
    WHERE tweet_created_at_date IS NOT NULL;


    /* =========================================================
       6. FACT VIRAL TWEET LOG
       Resolve UserID -> UserKey
       ========================================================= */

    INSERT INTO dbo.FactViralTweetLog
    (
        TweetID,
        DateKey,
        UserKey,
        FullText,
        SentimentScore,
        InteractionScore,
        FollowersCount,
        FavoriteCount,
        RetweetCount,
        ReplyCount,
        ViewCount,
        CredibilityScore
    )
    SELECT
        v.tweet_id,

        YEAR(v.tweet_created_at_date) * 10000
            + MONTH(v.tweet_created_at_date) * 100
            + DAY(v.tweet_created_at_date),

        u.UserKey,
        v.full_text,
        v.sentiment_score,
        v.interaction_score,
        v.followers_count,
        v.favorite_count,
        v.retweet_count,
        v.reply_count,
        v.view_count,
        v.credibility_score

    FROM dbo.ext_ViralTweetLog v

    INNER JOIN dbo.DimUser u
        ON v.user_id = u.UserID

    WHERE v.tweet_created_at_date IS NOT NULL;


    /* =========================================================
       7. FACT USER TYPE DISTRIBUTION
       Grain: 1 row / date
       ========================================================= */

    INSERT INTO dbo.FactUserTypeDistribution
    (
        DateKey,
        InfluencerTweetCount,
        NewAccountTweetCount,
        BlueVerifiedTweetCount,
        TotalTweets,
        DistinctUsers,
        AvgAccountAge,
        GeneralPublicTweetCount,
        EstablishedAccountTweetCount,
        UnverifiedTweetCount
    )
    SELECT
        YEAR(tweet_created_at_date) * 10000
            + MONTH(tweet_created_at_date) * 100
            + DAY(tweet_created_at_date),

        influencer_tweet_count,
        new_account_tweet_count,
        blue_verified_tweet_count,
        total_tweets,
        distinct_users,
        avg_account_age,
        general_public_tweet_count,
        established_account_tweet_count,
        unverified_tweet_count

    FROM dbo.ext_UserTypeDistributionPerDay
    WHERE tweet_created_at_date IS NOT NULL;


    /* =========================================================
       8. TOP USERS BY TWEET COUNT
       ========================================================= */

    INSERT INTO dbo.FactTopUsersByTweetCount
    (
        UserKey,
        TweetCount,
        FollowersCount,
        CredibilityScore,
        IsInfluencer
    )
    SELECT
        u.UserKey,
        e.tweet_count,
        e.followers_count,
        e.credibility_score,
        e.is_influencer

    FROM dbo.ext_TopUsersByTweetCount e

    INNER JOIN dbo.DimUser u
        ON e.user_id = u.UserID;


    /* =========================================================
       9. TOP USERS BY VIRAL TWEETS
       ========================================================= */

    INSERT INTO dbo.FactTopUsersByViralTweets
    (
        UserKey,
        ViralTweetCount,
        AvgInteractionScore,
        FollowersCount,
        CredibilityScore
    )
    SELECT
        u.UserKey,
        e.viral_tweet_count,
        e.avg_interaction_score,
        e.followers_count,
        e.credibility_score

    FROM dbo.ext_TopUsersByViralTweets e

    INNER JOIN dbo.DimUser u
        ON e.user_id = u.UserID;


    /* =========================================================
       10. INFLUENCER MONTHLY ENGAGEMENT
       Grain: 1 row / month
       ========================================================= */

    INSERT INTO dbo.FactInfluencerMonthlyEngagement
    (
        MonthDateKey,
        CalendarYear,
        CalendarMonth,
        InfluencerTweetCount,
        AvgSentimentScore,
        AvgInteraction,
        DistinctInfluencers,
        TotalFavorites,
        TotalRetweets,
        TotalReplies
    )
    SELECT
        YEAR(year_month) * 10000
            + MONTH(year_month) * 100
            + 1,

        year,
        month,
        influencer_tweet_count,
        avg_sentiment_score,
        avg_interaction,
        distinct_influencers,
        total_favorites,
        total_retweets,
        total_replies

    FROM dbo.ext_InfluencerMonthlyEngagement
    WHERE year_month IS NOT NULL;

    /* =========================================================
   11. FACT PREDICTION
   Grain: one row per prediction session / target
   ========================================================= */

    INSERT INTO dbo.FactPrediction
    (
        PredictionDateKey,
        TargetDateKey,
        PredictedVolume,
        ActualVolume,
        PredictionAccuracy,
        EvaluationStatus,
        ModelVersion,
        PredictionGeneratedAtUTC,
        TweetCount,
        AvgSentimentScore,
        AvgInteractionScore
    )
    SELECT
        (
            YEAR(prediction_date) * 10000
            + MONTH(prediction_date) * 100
            + DAY(prediction_date)
        ) AS PredictionDateKey,

        CASE
            WHEN target_date IS NOT NULL
            THEN (
                YEAR(target_date) * 10000
                + MONTH(target_date) * 100
                + DAY(target_date)
            )
            ELSE NULL
        END AS TargetDateKey,

        predicted_volume,
        actual_volume,
        prediction_accuracy,
        evaluation_status,
        model_version,
        prediction_generated_at_utc,
        tweet_count,
        avg_sentiment_score,
        avg_interaction_score

    FROM dbo.ext_PredictionHistory
    WHERE prediction_date IS NOT NULL
    ;

END;
GO

SELECT
    s.name AS schema_name,
    t.name AS external_table
FROM sys.external_tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
ORDER BY t.name;

EXEC dbo.sp_DailyRefresh;

SELECT COUNT(*) AS DimDate_Count
FROM dbo.DimDate;
GO

SELECT COUNT(*) AS DimUser_Count
FROM dbo.DimUser;
GO

SELECT COUNT(*) AS FactDailyTweet_Count
FROM dbo.FactDailyTweet;
GO

SELECT COUNT(*) AS FactMarketSummaryPerDay_Count
FROM dbo.FactMarketSummaryPerDay;
GO

SELECT COUNT(*) AS FactViralTweetLog_Count
FROM dbo.FactViralTweetLog;
GO

SELECT COUNT(*) AS FactUserTypeDistribution_Count
FROM dbo.FactUserTypeDistribution;
GO

SELECT COUNT(*) AS FactTopUsersByTweetCount_Count
FROM dbo.FactTopUsersByTweetCount;
GO

SELECT COUNT(*) AS FactTopUsersByViralTweets_Count
FROM dbo.FactTopUsersByViralTweets;
GO

SELECT COUNT(*) AS FactInfluencerMonthlyEngagement_Count
FROM dbo.FactInfluencerMonthlyEngagement;
GO

SELECT *
FROM dbo.FactPrediction;
GO

