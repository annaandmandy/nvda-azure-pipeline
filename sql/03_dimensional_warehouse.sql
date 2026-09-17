/* ============================================================
   NVDA SENTIMENT DATA WAREHOUSE
   Dedicated SQL Pool: sqlNVDA
   ============================================================ */


/* ============================================================
   1. DIM DATE
   Grain: one row per calendar date
   ============================================================ */

IF OBJECT_ID('dbo.DimDate', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.DimDate
    (
        DateKey         INT          NOT NULL,
        FullDate        DATE         NOT NULL,
        CalendarYear    INT          NOT NULL,
        CalendarMonth   INT          NOT NULL,
        MonthName       VARCHAR(20)  NOT NULL,
        DayOfMonth      INT          NOT NULL,
        DayOfWeekNumber INT          NOT NULL,
        DayName         VARCHAR(20)  NOT NULL,
        YearMonth       VARCHAR(7)   NOT NULL
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = REPLICATE
    );
END;
GO


/* ============================================================
   2. DIM USER
   Grain: one row per X/Twitter user
   ============================================================ */

IF OBJECT_ID('dbo.DimUser', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.DimUser
    (
        UserKey             BIGINT       NOT NULL,
        UserID              VARCHAR(100) NOT NULL,
        IsBlueVerified      BIT,
        AccountCreatedAt    DATE,
        AccountAgeDays      INT,
        IsNewAccount        BIT,
        IsInfluencer        BIT,
        FollowersCount      BIGINT,
        FriendsCount        BIGINT,
        MediaCount          BIGINT,
        CredibilityScore    FLOAT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = REPLICATE
    );
END;
GO


/* ============================================================
   3. FACT DAILY TWEET
   Grain: one row per day
   ============================================================ */

IF OBJECT_ID('dbo.FactDailyTweet', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactDailyTweet
    (
        DateKey                 INT NOT NULL,
        TotalTweets             BIGINT,
        AvgSentiment            FLOAT,
        AvgInteractionScore     FLOAT,
        AvgFavoriteRatio        FLOAT,
        AvgReplyRatio           FLOAT,
        TotalViralTweet         BIGINT,
        AvgCredibilityScore     FLOAT,
        AvgFollowers            FLOAT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   4. FACT MARKET SUMMARY PER DAY
   Grain: one row per market date
   ============================================================ */

IF OBJECT_ID('dbo.FactMarketSummaryPerDay', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactMarketSummaryPerDay
    (
        DateKey                 INT NOT NULL,
        CurrentOpen             FLOAT,
        CurrentClose            FLOAT,
        CurrentHigh             FLOAT,
        CurrentLow              FLOAT,
        CurrentVolume           FLOAT,
        ActualNextVolume        FLOAT,
        AvgSentiment            FLOAT,
        TweetCount              BIGINT,
        PredictedNextVolume     FLOAT,
        PredictionAccuracy      FLOAT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   5. FACT VIRAL TWEET LOG
   Grain: one row per viral tweet
   ============================================================ */

IF OBJECT_ID('dbo.FactViralTweetLog', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactViralTweetLog
    (
        TweetID             VARCHAR(100) NOT NULL,
        DateKey             INT          NOT NULL,
        UserKey             BIGINT       NOT NULL,
        FullText            VARCHAR(4000),
        SentimentScore      FLOAT,
        InteractionScore    FLOAT,
        FollowersCount      BIGINT,
        FavoriteCount       BIGINT,
        RetweetCount        BIGINT,
        ReplyCount          BIGINT,
        ViewCount           BIGINT,
        CredibilityScore    FLOAT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   6. FACT USER TYPE DISTRIBUTION
   Grain: one row per day
   ============================================================ */

IF OBJECT_ID('dbo.FactUserTypeDistribution', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactUserTypeDistribution
    (
        DateKey                       INT NOT NULL,
        InfluencerTweetCount          BIGINT,
        NewAccountTweetCount          BIGINT,
        BlueVerifiedTweetCount        BIGINT,
        TotalTweets                   BIGINT,
        DistinctUsers                 BIGINT,
        AvgAccountAge                 FLOAT,
        GeneralPublicTweetCount       BIGINT,
        EstablishedAccountTweetCount  BIGINT,
        UnverifiedTweetCount          BIGINT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   7. FACT TOP USERS BY TWEET COUNT
   Grain: one row per user
   ============================================================ */

IF OBJECT_ID('dbo.FactTopUsersByTweetCount', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactTopUsersByTweetCount
    (
        UserKey              BIGINT NOT NULL,
        TweetCount           BIGINT,
        FollowersCount       BIGINT,
        CredibilityScore     FLOAT,
        IsInfluencer         BIT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   8. FACT TOP USERS BY VIRAL TWEETS
   Grain: one row per user
   ============================================================ */

IF OBJECT_ID('dbo.FactTopUsersByViralTweets', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactTopUsersByViralTweets
    (
        UserKey              BIGINT NOT NULL,
        ViralTweetCount      BIGINT,
        AvgInteractionScore  FLOAT,
        FollowersCount       BIGINT,
        CredibilityScore     FLOAT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


/* ============================================================
   9. FACT INFLUENCER MONTHLY ENGAGEMENT
   Grain: one row per month
   ============================================================ */

IF OBJECT_ID('dbo.FactInfluencerMonthlyEngagement', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactInfluencerMonthlyEngagement
    (
        MonthDateKey          INT NOT NULL,
        CalendarYear          INT,
        CalendarMonth         INT,
        InfluencerTweetCount  BIGINT,
        AvgSentimentScore     FLOAT,
        AvgInteraction        FLOAT,
        DistinctInfluencers   BIGINT,
        TotalFavorites        BIGINT,
        TotalRetweets         BIGINT,
        TotalReplies          BIGINT
    )
    WITH
    (
        HEAP,
        DISTRIBUTION = ROUND_ROBIN
    );
END;
GO


PRINT 'Dimensional warehouse tables created successfully.';
GO
