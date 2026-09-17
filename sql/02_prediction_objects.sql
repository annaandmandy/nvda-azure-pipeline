IF EXISTS (
    SELECT 1
    FROM sys.external_tables
    WHERE name = 'ext_PredictionHistory'
)
DROP EXTERNAL TABLE dbo.ext_PredictionHistory;
GO

CREATE EXTERNAL TABLE dbo.ext_PredictionHistory
(
    prediction_date              DATE,
    target_date                  DATE,
    predicted_volume             FLOAT,
    actual_volume                FLOAT,
    prediction_accuracy          FLOAT,
    evaluation_status            VARCHAR(20),
    model_version                VARCHAR(100),
    prediction_generated_at_utc  DATETIME2,
    tweet_count                  BIGINT,
    avg_sentiment_score          FLOAT,
    avg_interaction_score        FLOAT
)
WITH
(
    LOCATION = 'aggregates/prediction_history/',
    DATA_SOURCE = NVDA_Gold,
    FILE_FORMAT = ParquetFormat
);
GO

SELECT *
FROM dbo.ext_PredictionHistory;
GO


CREATE TABLE dbo.FactPrediction
(
    PredictionDateKey          INT          NOT NULL,
    TargetDateKey              INT,
    PredictedVolume            FLOAT,
    ActualVolume               FLOAT,
    PredictionAccuracy         FLOAT,
    EvaluationStatus           VARCHAR(20),
    ModelVersion               VARCHAR(100),
    PredictionGeneratedAtUTC   DATETIME2,
    TweetCount                 BIGINT,
    AvgSentimentScore          FLOAT,
    AvgInteractionScore        FLOAT
)
WITH
(
    HEAP,
    DISTRIBUTION = ROUND_ROBIN
);
GO

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
    YEAR(prediction_date) * 10000
        + MONTH(prediction_date) * 100
        + DAY(prediction_date),

    CASE
        WHEN target_date IS NOT NULL
        THEN YEAR(target_date) * 10000
           + MONTH(target_date) * 100
           + DAY(target_date)
    END,

    predicted_volume,
    actual_volume,
    prediction_accuracy,
    evaluation_status,
    model_version,
    prediction_generated_at_utc,
    tweet_count,
    avg_sentiment_score,
    avg_interaction_score

FROM dbo.ext_PredictionHistory;
GO

SELECT *
FROM dbo.FactPrediction;
GO
