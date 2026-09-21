-- Read-only checks after publishing notebooks and running PL_BackfillRetrain.
-- Both results should contain one September 16 prediction targeting September 17,
-- ActualVolume = 94191300, EvaluationStatus = Evaluated (reported source value).
SELECT prediction_date, target_date, predicted_volume, actual_volume,
       prediction_accuracy, evaluation_status,
       1.0 - ABS(actual_volume - predicted_volume) / NULLIF(actual_volume, 0)
           AS expected_accuracy
FROM dbo.ext_PredictionHistory
WHERE prediction_date = '2026-09-16';

SELECT PredictionDateKey, TargetDateKey, PredictedVolume, ActualVolume,
       PredictionAccuracy, EvaluationStatus,
       1.0 - ABS(ActualVolume - PredictedVolume) / NULLIF(ActualVolume, 0)
           AS ExpectedAccuracy
FROM dbo.FactPrediction
WHERE PredictionDateKey = 20260916;
