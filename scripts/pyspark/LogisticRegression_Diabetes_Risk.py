# -*- coding: utf-8 -*-
# =============================================
# Logistic Regression: Diabetes Risk Prediction
# Project: DS8003 - Health Risk Analysis
# Author: Shahida Batool / Group 5
# =============================================
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import FloatType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# -------------------------------
# 1 Create Spark session with Hive support
# -------------------------------
spark = SparkSession.builder \
    .appName("DiabetesRisk_LogReg") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------------------
# 2 Load Hive table
# -------------------------------
spark.sql("USE health_risk_db")
df = spark.sql("SELECT * FROM smoking_drinking_features_clean")

# -------------------------------
# 3 Create diabetes target
# -------------------------------
df = df.withColumn(
    "diabetes_risk",
    when(col("blds") >= 126, 1).otherwise(0)
)

# -------------------------------
# 4 Feature selection: 14 medically relevant features
# -------------------------------
feature_cols = [
    'age',             # Risk increases with age
    'sex',             # Gender differences in diabetes risk
    'bmi',             # Obesity is a major risk factor
    'waistline',       # Central obesity indicator
    'sbp',             # High systolic BP linked to diabetes
    'dbp',             # High diastolic BP linked to diabetes
    'blds',            # Fasting blood glucose (main diabetes measure)
    'hdl_chole',       # Low HDL indicates metabolic risk
    'triglyceride',    # High TG linked to insulin resistance
    'hemoglobin',      # Reflects general health
    'urine_protein',   # Early kidney damage marker
    'gamma_gtp',       # Liver function, associated with insulin resistance
    'smk_stat_type_cd',# Smoking affects insulin resistance
    'drk_yn'           # Alcohol affects glucose metabolism
]

# -------------------------------
# 5 Encode categorical features
# -------------------------------
# Smoking: One-hot encode
smk_indexer = StringIndexer(inputCol="smk_stat_type_cd", outputCol="smk_stat_type_cd_index")
smk_encoder = OneHotEncoder(inputCol="smk_stat_type_cd_index", outputCol="smk_stat_type_cd_vec")
feature_cols.remove("smk_stat_type_cd")
feature_cols.append("smk_stat_type_cd_vec")

# Alcohol: convert Y/N to numeric
df = df.withColumn("drk_yn_num", when(col("drk_yn") == 'Y', 1).otherwise(0))
feature_cols.remove("drk_yn")
feature_cols.append("drk_yn_num")

# Sex: convert F/M to numeric
df = df.withColumn("sex_num", when(col("sex") == 'Female', 0).otherwise(1))
feature_cols.remove("sex")
feature_cols.append("sex_num")

# -------------------------------
# 6 Assemble features vector
# -------------------------------
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
label_indexer = StringIndexer(inputCol="diabetes_risk", outputCol="label")

# -------------------------------
# 7 Split dataset into train/test (80/20)
# -------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# -------------------------------
# 8 Logistic Regression Model
# -------------------------------
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[smk_indexer, smk_encoder, assembler, label_indexer, lr])

# -------------------------------
# 9 Train the model
# -------------------------------
model = pipeline.fit(train_df)

# -------------------------------
# 10 Make predictions on test data
# -------------------------------
predictions = model.transform(test_df)

# -------------------------------
# 11 Map numeric labels to readable labels
# -------------------------------
predictions = predictions.withColumn(
    "label_text", when(col("label") == 0.0, "Low Diabetes Risk").otherwise("High Diabetes Risk")
)
predictions = predictions.withColumn(
    "prediction_text", when(col("prediction") == 0.0, "Low Risk").otherwise("High  Risk")
)

# -------------------------------
# 12 Extract probability of High Risk (class 1)
# -------------------------------
prob_udf = udf(lambda v: float(v[1]), FloatType())
predictions = predictions.withColumn("probability_high_risk", prob_udf("probability"))

# Show sample of predictions
print("Sample predictions with probability:")
predictions.select("label_text", "prediction_text", "probability_high_risk").show(10)

# -------------------------------
# 13 Evaluate model accuracy
# -------------------------------
accuracy_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = accuracy_eval.evaluate(predictions)
print("Model Accuracy =", accuracy)

if accuracy >= 0.85:
    print("Model Status: Excellent accuracy — the model performs very well.")
elif accuracy >= 0.70:
    print("Model Status: Accuracy is acceptable — the model is reasonably good.")
else:
    print("Model Status: Low accuracy — model needs improvement.")

# -------------------------------
# 14 Confusion matrix with percentages
# -------------------------------
predictions = predictions.withColumn(
    "label_text_full", when(col("label") == 1.0, "High Diabetes Risk").otherwise("Low Diabetes Risk")
)
predictions = predictions.withColumn(
    "prediction_text_full", when(col("prediction") == 1.0, "High Risk").otherwise("Low Risk")
)
total_count = predictions.count()
confusion_df = predictions.groupBy("label_text_full", "prediction_text_full") \
    .count() \
    .withColumn("percentage", (col("count") / total_count * 100))

print("Confusion matrix with percentages:")
confusion_df.show(truncate=False)

# -------------------------------
# 15 Save predictions for Power BI
# -------------------------------
base_path = "file:///root/data/health_risk/Logistic_Regression_Diabetes"

# 15a. Full predictions
predictions.select(
    "label_text",
    "prediction_text",
    "probability_high_risk",
    "age", "bmi", "waistline", "blds",
    "hdl_chole", "triglyceride",
    "sbp", "dbp", "hemoglobin",
     "urine_protein",
    "gamma_gtp",
    "drk_yn",          # alcohol
    "smk_stat_type_cd",# original smoking code
    "sex_num"          # sex numeric
).coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/predictions_full")
print("Predictions CSV saved for Power BI.")
# 15b. Summary of predicted risks
risk_counts = predictions.groupBy("prediction_text").count()
total = predictions.count()
risk_percent = risk_counts.withColumn("percentage", (col("count") / total * 100))
risk_percent.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/risk_summary")
print("Predicted risks CSV saved.")
# 15c. Misclassified cases
misclassified_df = predictions.filter(col("label") != col("prediction"))
misclassified_count = misclassified_df.count()
if misclassified_count > 0:
    misclassified_df.select(
        "label_text",
        "prediction_text",
        "probability_high_risk",
        *feature_cols
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/misclassified_cases")

    print("Misclassified cases CSV saved for Power BI.")
else:
    print("No misclassified cases — model accuracy is 100%, nothing to save.")
# 15d. Logistic Regression Feature Importance 
lr_model_fitted = model.stages[-1]
if isinstance(lr_model_fitted, LogisticRegressionModel):
    coef_array = lr_model_fitted.coefficients.toArray()
    importance_rows = [
        (feature_cols[i], abs(float(coef_array[i]))) 
        for i in range(len(feature_cols))
    ]
    importance_df = spark.createDataFrame(
        importance_rows,
        ["feature", "importance_score"]
    ).orderBy(col("importance_score").desc())
    importance_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/feature_importance")
    print("Feature importance CSV saved for Power BI.")
else:
    print("Error: LogisticRegressionModel not found in pipeline stages — cannot extract coefficients.")
# -------------------------------
# 16 Stop Spark session
# -------------------------------
spark.stop()
