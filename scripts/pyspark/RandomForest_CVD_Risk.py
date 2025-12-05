# -*- coding: utf-8 -*-
# =============================================
# Random Forest: Cardiovascular Risk Prediction
# Project: DS8003 - Health Risk Analysis
# Author: Shahida Batool / Group 5
# =============================================
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import FloatType
import math
# -------------------------------
# 1 Create Spark session with Hive support
# -------------------------------
spark = SparkSession.builder \
    .appName("HeartRisk_RF_PySpark") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------------------
# 2 Load Hive table
# -------------------------------
spark.sql("USE health_risk_db")
df = spark.sql("SELECT * FROM smoking_drinking_features_clean")

# -------------------------------
# 3 Create heart disease risk target
# -------------------------------
df = df.withColumn(
    "heart_disease_risk",
    when(
        (col("bp_high_flag") == 1) | 
        (col("blds_high_flag") == 1) | 
        (col("liver_risk_flag") == 1), 1
    ).otherwise(0)
)

# -------------------------------
# 4 Feature selection with medical justification
# -------------------------------
feature_cols = [
    'gamma_gtp',        # Liver function
    'sgot_alt',         # Liver enzyme ALT
    'sbp',              # Systolic BP
    'dbp',              # Diastolic BP
    'blds',             # Blood sugar (diabetes/CVD risk)
    'sgot_ast',         # Liver enzyme AST
    'waistline',        # Central obesity
    'triglyceride',     # Lipid profile, metabolic risk
    'age',              # Demographics, CVD/diabetes risk
    'sex',              # Biological sex differences
    'drk_yn',       # Alcohol consumption
    'smk_stat_type_cd', # Smoking status
    'bmi'               # BMI, obesity indicator
]

# -------------------------------
# 5 Encode categorical columns
# -------------------------------
# 'sex' to numeric
sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_num")
df = sex_indexer.fit(df).transform(df)
feature_cols[feature_cols.index('sex')] = 'sex_num'

# 'smk_stat_type_cd' to numeric index
smk_indexer = StringIndexer(inputCol="smk_stat_type_cd", outputCol="smk_stat_type_cd_index")
df = smk_indexer.fit(df).transform(df)
feature_cols[feature_cols.index('smk_stat_type_cd')] = 'smk_stat_type_cd_index'

# 'drk_yn' to numeric
df = df.withColumn("drk_yn_num", when(col("drk_yn") == 'Y', 1).otherwise(0))
feature_cols[feature_cols.index('drk_yn')] = 'drk_yn_num'

# -------------------------------
# 6 Assemble features 
# -------------------------------
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
label_indexer = StringIndexer(inputCol="heart_disease_risk", outputCol="label")

# -------------------------------
# 7 Split dataset
# -------------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# -------------------------------
# 8 Random Forest Model
# -------------------------------
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=5)
pipeline = Pipeline(stages=[assembler, label_indexer, rf])

# -------------------------------
# 9 Train the model
# -------------------------------
model = pipeline.fit(train_df)

# -------------------------------
# 10 Make predictions
# -------------------------------
predictions = model.transform(test_df)

# -------------------------------
# 11 Map numeric labels to readable labels
# -------------------------------
predictions = predictions.withColumn(
    "Label", when(col("label")==0.0,"Low CVD Risk").otherwise("High CVD Risk")
)
predictions = predictions.withColumn(
    "Prediction", when(col("prediction")==0.0,"Low Risk").otherwise("High Risk")
)

# -------------------------------
# 12 Extract probability of High Risk
# -------------------------------
prob_udf = udf(lambda v: float(v[1]), FloatType())
predictions = predictions.withColumn("probability_high_risk", prob_udf("probability"))

print("Sample predictions with probability:")
predictions.select("Label", "Prediction", "probability_high_risk").show(10)
# -------------------------------
# 13 Calculate RMSE
# -------------------------------
pred_rdd = predictions.select("label","prediction").rdd.map(lambda row: (row.label,row.prediction))
mse = pred_rdd.map(lambda lp: (lp[0]-lp[1])**2).mean()
rmse = math.sqrt(mse)
print("Root Mean Squared Error (RMSE) = {:.4f}".format(rmse))

# -------------------------------
# 14 Interpret RMSE
# -------------------------------
accuracy_estimate = 1 - rmse
if accuracy_estimate >= 0.85:
    print("Model Status: Excellent accuracy — the model performs very well.")
elif accuracy_estimate >= 0.70:
    print("Model Status: Accuracy is acceptable — the model is reasonably good.")
else:
    print("Model Status: Low accuracy — model needs improvement.")

# -------------------------------
# 15 Confusion matrix with percentages
# -------------------------------
total_count = predictions.count()
confusion_df = predictions.groupBy("Label", "Prediction") \
    .count() \
    .withColumn("percentage", (col("count") / total_count * 100))
print("Confusion matrix with counts and percentages:")
confusion_df.show(truncate=False)

# -------------------------------
# 16 Inspect misclassified examples
# -------------------------------
misclassified_df = predictions.filter(col("Label") != col("Prediction"))
misclassified_df = misclassified_df.select(
    "Label",
    "Prediction",
    "probability_high_risk",
    *feature_cols
)
print("Sample misclassified predictions:")
misclassified_df.show(10, truncate=False)
# -------------------------------
# 17 Save predictions for Power BI
# -------------------------------
base_path = "file:///root/data/health_risk/Random_Forest_CVD"
# 17a. Full predictions
columns_to_save = ["Label", "Prediction", "probability_high_risk"] + feature_cols
predictions.select(*columns_to_save) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv(base_path + "/predictions_full")
print("Predictions with selected features CSV saved for Power BI.")

# 17b. Summary of predicted risks
risk_counts = predictions.groupBy("Prediction").count()
total = predictions.count()
risk_percent = risk_counts.withColumn("percentage", (col("count") / total * 100))
risk_percent.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/risk_summary")
print("Summary of predicted risks CSV saved for Power BI.")
# 17c. Misclassified cases
total_misclassified = misclassified_df.count()
if total_misclassified > 0:
    misclassified_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/misclassified_cases")
    print("Misclassified cases CSV saved for Power BI.")
else:
    print("No misclassified cases found — nothing to save.")
# 17d. Feature Importance
rf_model_fitted = model.stages[-1]
if isinstance(rf_model_fitted, RandomForestClassificationModel):
    importances = rf_model_fitted.featureImportances.toArray()
    importance_rows = [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))]
    importance_df = spark.createDataFrame(importance_rows, ["feature", "importance_score"])
    importance_df = importance_df.orderBy(col("importance_score").desc())
    importance_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/feature_importance")
    print("Feature importance CSV saved for Power BI.")
else:
    print("Error: Could not extract RandomForestClassificationModel from the pipeline stages for feature importance.")
# -------------------------------
# 18 Stop Spark session
# -------------------------------
spark.stop()
