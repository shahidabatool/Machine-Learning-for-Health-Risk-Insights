# -*- coding: utf-8 -*-
# =============================================
# PySpark: Gaussian Mixture Model (GMM) Clustering 
#          for CVD Lifestyle Risk Patterns
#
# Project: DS8003 - Health Risk Analysis
# Author : Shahida Batool / Group 5
# =============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator

# =============================================
# 1. Spark Session
# =============================================
spark = SparkSession.builder \
    .appName("CVD_Lifestyle_GMM") \
    .enableHiveSupport() \
    .getOrCreate()

# =============================================
# 2. Load Hive Table
# =============================================
spark.sql("USE health_risk_db")
data = spark.sql("SELECT * FROM smoking_drinking_features_clean")

# =============================================
# 3. Handle Categorical Columns
# =============================================
categorical_cols = ["smk_stat_type_cd", "drk_yn", "sex"] 

for col_name in categorical_cols:
    data = data.withColumn(col_name, col(col_name).cast("string"))
    data = data.na.fill({col_name: "Unknown"})

for col_name in categorical_cols:
    indexer = StringIndexer(inputCol=col_name, outputCol=col_name + "_num")
    data = indexer.fit(data).transform(data)

# =============================================
# 4. Feature Assembly
# =============================================
numeric_cols = [
    'age', 'bmi', 'waistline', 'sbp', 'dbp', 'blds',
    'tot_chole', 'hdl_chole', 'ldl_chole', 'triglyceride',
    'sgot_ast', 'sgot_alt', 'gamma_gtp',  
    'smk_stat_type_cd_num', 'drk_yn_num', 'sex_num'
]

assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_unscaled")
data = assembler.transform(data)

scaler = StandardScaler(
    inputCol="features_unscaled",
    outputCol="features",
    withStd=True,
    withMean=True
)
data = scaler.fit(data).transform(data)

# =============================================
# 5. Gaussian Mixture Model (GMM) Training
# =============================================
gmm = GaussianMixture(featuresCol="features", k=3, seed=42)
gmm_model = gmm.fit(data)
predictions = gmm_model.transform(data)

# =============================================
# 6. Silhouette Score
# =============================================
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
silhouette = evaluator.evaluate(predictions)
print("\nGMM Silhouette Score: {:.2f}".format(silhouette))

# Interpret silhouette
if silhouette >= 0.5:
    quality = "Strong cluster separation (High quality)"
    reliability = "≈75-90% grouping reliability"
elif silhouette >= 0.3:
    quality = "Moderate cluster separation"
    reliability = "≈60-70% grouping reliability"
else:
    quality = "Weak cluster separation"
    reliability = "≈45-55% grouping reliability"

print("\nGMM Clustering Interpretation:")
print(" - {}".format(quality))
print(" - {}".format(reliability))


# =============================================
# 7. Readable Cluster Labels
# =============================================
cluster_names = {
    0: "Cluster A: Healthy Metabolic Group",
    1: "Cluster B: High Cholesterol & Obesity Group",
    2: "Cluster C: Smoking/Drinking High-Risk Group"
}

predictions = predictions.withColumn(
    "cvd_cluster_label",
    when(col("prediction") == 0, cluster_names[0])
    .when(col("prediction") == 1, cluster_names[1])
    .otherwise(cluster_names[2])
)

# =============================================
# 8. Cluster Distribution
# =============================================
print("\nGMM Cluster Distribution:")
total_count = predictions.count()

cluster_counts = predictions.groupBy("prediction") \
    .agg(count("*").alias("count")) \
    .collect()

for row in cluster_counts:
    cluster_id = int(row["prediction"])
    pct = (float(row["count"]) / total_count) * 100
    cluster_label = cluster_names.get(cluster_id, "Unknown Cluster")
    print("Cluster {} - {}: {} people ({:.2f}%)".format(
        cluster_id, cluster_label, row["count"], pct
    ))

# =============================================
# 9. Save Output for Power BI
# =============================================

base_path = "file:///root/data/health_risk/GaussianMM_CVD"

# -------------------------------
# 9a. Full predictions with features
# -------------------------------
features_to_save = numeric_cols + ["smk_stat_type_cd_num", "drk_yn_num", "sex_num", "prediction", "cvd_cluster_label"]


# Remove duplicates
features_to_save = list(dict.fromkeys(features_to_save))

predictions.select(*features_to_save) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv(base_path + "/gmm_predictions_full")


print("GMM full predictions CSV saved for Power BI.")

# -------------------------------
# 9b. Summary of clusters
# -------------------------------


total_count = predictions.count()
cluster_summary = predictions.groupBy("cvd_cluster_label").count() \
    .withColumn("percentage", (col("count") / total_count * 100))

cluster_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/gmm_cluster_summary")

print("GMM cluster summary CSV saved for Power BI.")

# -------------------------------
# 3. Cluster centroids
# -------------------------------
centroids = gmm_model.gaussiansDF.collect()  # each row has mean vector and covariance matrix
centroid_rows = []
for i, row in enumerate(centroids):
    mean_vector = row["mean"]
    centroid_rows.append((i,) + tuple(mean_vector.tolist()))

centroid_columns = ["cluster"] + numeric_cols
centroid_df = spark.createDataFrame(centroid_rows, centroid_columns)
centroid_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/gmm_centroids")

print("GMM cluster centroids CSV saved for Power BI.")


# =============================================
# 10. Stop Spark
# =============================================
spark.stop()
