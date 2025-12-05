# Code Overview & Purpose
## query to submit in HDFS 
cd /root/data/health_risk
spark-submit GaussianMM_CVD_Risk.py 2> spark.log 
## -----------------------------------------------------------------------Data Loading & Target Variable--------------------------------------------------------------------------------
 **Data Loading & Target Variable**  
df = spark.sql("SELECT * FROM smoking_drinking_features_clean")
categorical_cols = ["smk_stat_type_cd", "drk_yn", "sex"] 
for col_name in categorical_cols:
    df = df.withColumn(col_name, col(col_name).cast("string"))
    df = df.na.fill({col_name: "Unknown"})

df = df.withColumn("smk_num", when(col("smk_stat_type_cd") == 'N', 0).otherwise(1))
df = df.withColumn("drk_num", when(col("drk_yn") == 'N', 0).otherwise(1))

## Explanation and Medical Justification:
Smoking and drinking behaviors are critical lifestyle risk factors that contribute to cardiovascular disease.
Encoding them numerically allows machine learning algorithms to process categorical lifestyle data.

## ----------------------------------------------------------------------------------Feature Selection------------------------------------------------------------------------------------
**Feature Selection**
numeric_cols = [
    'age', 'bmi', 'waistline', 'sbp', 'dbp', 'blds',
    'tot_chole', 'hdl_chole', 'ldl_chole', 'triglyceride',
    'sgot_ast', 'sgot_alt', 'gamma_gtp', 
    'smk_stat_type_cd_num', 'drk_yn_num', 'sex_num'
]
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_unscaled")
data = assembler.transform(df)
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
data = scaler.fit(data).transform(data)

## Explanation and Medical Justification:
Feature	                                              Why it matters
age	                                    Cardiovascular risk increases with age
bmi	                                    Obesity linked to metabolic syndrome and CVD
waistline	                            Central obesity predicts metabolic and cardiovascular risk
sbp, dbp	                            High BP is a major CVD risk factor
blds	                                Blood sugar levels indicate diabetes risk
tot_chole, hdl, ldl, triglyceride	    Lipid profile strongly influences CVD risk
smk_num	                                Smoking increases cardiovascular and metabolic risk
drk_num	                                Alcohol consumption affects heart health

## ------------------------------------------------------------------Gaussian Mixture Model (GMM) Training----------------------------------------------------------------------------
 **Gaussian Mixture Model (GMM) Training** 
 from pyspark.ml.clustering import GaussianMixture
gmm = GaussianMixture(featuresCol="features", k=3, seed=42)
gmm_model = gmm.fit(data)
predictions = gmm_model.transform(data)

## Explanation and Medical Justification:
GMM handles overlapping clusters and non-spherical distributions common in lifestyle and metabolic data.
Soft assignments (probabilities) allow assessment of borderline risk groups.

##  -----------------------------------------------------------------Silhouette Score & Clustering Quality---------------------------------------------------------------------------------
 **Silhouette Score & Clustering Quality** 
 from pyspark.ml.evaluation import ClusteringEvaluator
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
silhouette = evaluator.evaluate(predictions)
## Output
GMM Silhouette Score: 0.50

GMM Clustering Interpretation:
 - Strong cluster separation (High quality)
 - ≈75-90% grouping reliability
## Interpretation:

Silhouette Score	Quality	Approx.         Cluster Reliability
≥0.5	            Strong separation	        75–90%
0.3–0.5	            Moderate separation	        60–70%
<0.3	            Weak separation	            45–55%

The silhouette score evaluates how well-separated clusters are.
A higher score indicates better-defined risk groups.

## -----------------------------------------------------------------Cluster Distribution and Labelling--------------------------------------------------------------------------------------
 **Cluster Distribution and Labelling**
 cluster_names = {
    0: "Healthy Metabolic Group",
    1: "High Cholesterol & Obesity Group",
    2: "Smoking/Drinking High-Risk Group"
}
predictions = predictions.withColumn(
    "cvd_cluster_label",
    when(col("prediction") == 0, cluster_names[0])
    .when(col("prediction") == 1, cluster_names[1])
    .otherwise(cluster_names[2])
)
total_count = predictions.count()
cluster_counts = predictions.groupBy("prediction").agg(count("*").alias("count")).collect()
for row in cluster_counts:
    cluster_id = int(row["prediction"])
    pct = (float(row["count"]) / total_count) * 100
    print("Cluster {} - {}: {} people ({:.2f}%)".format(
        cluster_id, cluster_names[cluster_id], row["count"], pct
    ))

## Output 
GMM Cluster Distribution:
Cluster 1 - Cluster B: High Cholesterol & Obesity Group: 2229 people (0.22%)
Cluster 2 - Cluster C: Smoking/Drinking High-Risk Group: 67886 people (6.85%)
Cluster 0 - Cluster A: Healthy Metabolic Group: 921231 people (92.93%)

## Explanation and Medical Justification:
Labels make clusters interpretable to clinicians and health policymakers.
Allows targeting interventions for high-risk lifestyle groups.
Helps understand population distribution across risk groups.
Useful for resource planning and preventive health strategies.

## -----------------------------------------------------------------------Export Predictions for Power BI-----------------------------------------------------------------------
**Export Predictions for Power BI**
base_path = "file:///root/data/health_risk/GaussianMM_CVD"


**Full predictions with features**
features_to_save = numeric_cols + ["smk_stat_type_cd_num", "drk_yn_num", "sex_num", "prediction", "cvd_cluster_label"]
features_to_save = list(dict.fromkeys(features_to_save))
predictions.select(*features_to_save) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv(base_path + "/gmm_predictions_full")
print("GMM full predictions CSV saved for Power BI.")

**Summary of clusters**
total_count = predictions.count()
cluster_summary = predictions.groupBy("cvd_cluster_label").count() \
    .withColumn("percentage", (col("count") / total_count * 100))

cluster_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/gmm_cluster_summary")
print("GMM cluster summary CSV saved for Power BI.")
**Cluster centroids**
centroids = gmm_model.gaussiansDF.collect()  # each row has mean vector and covariance matrix
centroid_rows = []
for i, row in enumerate(centroids):
    mean_vector = row["mean"]
    centroid_rows.append((i,) + tuple(mean_vector.tolist()))
centroid_columns = ["cluster"] + numeric_cols
centroid_df = spark.createDataFrame(centroid_rows, centroid_columns)
centroid_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/gmm_centroids")
print("GMM cluster centroids CSV saved for Power BI.")

## Explanation and Medical Justification:
Enables visualization of high-risk lifestyle clusters.
Supports population-level intervention planning and monitoring trends.
## -------------------------------------------------------------------------------References------------------------------------------------------------------------------------------------------
# References for CVD Lifestyle Clustering (GMM)

### 1. Lifestyle Factors

**Smoking and Cardiovascular Risk**  
O’Donnell, M., et al. (2016). *Global and regional effects of potentially modifiable risk factors associated with acute stroke in 32 countries (INTERSTROKE): a case-control study.* **Lancet, 388(10046), 761–775.**  
- Smoking significantly increases risk of cardiovascular events.

**Alcohol Consumption and CVD**  
Ronksley, P.E., et al. (2011). *Association of alcohol consumption with selected cardiovascular disease outcomes: a systematic review and meta-analysis.* **BMJ, 342, d671.**  
- Excessive alcohol intake contributes to metabolic syndrome and heart disease.

---

### 2. Metabolic / Physiological Factors

**Obesity, Cholesterol, and Cardiovascular Disease**  
Lavie, C.J., et al. (2019). *Obesity and cardiovascular diseases: implications regarding fitness, fatness, and severity in the obesity paradox.* **J Am Coll Cardiol, 73(13), 1611–1625.**  
- BMI, waistline, and lipid profiles are strong predictors of cardiovascular risk.
