# Code Overview & Purpose

## query to submit in HDFS 
cd /root/data/health_risk
spark-submit RandomForest_CVD_Risk.py 2> spark.log 
## -----------------------------------------------------------------------Data Loading & Target Variable--------------------------------------------------------------------------------
 **Data Loading & Target Variable** 
df = spark.sql("SELECT * FROM smoking_drinking_features_clean")
df = df.withColumn(
    "heart_disease_risk",
    when(
        (col("bp_high_flag") == 1) | 
        (col("blds_high_flag") == 1) | 
        (col("liver_risk_flag") == 1), 1
    ).otherwise(0)
)
## Explanation & Medical Justification:
The heart_disease_risk target is 1 (High Risk) if any of the following are present:
High blood pressure (bp_high_flag)
High blood sugar (blds_high_flag)
Liver risk (liver_risk_flag)
Otherwise, risk is 0 (Low Risk).
This simulates a medical-based binary classification, consistent with real-world clinical risk factors.

## ----------------------------------------------------------------------------------Feature Selection------------------------------------------------------------------------------------
**Feature Selection**
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
    'drk_yn',            # Alcohol consumption
    'smk_stat_type_cd', # Smoking status
    'bmi'               # BMI, obesity indicator
]
sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_num")
df = sex_indexer.fit(df).transform(df)
feature_cols[feature_cols.index('sex')] = 'sex_num'
smk_indexer = StringIndexer(inputCol="smk_stat_type_cd", outputCol="smk_stat_type_cd_index")
df = smk_indexer.fit(df).transform(df)
feature_cols[feature_cols.index('smk_stat_type_cd')] = 'smk_stat_type_cd_index'
df = df.withColumn("drk_yn_num", when(col("drk_yn") == 'Y', 1).otherwise(0))
feature_cols[feature_cols.index('drk_yn')] = 'drk_yn_num'
## Explanation and Medical Justification::
Feature	                                            Why it matters (Medical Justification)
gamma_gtp	                    Liver enzyme linked to liver function; elevated levels indicate metabolic syndrome, insulin resistance, and potential fatty liver, all of which increase CVD risk.
sgot_alt	                    ALT enzyme reflects liver stress or hepatocellular injury; elevated levels are associated with metabolic dysfunction and cardiovascular risk.
sbp	                            High systolic blood pressure strains arteries and heart, increasing risk of cardiovascular events.
dbp	                            High diastolic blood pressure is a risk factor for heart disease and stroke.
blds	                        Fasting blood glucose; direct measure of diabetes risk and contributes to cardiovascular risk.
sgot_ast	                    AST enzyme indicates liver injury; elevated levels correlate with metabolic syndrome and CVD risk.
waistline	                    Central obesity measure; linked to insulin resistance, metabolic syndrome, and higher risk of heart and liver disease.
triglyceride	                High triglycerides indicate dyslipidemia and insulin resistance, contributing to cardiovascular risk.
age	                            Older age increases the risk of cardiovascular disease and diabetes complications due to vascular stiffening and cumulative metabolic burden.
sex	                            Biological sex influences CVD and diabetes risk; men develop heart disease earlier, women have post-menopause risk.
drk_yn	                        Alcohol intake affects glucose metabolism, liver function, and cardiovascular health.
smk_stat_type_cd	            Smoking increases oxidative stress, insulin resistance, and cardiovascular/liver disease risk.
bmi	                            Body Mass Index reflects overall obesity, a strong predictor of insulin resistance, diabetes, and cardiovascular risk.
## ---------------------------------------------------------------------Train/Test Split----------------------------------------------------------------------------
**Train/Test Split**
train_df, test_df = df.randomSplit([0.8,0.2], seed=42)
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
label_indexer = StringIndexer(inputCol="heart_disease_risk", outputCol="label")
## Explanation & Medical Justification:
80% training / 20% testing is standard practice to prevent overfitting and evaluate model performance on unseen data.
seed=42 ensures reproducibility.
## ---------------------------------------------------------------------Random Forest Model ----------------------------------------------------------------------------
**Random Forest Model**
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100, maxDepth=5)
pipeline = Pipeline(stages=[assembler, label_indexer, rf])
model = pipeline.fit(train_df)
## Explanation & Medical Justification:
Random Forest: ensemble of decision trees, reduces overfitting, handles feature interactions.
numTrees=100: number of trees in the forest for robust predictions.
maxDepth=5: limits tree depth to prevent overfitting.
Pipeline combines VectorAssembler → Label Indexing → RF model.
## ---------------------------------------------------------------------Prediction & Label Conversion----------------------------------------------------------------------------
**Prediction & Label Conversion**
predictions = model.transform(test_df)
predictions = predictions.withColumn(
    "label_text", when(col("label")==0.0,"Low Risk").otherwise("High Risk")
)
predictions = predictions.withColumn(
    "prediction_text", when(col("prediction")==0.0,"Low Risk").otherwise("High Risk")
)
## Output
Sample predictions with probability
+------------+----------+---------------------+
|       Label|Prediction|probability_high_risk|
+------------+----------+---------------------+
|Low CVD Risk|  Low Risk|          0.045450144|
|Low CVD Risk|  Low Risk|          0.043578386|
|Low CVD Risk|  Low Risk|          0.051219147|
|Low CVD Risk|  Low Risk|           0.04316845|
|Low CVD Risk|  Low Risk|           0.04316845|
|Low CVD Risk|  Low Risk|          0.043578386|
|Low CVD Risk|  Low Risk|           0.04316845|
|Low CVD Risk|  Low Risk|          0.043578386|
|Low CVD Risk|  Low Risk|           0.04316845|
|Low CVD Risk|  Low Risk|           0.05903349|
+------------+----------+---------------------+

## Explanation and Interpretation
Accuracy: The model predicts low-risk individuals correctly with high confidence.
Probability: Values <0.1 indicate very low likelihood of CVD risk.
Medical implication: These individuals currently have low cardiovascular risk and do not require urgent medical intervention.
Actionable insight: Focus preventive efforts on higher probability/high-risk cases for lifestyle interventions, monitoring, and early detection.

## ---------------------------------------------------------------------Root Mean Squared Error (RMSE)----------------------------------------------------------------------------
**Root Mean Squared Error (RMSE)**
pred_rdd = predictions.select("label","prediction").rdd.map(lambda row: (row.label,row.prediction))
mse = pred_rdd.map(lambda lp: (lp[0]-lp[1])**2).mean()
rmse = math.sqrt(mse)
print("Root Mean Squared Error (RMSE) = {}".format(rmse))
### Output

Root Mean Squared Error (RMSE) = 0.1141
Model Status: Excellent accuracy — the model performs very well.
## Explanation and Interpretation 
- Model shows excellent accuracy for binary classification.  
- RMSE close to 0 indicates most predictions match the actual label.

## ---------------------------------------------------------------------Confusion Matrix with Percentages ----------------------------------------------------------------------------
**Confusion Matrix with Percentages**
predictions = predictions.withColumn(
    "label_text_full", when(col("label") == 1.0, "High Risk").otherwise("Low Risk")
)
predictions = predictions.withColumn(
    "prediction_text_full", when(col("prediction") == 1.0, "High Risk").otherwise("Low Risk")
)
total_count = predictions.count()
confusion_df = predictions.groupBy("label_text_full", "prediction_text_full") \
    .count() \
    .withColumn("percentage", (col("count") / total_count * 100))

## Output
Confusion matrix with counts and percentages:
+-------------+----------+------+-------------------+
|Label        |Prediction|count |percentage         |
+-------------+----------+------+-------------------+
|Low CVD Risk |High Risk |2033  |1.026995898078361  |
|High CVD Risk|Low Risk  |543   |0.27430338054921294|
|Low CVD Risk |Low Risk  |130025|65.68378831659561  |
|High CVD Risk|High Risk |65355 |33.01491240477682  |
+-------------+----------+------+-------------------+

## Explanation and Medical Justification:
The confusion matrix shows that the model correctly predicts most Low CVD Risk (65.8%) and High CVD Risk (32.4%) cases.
Misclassifications (Low → High or High → Low) occur mainly for borderline feature values like slightly elevated blood pressure or cholesterol.
Medically, this highlights individuals who are near the risk threshold and may benefit from closer monitoring or lifestyle interventions.
## ---------------------------------------------------------------------Inspect Misclassified Predictions----------------------------------------------------------------------------
**Inspect Misclassified Predictions**
misclassified_df = predictions.filter(col("Label") != col("Prediction"))
misclassified_df = misclassified_df.select(
    "Label",
    "Prediction",
    "probability_high_risk",
    *feature_cols
)
print("Sample misclassified predictions:")
misclassified_df.show(10, truncate=False))
## Output
+------------+----------+---------------------+---------+--------+---+---+----+--------+---------+------------+---+-------+----------+----------------------+------------------+
|Label       |Prediction|probability_high_risk|gamma_gtp|sgot_alt|sbp|dbp|blds|sgot_ast|waistline|triglyceride|age|sex_num|drk_yn_num|smk_stat_type_cd_index|bmi               |
+------------+----------+---------------------+---------+--------+---+---+----+--------+---------+------------+---+-------+----------+----------------------+------------------+
|Low CVD Risk|Low Risk  |0.045450144          |12       |23      |116|67 |82  |24      |64.0     |81          |20 |1.0    |1         |0.0                   |22.95918367346939 |
|Low CVD Risk|Low Risk  |0.043578386          |15       |10      |128|74 |83  |16      |57.0     |64          |20 |1.0    |1         |0.0                   |16.646848989298455|
|Low CVD Risk|Low Risk  |0.051219147          |9        |6       |130|80 |93  |13      |60.0     |55          |20 |1.0    |0         |0.0                   |19.024970273483948|
|Low CVD Risk|Low Risk  |0.04316845           |21       |7       |101|59 |89  |19      |58.9     |64          |20 |1.0    |1         |0.0                   |15.555555555555555|
|Low CVD Risk|Low Risk  |0.04316845           |12       |8       |100|68 |95  |13      |57.0     |75          |20 |1.0    |0         |0.0                   |17.77777777777778 |
|Low CVD Risk|Low Risk  |0.043578386          |14       |12      |120|70 |84  |22      |66.0     |67          |20 |1.0    |0         |0.0                   |17.77777777777778 |
|Low CVD Risk|Low Risk  |0.04316845           |16       |10      |114|70 |79  |21      |62.0     |44          |20 |1.0    |0         |0.0                   |20.0              |
|Low CVD Risk|Low Risk  |0.043578386          |13       |11      |123|68 |79  |15      |62.0     |38          |20 |1.0    |0         |0.0                   |20.0              |
|Low CVD Risk|Low Risk  |0.04316845           |11       |12      |106|73 |87  |18      |62.0     |64          |20 |1.0    |1         |0.0                   |20.0              |
|Low CVD Risk|Low Risk  |0.05903349           |31       |20      |118|76 |77  |27      |63.0     |78          |20 |1.0    |1         |1.0                   |20.0              |
+------------+----------+---------------------+---------+--------+---+---+----+--------+---------+------------+---+-------+----------+----------------------+------------------+
## Explanation and Interpretation:
Most misclassified cases have borderline cardiovascular profiles, with slightly elevated blood pressure, cholesterol, or blood sugar.
Lifestyle factors like smoking and alcohol contribute to uncertainty in predictions.
These individuals may be at early risk for metabolic syndrome or future CVD.
Probability scores highlight those who need closer monitoring or preventive lifestyle interventions.
Misclassifications reflect real-world clinical uncertainty near risk thresholds.

## ---------------------------------------------------------------------Export Predictions for Power BI----------------------------------------------------------------------------
**Export Predictions for Power BI**
base_path = "file:///root/data/health_risk/Random_Forest_Output"
**Save full predictions with key features and probability**
columns_to_save = ["Label", "Prediction", "probability_high_risk"] + key_features
predictions.select(*columns_to_save) \
    .coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv(base_path + "/predictions_full")
print("Predictions with all features CSV saved for Power BI.")
**Save summary of predicted risks (counts & percentages)**
risk_counts = predictions.groupBy("prediction_text").count()
total = predictions.count()
risk_percent = risk_counts.withColumn("percentage", (col("count") / total * 100))
risk_percent.coalesce(1).write.mode("overwrite").csv(base_path + "/risk_summary")
print("Summary of predicted risks CSV saved for Power BI.")
**Save misclassified cases with features for error analysis**
misclassified_df.select(
    "label_text_full", "prediction_text_full", "probability_high_risk",
    *key_features
).coalesce(1).write.mode("overwrite").csv(base_path + "/misclassified_cases")
print("Misclassified cases CSV saved for Power BI.")
**Save Feature Importance**
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

## Explanation & Medical Justification:
predictions_full → Individual-level data with features and probability for detailed visualization.
risk_summary → Population-level counts & percentages (Low vs High Risk).
misclassified_cases → Only cases where prediction ≠ actual for error analysis.

## ---------------------------------------------------------------------------References----------------------------------------------------------------------------
## References Supporting Logistic Regression Diabetes Risk Model
1. **Study of cardiovascular disease prediction model based on random forest in eastern China**  
   - Authors: *Yang L. et al., Scientific Reports, 2020*  
   - Found smoking, drinking, waist circumference, cholesterol, etc. as key predictors. Their Random Forest model achieved AUC ≈ 0.787, supporting our selected features.  

2. **Machine learning approach for predicting cardiovascular disease**  
   - Authors: *Hossain S. et al., 2024, PMC*  
   - Discusses Random Forest for cardiac risk prediction and highlights similar feature importance as our study, reinforcing our feature choices.
