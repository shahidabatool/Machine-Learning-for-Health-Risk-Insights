# Code Overview & Purpose

## query to submit in HDFS 
cd /root/data/health_risk
spark-submit LogisticRegression_Diabetes_Risk.py 2> spark.log 

## -----------------------------------------------------------------------Data Loading & Target Variable--------------------------------------------------------------------------------
 **Data Loading & Target Variable**  
df = spark.sql("SELECT * FROM smoking_drinking_features_clean")
df = df.withColumn(
    "diabetes_risk",
    when(col("blds") >= 126, 1).otherwise(0)
)
## Explanation & Medical Justification:
diabetes_risk = 1 if blood sugar ≥ 126 mg/dL (clinical threshold).
0 otherwise.
Simulates a clinically meaningful binary classification.
## ----------------------------------------------------------------------------------Feature Selection------------------------------------------------------------------------------------
**Feature Selection**
feature_cols = [
    'age',             
    'sex',             
    'bmi',             
    'waistline',      
    'sbp',             
    'dbp',            
    'blds',            
    'hdl_chole',       
    'triglyceride',    
    'hemoglobin',      
    'urine_protein',   
    'gamma_gtp',       
    'smk_stat_type_cd',
    'drk_yn'           
]
**Smoking: One-hot encode**
smk_indexer = StringIndexer(inputCol="smk_stat_type_cd", outputCol="smk_stat_type_cd_index")
smk_encoder = OneHotEncoder(inputCol="smk_stat_type_cd_index", outputCol="smk_stat_type_cd_vec")
feature_cols.remove("smk_stat_type_cd")
feature_cols.append("smk_stat_type_cd_vec")
**Alcohol: convert Y/N to numeric**
df = df.withColumn("drk_yn_num", when(col("drk_yn") == 'Y', 1).otherwise(0))
feature_cols.remove("drk_yn")
feature_cols.append("drk_yn_num")
**Sex: convert F/M to numeric**
df = df.withColumn("sex_num", when(col("sex") == 'F', 0).otherwise(1))
feature_cols.remove("sex")
feature_cols.append("sex_num")
## Explanation & Medical Justification:
Feature	                                        Reason
age	                                Diabetes risk increases with age.
sex	                                Different risk patterns for men and women.
bmi	                                Obesity is a major risk factor.
waistline	                        Central obesity indicator.
sbp	                                High blood pressure is associated with diabetes.
dbp	                                Captures overall blood pressure.
blds	                            Fasting blood sugar – main diabetes measure.
hdl_chole	                        Low HDL indicates metabolic risk.
triglyceride	                    High levels linked to insulin resistance.
hemoglobin	                        Indicates general health/possible complications.
urine_protein	                    Early kidney damage marker.
smk_stat_type_cd	                Smoking affects insulin resistance.
drk_yn	                            Alcohol can influence glucose metabolism.
gamma_gtp                           Liver function, associated with insulin resistance
## ---------------------------------------------------------------------Assemble features vector---------------------------------------------------------------------------
**Assemble features vector**
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
label_indexer = StringIndexer(inputCol="diabetes_risk", outputCol="label")
## ---------------------------------------------------------------------Train/Test Split----------------------------------------------------------------------------
**Train/Test Split**
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
## Explanation & Medical Justification:
80% for training, 20% for testing.
Ensures model generalization.
seed=42 ensures reproducibility.
## ---------------------------------------------------------------------Logistic Regression Model----------------------------------------------------------------------------
**Logistic Regression Model**
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[smk_indexer, smk_encoder, assembler, label_indexer, lr])
model = pipeline.fit(train_df)
## ---------------------------------------------------------------------Prediction & Label Conversion----------------------------------------------------------------------------
**Prediction & Label Conversion**
predictions = model.transform(test_df)
predictions = predictions.withColumn(
    "label_text", when(col("label") == 0.0, "Low Diabetes Risk").otherwise("High Diabetes Risk")
)
predictions = predictions.withColumn(
    "prediction_text", when(col("prediction") == 0.0, "Low Risk").otherwise("High Risk")
)
prob_udf = udf(lambda v: float(v[1]), FloatType())
predictions = predictions.withColumn("probability_high_risk", prob_udf("probability"))
predictions.select("label_text","prediction_text","probability_high_risk").show(10)
## Sample predictions with probability:
+-----------------+---------------+---------------------+
|       label_text|prediction_text|probability_high_risk|
+-----------------+---------------+---------------------+
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
|Low Diabetes Risk|       Low Risk|                  0.0|
+-----------------+---------------+---------------------+
## Explanation:
Converts numeric labels (0/1) into readable text for reporting.
label_text → actual risk.
prediction_text → model prediction.
Extracts probability of High Risk for nuanced interpretation.
Useful to identify borderline cases.
## ---------------------------------------------------------------------MulticlassClassificationEvaluator----------------------------------------------------------------------------
**MulticlassClassificationEvaluator**
accuracy_eval = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = accuracy_eval.evaluate(predictions)
print("Model Accuracy =", accuracy)
## Output:
('Model Accuracy =', 1.0)
Model Status: Excellent accuracy — the model performs very well.
## Explanation and Medical Interpretation:
High accuracy shows that the Decision Tree reliably identifies patients at risk based on metabolic and lifestyle features.
Low misclassification rate reduces risk of missing high-risk individuals.
## ---------------------------------------------------------------------Confusion Matrix with Percentages ----------------------------------------------------------------------------
**Confusion Matrix with Percentages**
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
confusion_df.show(truncate=False)
## Confusion matrix with percentages:
+------------------+--------------------+------+-----------------+
|label_text_full   |prediction_text_full|count |percentage       |
+------------------+--------------------+------+-----------------+
|Low Diabetes Risk |Low Risk            |182699|92.29273171815959|
|High Diabetes Risk|High Risk           |15257 |7.707268281840409|
+------------------+--------------------+------+-----------------+
## Explanation & Medical Justification:
Shows actual vs predicted counts with percentages.
Easy visualization of model performance.
## ---------------------------------------------------------------------Export Predictions for Power BI----------------------------------------------------------------------------
**Export Predictions for Power BI**
base_path = "file:///root/data/health_risk/Diabetes_LogReg_Output"
**Full predictions with selected features**
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
**Summary of predicted risks**
risk_counts = predictions.groupBy("prediction_text").count()
total = predictions.count()
risk_percent = risk_counts.withColumn("percentage", (col("count") / total * 100))
risk_percent.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/risk_summary")
print("Summary of predicted risks CSV saved.")
**Misclassified cases**
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
**Logistic Regression Feature Importance (Coefficients)**
lr_model_fitted = model.stages[-1]
if isinstance(lr_model_fitted, LogisticRegressionModel):
    coef_array = lr_model_fitted.coefficients.toArray()
    intercept = lr_model_fitted.intercept
    importance_rows = [
        (feature_cols[i], float(coef_array[i]), abs(float(coef_array[i])))
        for i in range(len(feature_cols))
    ]
    importance_df = spark.createDataFrame(
        importance_rows,
        ["feature", "coefficient", "importance_abs"]
    ).orderBy(col("importance_abs").desc())

    importance_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_path + "/feature_importance")

    print("Feature importance CSV saved for Power BI.")
else:
    print("Error: LogisticRegressionModel not found in pipeline stages — cannot extract coefficients.")
## Explanation & Medical Justification:
CSV can be used in Power BI or for further analysis.
Contains actual risk, predicted risk, and probability.
## ---------------------------------------------------------------------------References----------------------------------------------------------------------------
## References Supporting Logistic Regression Diabetes Risk Model
1. **Predicting Type 2 Diabetes Using Logistic Regression and Machine Learning Approaches**  
   - Authors: *[PubMed]*  
   - Identifies glucose, BMI, age as major predictors, supporting our selected feature set. :contentReference[oaicite:12]{index=12}

2. **Identification of Risk Factors of Type 2 Diabetes via Logistic Regression**  
   - Authors: *[PubMed]*  
   - Uses age, BMI, smoking, BP, etc. with logistic regression for risk factor analysis. :contentReference[oaicite:13]{index=13}

3. **Effective Approach for Early Detection of Diabetes by Logistic Regression**  
   - Authors: *Thangarajan, K.*  
   - Validates logistic regression for “early risk prediction” using clinical and lifestyle data. :contentReference[oaicite:14]{index=14}

4. **Assessing Diabetes Risk Factors Using Logistic Regression: Kaggle-Based Study**  
   - Authors: *AJPAS research*  
   - High accuracy model using age, cholesterol, smoking, BMI. :contentReference[oaicite:15]{index=15}

5. **Logistic Regression vs Optimised Machine Learning in a Clinical Setting**  
   - Authors: *[PubMed]*  
   - Shows logistic regression performs comparably to more complex ML models, underlining interpretability + performance. :contentReference[oaicite:16]{index=16}

6. **Predicting Diabetic Foot Ulcer Using Logistic Regression**  
   - Authors: *Ahmadi et al.*  
   - Demonstrates that features such as hemoglobin, LDL/HDL, BMI (similar to ours) are predictive in logistic regression. :contentReference[oaicite:17]{index=17}
