# Machine Learning for Health Risk Insights

**Author:** Shahida Batool
**Technologies:** PySpark, Hive, Python, Random Forest, Logistic Regression, Gaussian Mixture Model (GMM), Pandas, Power BI

---

## Project Overview

This project analyzes lifestyle and health-related data from Kaggle, focusing on smoking and drinking habits. The objective is to provide actionable health risk insights using machine learning and big data techniques.

The project combines **big data processing** with **machine learning models** to identify high-risk individuals, uncover hidden patterns, and deliver predictive insights.

---

## Dataset

* **Source:** Kaggle Smoking and Drinking Dataset
* **Description:** Contains individual-level data on age, gender, lifestyle habits (smoking, drinking), and health indicators.
* **Preprocessing:** Data cleaning, feature engineering, and transformations performed using **PySpark** and stored in **Hive tables** for scalable processing.

---

## Methodology

1. **Data Cleaning & Preprocessing**

   * Imported raw CSV data into Hive.
   * Cleaned and transformed data using PySpark.
   * Generated derived features such as age groups, risk scores, and lifestyle indicators.

2. **Exploratory Data Analysis (EDA)**

   * Visualized age, smoking, and drinking distributions.
   * Identified correlations and trends among high-risk groups.

3. **Machine Learning Models**

   * **Logistic Regression:** Predicted likelihood of being in a high-risk group.
   * **Random Forest:** Classified individuals into risk groups and analyzed feature importance.
   * **Gaussian Mixture Model (GMM):** Clustered individuals to detect hidden health risk patterns.

4. **Evaluation**

   * Classification models evaluated using accuracy, RMSE, and confusion matrices.
   * Clustering results visualized to interpret risk clusters.

---

## Key Insights

* High-risk individuals are mainly aged 40–60, with some in their 30s.
* Significant portion of high-risk group continues smoking and drinking.
* Random Forest highlighted influential features such as age, lifestyle habits, and cholesterol/glucose levels.
* GMM clustering revealed three main health risk clusters, enabling better segmentation of individuals.

---

## Tools & Technologies

* **Big Data:** Hive, PySpark, Hadoop (HDFS)
* **Machine Learning:** Scikit-learn, PySpark MLlib
* **Data Analysis & Visualization:** Pandas, Matplotlib, Seaborn, Power BI
* **Programming Languages:** Python, SQL

---

## How to Run

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/health-risk-insights.git
   ```
2. Load the dataset into Hive tables.
3. Execute PySpark scripts for preprocessing, EDA, and ML models.
4. Visualize results using Power BI or Python plots.

---

## Future Work

* Incorporate additional health indicators for improved risk prediction.
* Deploy models using interactive dashboards (Streamlit or Dash).
* Integrate time-series data for predictive monitoring of lifestyle changes.

---

## License

This project is open-source and available for educational and research purposes.
