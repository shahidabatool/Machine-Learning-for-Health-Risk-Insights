# Phase 2 – HDFS & Hive Baseline Analysis

**Module:** DS8003 – Big Data Tools  
**Project:** Health Risk Analysis  
**Author:** Nimrah / Group 5  
**Branch:** `hive-work`  

---

## 1. Quick Sanity Checks (Tables + Row Counts)

Before detailed analysis, basic row-count checks were run on the two core tables:

- `smoking_drinking_raw`
- `smoking_drinking_features`

**Queries**

```sql
-- SELECT COUNT(*) AS cnt_raw FROM smoking_drinking_raw;
-- SELECT COUNT(*) AS cnt_features FROM smoking_drinking_features;
```

These were used to verify that:

- both tables are non-empty, and  
- the processed features table has the same number of rows as the raw table (≈ 991k).

---

## 2. Data Quality Checks (smoking_drinking_raw)

This section documents the basic data quality checks run on the raw Hive table
`smoking_drinking_raw` before creating the cleaned features table.

---

### 2.1 NULL / Missing Value Checks

**Query**

```sql
SELECT
  COUNT(*)                                          AS total_rows,
  SUM(CASE WHEN sex IS NULL OR sex = '' THEN 1 ELSE 0 END)           AS missing_sex,
  SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END)                        AS missing_age,
  SUM(CASE WHEN height IS NULL THEN 1 ELSE 0 END)                     AS missing_height,
  SUM(CASE WHEN weight IS NULL THEN 1 ELSE 0 END)                     AS missing_weight,
  SUM(CASE WHEN SBP IS NULL THEN 1 ELSE 0 END)                        AS missing_SBP,
  SUM(CASE WHEN DBP IS NULL THEN 1 ELSE 0 END)                        AS missing_DBP,
  SUM(CASE WHEN BLDS IS NULL THEN 1 ELSE 0 END)                       AS missing_BLDS,
  SUM(CASE WHEN SGOT_AST IS NULL THEN 1 ELSE 0 END)                   AS missing_SGOT_AST,
  SUM(CASE WHEN SGOT_ALT IS NULL THEN 1 ELSE 0 END)                   AS missing_SGOT_ALT,
  SUM(CASE WHEN gamma_GTP IS NULL THEN 1 ELSE 0 END)                  AS missing_gamma_GTP,
  SUM(CASE WHEN SMK_stat_type_cd IS NULL THEN 1 ELSE 0 END)           AS missing_SMK_stat_type_cd,
  SUM(CASE WHEN DRK_YN IS NULL OR DRK_YN = '' THEN 1 ELSE 0 END)      AS missing_DRK_YN
FROM smoking_drinking_raw;
```

**Output (summary)**

- Total rows ≈ **991k**.  
- **No missing values** in key numeric variables (age, height, weight, SBP, DBP, BLDS, liver enzymes).  
- Smoking (`SMK_stat_type_cd`) and drinking (`DRK_YN`) have **no or negligible missing values**.  

**Conclusion:** dataset is essentially complete; no imputation needed at this stage.

---

### 2.2 Category Checks (Smoking & Drinking)

**Queries**

```sql
SELECT DISTINCT SMK_stat_type_cd
FROM smoking_drinking_raw
ORDER BY SMK_stat_type_cd;

SELECT DISTINCT DRK_YN
FROM smoking_drinking_raw
ORDER BY DRK_YN;
```

**Output (summary)**

- `SMK_stat_type_cd` distinct values: **{1, 2, 3}**  
- `DRK_YN` distinct values: **{'N', 'Y'}**

**Conclusion:** categories are clean and match the expected coding used in Phase 1.

---

### 2.3 Range Checks (Key Numeric Columns)

**Queries**

```sql
SELECT
  MIN(age)    AS min_age,
  MAX(age)    AS max_age,
  MIN(height) AS min_height,
  MAX(height) AS max_height,
  MIN(weight) AS min_weight,
  MAX(weight) AS max_weight
FROM smoking_drinking_raw;

SELECT
  MIN(SBP)        AS min_SBP,
  MAX(SBP)        AS max_SBP,
  MIN(DBP)        AS min_DBP,
  MAX(DBP)        AS max_DBP,
  MIN(BLDS)       AS min_BLDS,
  MAX(BLDS)       AS max_BLDS,
  MIN(SGOT_AST)   AS min_SGOT_AST,
  MAX(SGOT_AST)   AS max_SGOT_AST,
  MIN(SGOT_ALT)   AS min_SGOT_ALT,
  MAX(SGOT_ALT)   AS max_SGOT_ALT,
  MIN(gamma_GTP)  AS min_gamma_GTP,
  MAX(gamma_GTP)  AS max_gamma_GTP
FROM smoking_drinking_raw;
```

**Output (summary)**

Approximate observed ranges:

- Height: **130–190 cm**  
- Weight: **25–140 kg**  
- SBP: **67–273 mmHg**  
- DBP: **32–185 mmHg**  
- BLDS: **25–852 mg/dL**  
- SGOT_AST: **1–9999**  
- SGOT_ALT: **1–7210**  
- gamma_GTP: **1–999**

**Conclusion:**  
Most values fall in realistic clinical ranges, but a small number of **extreme highs** (especially for liver enzymes and BLDS) are treated as outliers in the next section.

---

### 2.4 Simple Outlier / Invalid Value Counts

**Query**

```sql
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN age < 20 OR age > 100 THEN 1 ELSE 0 END)           AS invalid_age,
  SUM(CASE WHEN height < 120 OR height > 220 THEN 1 ELSE 0 END)    AS invalid_height,
  SUM(CASE WHEN weight < 30 OR weight > 200 THEN 1 ELSE 0 END)     AS invalid_weight,
  SUM(CASE WHEN SBP < 60 OR SBP > 260 THEN 1 ELSE 0 END)           AS invalid_SBP,
  SUM(CASE WHEN DBP < 30 OR DBP > 150 THEN 1 ELSE 0 END)           AS invalid_DBP,
  SUM(CASE WHEN BLDS < 40 OR BLDS > 400 THEN 1 ELSE 0 END)         AS invalid_BLDS
FROM smoking_drinking_raw;
```

**Output (summary)**

- Only a **very small fraction** of rows fall outside these sanity ranges (well under 1% of the dataset).  

**Conclusion:**  
Rather than deleting these records, Phase 2 keeps all rows and later **adds explicit outlier flags** in `smoking_drinking_features_clean` (Section 3).

---

## 3. Creation of Cleaned Features Table (smoking_drinking_features_clean)

Section 3 creates a cleaned version of the Phase-1 table (`smoking_drinking_features`)
by **preserving all original columns** and adding **rule-based outlier flags**.  
No rows are removed — the goal is to keep the dataset complete while marking extreme values.

---

### 3.1 Create Clean Table with Outlier Flags

**Query**

```sql
DROP TABLE IF EXISTS smoking_drinking_features_clean;

CREATE TABLE smoking_drinking_features_clean AS
SELECT
  f.*,  -- all existing columns from phase 1

  CASE 
    WHEN weight < 30 OR weight > 200 THEN 1 
    ELSE 0 
  END AS weight_outlier_flag,

  CASE 
    WHEN SBP > 250 THEN 1 
    ELSE 0 
  END AS sbp_outlier_flag,

  CASE 
    WHEN DBP > 150 THEN 1 
    ELSE 0 
  END AS dbp_outlier_flag,

  CASE 
    WHEN BLDS < 40 OR BLDS > 400 THEN 1 
    ELSE 0 
  END AS blds_outlier_flag,

  CASE 
    WHEN SGOT_AST > 1000 THEN 1 
    ELSE 0 
  END AS ast_outlier_flag,

  CASE 
    WHEN SGOT_ALT > 1000 THEN 1 
    ELSE 0 
  END AS alt_outlier_flag,

  CASE 
    WHEN gamma_GTP > 600 THEN 1 
    ELSE 0 
  END AS ggtp_outlier_flag

FROM smoking_drinking_features f;
```

---

### 3.2 Output (Summary)

Using the results from the executed script:

- Row count in `smoking_drinking_features_clean` = **same as original** (`~991k`)  
- Outlier flags capture rare extreme cases:
  - Weight < 30 or > 200  
  - SBP > 250  
  - DBP > 150  
  - BLDS < 40 or > 400  
  - AST > 1000  
  - ALT > 1000  
  - gamma_GTP > 600  

These extreme values match the upper ranges observed during the range checks in Section 2.

---

### 3.3 Conclusion

- All rows from Phase 1 are **preserved** to avoid information loss.  
- Outliers are **flagged, not deleted**, allowing:
  - filtering in downstream ML (Phase 4),  
  - sensitivity comparisons,  
  - or using the outlier flags as predictive features.  
- The cleaned table is now the **primary dataset** for all remaining phases.

---

## 4. Baseline Summaries (Overall Population)

This section presents descriptive statistics for the full dataset using  
`smoking_drinking_features_clean`. These metrics help establish baseline
health patterns before grouped analysis (Section 5).

---

### 4.1 Core Metrics (SBP, DBP, BLDS, BMI)

**Query**

```sql
SELECT
  AVG(SBP)  AS avg_SBP,
  AVG(DBP)  AS avg_DBP,
  AVG(BLDS) AS avg_BLDS,
  AVG(bmi)  AS avg_bmi,
  COUNT(*)  AS total_rows
FROM smoking_drinking_features_clean;
```

**Output (summary)**

- Avg SBP: **122.43**  
- Avg DBP: **76.05**  
- Avg BLDS: **100.42**  
- Avg BMI: **23.92**  
- Total rows: **~991,346**

**Conclusion:**  
The population shows slightly elevated BP, borderline-high fasting glucose,
and BMI near the normal/overweight boundary.

---

### 4.2 High Blood Pressure Prevalence

**Query**

```sql
SELECT
  SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) AS high_bp_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) / COUNT(*) AS high_bp_pct
FROM smoking_drinking_features_clean;
```

**Output**

- High BP Count: **127,196**  
- High BP %: **12.83%**

**Conclusion:**  
Approximately **1 in 8 adults** meet the hypertension threshold.

---

### 4.3 High Blood Sugar (Diabetic-Range) Prevalence

**Query**

```sql
SELECT
  SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) AS high_blds_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) / COUNT(*) AS high_blds_pct
FROM smoking_drinking_features_clean;
```

**Output**

- High BLDS Count: **76,199**  
- High BLDS %: **7.69%**

**Conclusion:**  
Diabetic-range fasting glucose affects around **7–8%** of the dataset.

---

### 4.4 Liver Risk Prevalence

**Query**

```sql
SELECT
  SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) AS liver_risk_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) / COUNT(*) AS liver_risk_pct
FROM smoking_drinking_features_clean;
```

**Output**

- Liver Risk Count: **208,208**  
- Liver Risk %: **21.00%**

**Conclusion:**  
Around **1 in 5 adults** show elevated liver enzyme values, aligning with lifestyle-linked risks.

---

## 5. Grouped Summaries (Lifestyle, Age Group, Sex)

This section breaks down key health metrics by demographic and lifestyle categories.  
All calculations use `smoking_drinking_features_clean`.

---

### 5.1 Lifestyle Group Counts

Lifestyle groups are derived as:

- `NS_ND` = Non-smoker, Non-drinker  
- `NS_D`  = Non-smoker, Drinker  
- `S_ND`  = Smoker, Non-drinker  
- `S_D`   = Smoker, Drinker  

**Query**

```sql
SELECT
  lifestyle_group,
  COUNT(*) AS people
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;
```

**Output**

| Group  | Count     |
|--------|-----------|
| NS_ND  | 443,481   |
| NS_D   | 333,911   |
| S_ND   | 52,377    |
| S_D    | 161,577   |

**Conclusion:**  
The population is dominated by non-smokers, but the S_D group is sizable enough for clear comparisons.

---

### 5.2 High Blood Pressure % by Lifestyle

**Query**

```sql
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) / COUNT(*) AS bp_high_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;
```

**Output**

| Group  | % High BP |
|--------|-----------|
| NS_ND  | 13.05%    |
| NS_D   | 12.56%    |
| S_ND   | 9.63%     |
| S_D    | 13.82%    |

**Conclusion:**  
Hypertension is highest in the **smoking + drinking** (S_D) group.

---

### 5.3 High BLDS % by Lifestyle

**Query**

```sql
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) / COUNT(*) AS blds_high_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;
```

**Output**

| Group  | % High BLDS |
|--------|-------------|
| NS_ND  | 7.60%       |
| NS_D   | 6.73%       |
| S_ND   | 9.83%       |
| S_D    | 9.20%       |

**Conclusion:**  
Blood sugar risk is clearly **higher among smokers**, regardless of drinking status.

---

### 5.4 Liver Risk % by Lifestyle

**Query**

```sql
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) / COUNT(*) AS liver_risk_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;
```

**Output**

| Group  | % Liver Risk |
|--------|--------------|
| NS_ND  | 13.40%       |
| NS_D   | 22.40%       |
| S_ND   | 23.08%       |
| S_D    | 38.31%       |

**Conclusion:**  
The S_D group shows **almost 3×** the liver abnormalities of NS_ND, confirming strong lifestyle effect.

---

### 5.5 Average BMI by Lifestyle

**Query**

```sql
SELECT
  lifestyle_group,
  AVG(bmi) AS avg_bmi
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;
```

**Output**

| Group  | Avg BMI |
|--------|---------|
| NS_ND  | 23.64   |
| NS_D   | 23.96   |
| S_ND   | 24.29   |
| S_D    | 24.48   |

**Conclusion:**  
BMI increases stepwise from NS_ND → S_D, aligning with escalating lifestyle risk patterns.
