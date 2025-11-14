-- ======================================================
-- Phase 2 - Baseline Analysis & Cleaning (Hive)
-- Project : DS8003 - Health Risk Analysis
-- Branch  : hive-work
-- Author  : Nimrah / Group 5
-- Purpose : Data quality checks, cleaning, and baseline
--           descriptive statistics using Hive.
-- ======================================================

USE health_risk_db;

-- ------------------------------------------------------
-- Section 1: Quick sanity checks (tables + row counts)
-- (You already ran these manually, but we keep them
--  here for reproducibility.)
-- ------------------------------------------------------

-- SELECT COUNT(*) AS cnt_raw FROM smoking_drinking_raw;
-- SELECT COUNT(*) AS cnt_features FROM smoking_drinking_features;

-- ------------------------------------------------------
-- Section 2: Data quality checks (NULLs, invalid values)
-- ------------------------------------------------------

-- 2.1 NULL / missing value checks (key columns)
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

-- 2.2 Category checks for smoking & drinking fields
SELECT DISTINCT SMK_stat_type_cd
FROM smoking_drinking_raw
ORDER BY SMK_stat_type_cd;

SELECT DISTINCT DRK_YN
FROM smoking_drinking_raw
ORDER BY DRK_YN;

-- 2.3 Range checks for key numeric columns
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

-- 2.4 Obvious outlier / invalid value counts (simple sanity rules)
SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN age < 20 OR age > 100 THEN 1 ELSE 0 END)           AS invalid_age,
  SUM(CASE WHEN height < 120 OR height > 220 THEN 1 ELSE 0 END)    AS invalid_height,
  SUM(CASE WHEN weight < 30 OR weight > 200 THEN 1 ELSE 0 END)     AS invalid_weight,
  SUM(CASE WHEN SBP < 60 OR SBP > 260 THEN 1 ELSE 0 END)           AS invalid_SBP,
  SUM(CASE WHEN DBP < 30 OR DBP > 150 THEN 1 ELSE 0 END)           AS invalid_DBP,
  SUM(CASE WHEN BLDS < 40 OR BLDS > 400 THEN 1 ELSE 0 END)         AS invalid_BLDS
FROM smoking_drinking_raw;


-- ------------------------------------------------------
-- Section 3: Create cleaned features table
-- (copy all features + add outlier flags)
-- ------------------------------------------------------

DROP TABLE IF EXISTS smoking_drinking_features_clean;

CREATE TABLE smoking_drinking_features_clean AS
SELECT
  f.*,  -- all existing columns from phase 1

  -- Outlier flags for weight, BP, sugar and liver enzymes
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



-- ------------------------------------------------------
-- Section 4: Baseline summaries (overall population)
-- (Counts, averages, distributions of risk flags, BMI, etc.)
-- ------------------------------------------------------

-- 4.1 Averages (SBP, DBP, BLDS, BMI)
SELECT
  AVG(SBP)  AS avg_SBP,
  AVG(DBP)  AS avg_DBP,
  AVG(BLDS) AS avg_BLDS,
  AVG(bmi)  AS avg_bmi,
  COUNT(*)  AS total_rows
FROM smoking_drinking_features_clean;

-- 4.2 High BP prevalence
SELECT
  SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) AS high_bp_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) / COUNT(*) AS high_bp_pct
FROM smoking_drinking_features_clean;

-- 4.3 High BLDS prevalence
SELECT
  SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) AS high_blds_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) / COUNT(*) AS high_blds_pct
FROM smoking_drinking_features_clean;

-- 4.4 Liver risk prevalence
SELECT
  SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) AS liver_risk_count,
  COUNT(*) AS total_rows,
  100.0 * SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) / COUNT(*) AS liver_risk_pct
FROM smoking_drinking_features_clean;


-- ------------------------------------------------------
-- Section 5: Grouped summaries (lifestyle, age_group, sex)
-- ------------------------------------------------------

-- 5.1 Lifestyle group counts
SELECT
  lifestyle_group,
  COUNT(*) AS people
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;

-- 5.2 High BP by lifestyle
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN bp_high_flag = 1 THEN 1 END) / COUNT(*) AS bp_high_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;

-- 5.3 High BLDS by lifestyle
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN blds_high_flag = 1 THEN 1 END) / COUNT(*) AS blds_high_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;

-- 5.4 Liver risk by lifestyle
SELECT
  lifestyle_group,
  100.0 * SUM(CASE WHEN liver_risk_flag = 1 THEN 1 END) / COUNT(*) AS liver_risk_pct
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;

-- 5.5 Average BMI by lifestyle
SELECT
  lifestyle_group,
  AVG(bmi) AS avg_bmi
FROM smoking_drinking_features_clean
GROUP BY lifestyle_group
ORDER BY lifestyle_group;

