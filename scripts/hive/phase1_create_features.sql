-- Phase 1: Create processed features table from raw data

USE health_risk_db;

-- Drop if re-running
DROP TABLE IF EXISTS smoking_drinking_features;

-- Create new table with raw + derived features
CREATE TABLE smoking_drinking_features AS
SELECT
  -- keep all original columns
  sex,
  age,
  height,
  weight,
  waistline,
  sight_left,
  sight_right,
  hear_left,
  hear_right,
  SBP,
  DBP,
  BLDS,
  tot_chole,
  HDL_chole,
  LDL_chole,
  triglyceride,
  hemoglobin,
  urine_protein,
  serum_creatinine,
  SGOT_AST,
  SGOT_ALT,
  gamma_GTP,
  SMK_stat_type_cd,
  DRK_YN,

  -- 1) BMI
  weight / POWER(height / 100.0, 2) AS bmi,

  -- 2) BMI category
  CASE
    WHEN weight / POWER(height / 100.0, 2) < 18.5 THEN 'UNDERWEIGHT'
    WHEN weight / POWER(height / 100.0, 2) < 25  THEN 'NORMAL'
    WHEN weight / POWER(height / 100.0, 2) < 30  THEN 'OVERWEIGHT'
    ELSE 'OBESE'
  END AS bmi_category,

  -- 3) Age group
  CASE
    WHEN age BETWEEN 20 AND 34 THEN '20-34'
    WHEN age BETWEEN 35 AND 49 THEN '35-49'
    WHEN age BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS age_group,

  -- 4) Blood pressure risk flag
  CASE
    WHEN SBP >= 140 OR DBP >= 90 THEN 1
    ELSE 0
  END AS bp_high_flag,

  -- 5) Blood sugar risk flag
  CASE
    WHEN BLDS >= 126 THEN 1
    ELSE 0
  END AS blds_high_flag,

  -- 6) Liver risk flag
  CASE
    WHEN SGOT_AST > 40 OR SGOT_ALT > 40 OR gamma_GTP > 60 THEN 1
    ELSE 0
  END AS liver_risk_flag,

  -- 7) Lifestyle group
  CASE
    WHEN (SMK_stat_type_cd IN (1, 2)) AND DRK_YN = 'N' THEN 'NS_ND'  -- non-smoker, non-drinker
    WHEN (SMK_stat_type_cd IN (1, 2)) AND DRK_YN = 'Y' THEN 'NS_D'   -- non-smoker, drinker
    WHEN (SMK_stat_type_cd = 3)        AND DRK_YN = 'N' THEN 'S_ND'  -- smoker, non-drinker
    WHEN (SMK_stat_type_cd = 3)        AND DRK_YN = 'Y' THEN 'S_D'   -- smoker, drinker
    ELSE 'UNKNOWN'
  END AS lifestyle_group

FROM smoking_drinking_raw;
