-- Phase 1: Hive schema for Smoking & Drinking Dataset with Body Signals
-- Location in HDFS: /data/health_risk/smoking_driking_dataset.csv

-- 1. Create database (if not already there)
CREATE DATABASE IF NOT EXISTS health_risk_db;

USE health_risk_db;

-- 2. Drop old table if re-running script
DROP TABLE IF EXISTS smoking_drinking_raw;

-- 3. Create external table over the CSV in HDFS
CREATE EXTERNAL TABLE smoking_drinking_raw (
  -- NOTE: If your CSV has an 'id' column as the first column,
  -- you can add this line back:
  -- id INT,

  sex            STRING,   -- biological sex (Male/Female or coded)
  age            INT,      -- age in years
  height         INT,      -- height in cm
  weight         DOUBLE,   -- weight in kg
  waistline      DOUBLE,   -- waist circumference in cm

  sight_left     DOUBLE,   -- left eye visual acuity
  sight_right    DOUBLE,   -- right eye visual acuity
  hear_left      INT,      -- 1 = normal, 2 = abnormal
  hear_right     INT,      -- 1 = normal, 2 = abnormal

  SBP            INT,      -- systolic blood pressure (mmHg)
  DBP            INT,      -- diastolic blood pressure (mmHg)

  BLDS           INT,      -- fasting blood sugar (mg/dL)
  tot_chole      INT,      -- total cholesterol (mg/dL)
  HDL_chole      INT,      -- HDL cholesterol (mg/dL)
  LDL_chole      INT,      -- LDL cholesterol (mg/dL)
  triglyceride   INT,      -- triglycerides (mg/dL)
  hemoglobin     DOUBLE,   -- hemoglobin (g/dL)

  urine_protein  INT,      -- urine protein level (ordinal)
  serum_creatinine DOUBLE, -- serum creatinine (mg/dL)

  SGOT_AST       INT,      -- liver enzyme AST (IU/L)
  SGOT_ALT       INT,      -- liver enzyme ALT (IU/L)
  gamma_GTP      INT,      -- liver enzyme gamma-GTP (IU/L)

  SMK_stat_type_cd INT,    -- smoking status code
  DRK_YN         STRING    -- drinking flag: 'Y' or 'N'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/health_risk'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);
