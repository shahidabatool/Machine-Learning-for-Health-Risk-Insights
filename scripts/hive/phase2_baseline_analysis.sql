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
-- (We will fill this section in the next steps.)
-- ------------------------------------------------------


-- ------------------------------------------------------
-- Section 3: Create cleaned features table
-- (CTAS / INSERT to build smoking_drinking_features_clean)
-- ------------------------------------------------------


-- ------------------------------------------------------
-- Section 4: Baseline summaries (overall population)
-- (Counts, averages, distributions of risk flags, BMI, etc.)
-- ------------------------------------------------------


-- ------------------------------------------------------
-- Section 5: Grouped summaries (lifestyle, age_group, sex)
-- ------------------------------------------------------
