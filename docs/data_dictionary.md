# Data Dictionary – Smoking & Drinking Dataset with Body Signals

## 1. Demographic & Body Size

| Column Name | Category   | Variable Type | Hive Type | Description                                      |
|------------|------------|---------------|-----------|--------------------------------------------------|
| sex        | Demographic| Categorical   | STRING    | Biological sex of the individual (Male/Female). |
| age        | Demographic| Numeric (int) | INT       | Age in years (typically 20–85).                 |
| height     | Body size  | Numeric (int) | INT       | Height in cm.                                   |
| weight     | Body size  | Numeric       | DOUBLE    | Weight in kg.                                   |
| waistline  | Body size  | Numeric       | DOUBLE    | Waist circumference in cm.                      |

## 2. Sensory Measures

| Column Name | Category | Variable Type | Hive Type | Description                         |
|------------|----------|---------------|-----------|-------------------------------------|
| sight_left | Sensory  | Numeric       | DOUBLE    | Left eye visual acuity (decimal).   |
| sight_right| Sensory  | Numeric       | DOUBLE    | Right eye visual acuity (decimal).  |
| hear_left  | Sensory  | Categorical   | INT       | Left ear hearing status (1/2).      |
| hear_right | Sensory  | Categorical   | INT       | Right ear hearing status (1/2).     |

## 3. Blood Pressure & Cardiovascular

| Column Name | Category       | Variable Type | Hive Type | Description                              |
|------------|----------------|---------------|-----------|------------------------------------------|
| SBP        | Cardiovascular | Numeric       | INT       | Systolic blood pressure (mmHg).          |
| DBP        | Cardiovascular | Numeric       | INT       | Diastolic blood pressure (mmHg).         |

## 4. Metabolic & Lipid Profile

| Column Name   | Category | Variable Type | Hive Type | Description                     |
|---------------|----------|---------------|-----------|---------------------------------|
| BLDS          | Metabolic| Numeric       | INT       | Fasting blood sugar (mg/dL).    |
| tot_chole     | Lipid    | Numeric       | INT       | Total cholesterol (mg/dL).      |
| HDL_chole     | Lipid    | Numeric       | INT       | HDL cholesterol (mg/dL).        |
| LDL_chole     | Lipid    | Numeric       | INT       | LDL cholesterol (mg/dL).        |
| triglyceride  | Lipid    | Numeric       | INT       | Triglycerides (mg/dL).          |
| hemoglobin    | Blood    | Numeric       | DOUBLE    | Hemoglobin (g/dL).              |

## 5. Kidney & Liver Function

| Column Name      | Category | Variable Type | Hive Type | Description                                   |
|------------------|----------|---------------|-----------|-----------------------------------------------|
| urine_protein    | Kidney   | Categorical   | INT       | Urine protein test level (ordinal code).      |
| serum_creatinine | Kidney   | Numeric       | DOUBLE    | Serum creatinine (mg/dL).                     |
| SGOT_AST         | Liver    | Numeric       | INT       | AST liver enzyme (IU/L).                      |
| SGOT_ALT         | Liver    | Numeric       | INT       | ALT liver enzyme (IU/L).                      |
| gamma_GTP        | Liver    | Numeric       | INT       | Gamma-GTP liver enzyme (IU/L).                |

## 6. Lifestyle Variables

| Column Name       | Category  | Variable Type | Hive Type | Description                                  |
|-------------------|-----------|---------------|-----------|----------------------------------------------|
| SMK_stat_type_cd  | Lifestyle | Categorical   | INT       | Smoking status code (never / ex / current).  |
| DRK_YN            | Lifestyle | Categorical   | STRING    | Drinking flag (`Y` = drinks, `N` = no drink).|
