# Derived Features Design – Health Risk Project

This document defines the derived features engineered from the raw smoking & drinking dataset.  
Each feature was reasoned from exploratory analysis in Hive and validated with population-level queries.  
They will be implemented as computed columns or Hive views and later reused in Spark modelling.

---

## 1. BMI (Body Mass Index)

**Name:** `bmi`  
**Inputs:** `weight` (kg), `height` (cm) **Type:** DOUBLE  

**Formula:**  
\[
\text{bmi} = \frac{\text{weight}}{(\text{height}/100)^2}
\]

**Insight:**  
Provides a normalized body-size indicator; most values cluster around 22–25 kg/m² in the dataset.

---

## 2. BMI Category

**Name:** `bmi_category` **Input:** `bmi` **Type:** STRING  

| Category | Range | Meaning |
|-----------|--------|---------|
| `UNDERWEIGHT` | < 18.5 | Low body mass |
| `NORMAL` | 18.5 – 24.9 | Healthy range |
| `OVERWEIGHT` | 25 – 29.9 | Elevated risk |
| `OBESE` | ≥ 30 | High obesity risk |

**Insight:** Facilitates visual comparison of lifestyle groups by weight status.

---

## 3. Age Group

**Name:** `age_group` **Input:** `age` **Type:** STRING  

| Group | Range (yrs) | Interpretation |
|--------|--------------|----------------|
| `20-34` | Early adulthood | Low chronic-risk phase |
| `35-49` | Mid adulthood | Rising risk period |
| `50-64` | Late adulthood | Peak onset of chronic issues |
| `65+` | Senior | Highest-risk segment |

**Insight:** These bins reflect life-stage health pattern shifts observed in the data.

---

## 4. Blood Pressure Risk Flag

**Name:** `bp_high_flag` **Inputs:** `SBP`, `DBP` **Type:** INT (0/1)  

**Rule:** `1` if SBP ≥ 140 or DBP ≥ 90; else `0`  

**Result:** ≈ 12.8 % of records flagged — consistent with adult hypertension prevalence.

---

## 5. Blood Sugar Risk Flag

**Name:** `blds_high_flag` **Input:** `BLDS` **Type:** INT (0/1)  

**Rule:** `1` if BLDS ≥ 126 mg/dL; else `0`  

**Result:** ≈ 7.4 % of population exceed threshold — realistic diabetes-risk rate.

---

## 6. Liver Risk Flag

**Name:** `liver_risk_flag` **Inputs:** `SGOT_AST`, `SGOT_ALT`, `gamma_GTP` **Type:** INT (0/1)  

**Rule:** `1` if (AST > 40 or ALT > 40 or gamma_GTP > 60); else `0`  

**Result:** ≈ 21 % of population flagged — plausible given drinker proportion and enzyme variability.

---

## 7. Lifestyle Group

**Name:** `lifestyle_group` **Inputs:** `SMK_stat_type_cd`, `DRK_YN` **Type:** STRING  

| Code | Meaning | % of Dataset |
|-------|----------|--------------|
| `NS_ND` | Non-smoker & Non-drinker | 44.7 % |
| `NS_D`  | Non-smoker & Drinker | 33.7 % |
| `S_D`   | Smoker & Drinker | 16.3 % |
| `S_ND`  | Smoker & Non-drinker |  5.3 % |

**Insight:** Provides a behavioural segmentation useful for comparing health-risk patterns.

---

**Summary:**  
All derived features were explored empirically in Hive and found to yield realistic population distributions.  
They form the foundation for subsequent MapReduce and Spark analyses in Phase 2–4.
