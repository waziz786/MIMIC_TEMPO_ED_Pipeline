# A Reproducible Dataset Assembly Pipeline for Clinical Deterioration Research in the Emergency Department Using MIMIC-IV

---

## Abstract

**Background.** Clinical deterioration among emergency department (ED) patients remains a leading cause of preventable morbidity and mortality. Early identification of patients at risk of adverse events — including unplanned intensive care unit (ICU) admission, initiation of vasopressors or mechanical ventilation, cardiac arrest, and death — is a central objective of acute care informatics. Despite growing interest in machine learning (ML) approaches for deterioration prediction, the lack of standardized, reproducible dataset construction pipelines has hindered progress, contributing to inconsistencies in outcome definitions, temporal leakage, and limited comparability across studies.

**Objective.** This paper presents a modular, end-to-end dataset assembly pipeline designed for clinical deterioration research using publicly available electronic health record (EHR) data. The pipeline transforms raw MIMIC-IV data into analysis-ready datasets with rigorously defined temporal windows, harmonized event logs, and configurable outcome labels, with the explicit goal of supporting reproducible downstream research.

**Methods.** The pipeline is implemented as a staged data processing system comprising six sequential phases: (1) base ED cohort construction from MIMIC-IV-ED, (2) clinical event extraction and harmonization across ICU, hospital, and diagnostic sources, (3) time-windowed outcome derivation with composite and component-level labels, (4) multi-resolution feature engineering across 1-hour, 6-hour, and 24-hour observation windows, (5) optional integration of machine-derived electrocardiogram (ECG) features from MIMIC-IV-ECG, and (6) configurable dataset materialization with automated quality assurance. The system is orchestrated through a Python framework with SQL-based data transformations and YAML-driven configuration.

**Results.** The pipeline produces a base cohort of adult ED visits with linked hospital admission records. It generates a unified event log spanning eight clinical event types, derives over 20 binary and time-to-event outcome variables across multiple time horizons (24-hour, 48-hour, 72-hour), and assembles feature baskets containing vital signs, laboratory values, demographic variables, process metrics, prior utilization history, and ECG measurements. Multiple analysis-ready datasets are materialized as CSV files, each defined by a specific feature window, outcome target, and cohort filter.

**Conclusion.** The described pipeline provides a transparent, configurable, and reproducible foundation for clinical deterioration research in the ED setting. By standardizing cohort construction, temporal alignment, and outcome labeling, it addresses critical gaps in current dataset practices and facilitates rigorous, comparable downstream modeling studies.

---

## 1. Introduction

Emergency departments serve as the primary point of entry for acutely ill patients, many of whom present with undifferentiated symptoms that may rapidly evolve into life-threatening conditions. Clinical deterioration — broadly defined as the progression to organ support requirements, critical care escalation, or death — represents a time-sensitive clinical trajectory in which early recognition and intervention can substantively alter outcomes [1, 2]. The identification of patients at risk of deterioration within the first hours of ED presentation has therefore become a central research objective in acute care informatics and clinical decision support [3].

Traditional approaches to deterioration detection have relied on aggregate physiological scoring systems, including the Modified Early Warning Score (MEWS), the National Early Warning Score (NEWS and NEWS2), and the Rapid Acute Physiological Score (RAPS) [4, 5]. These scoring systems aggregate bedside vital signs into composite indices designed to trigger clinical escalation protocols. While widely adopted in inpatient settings, their application in the ED context has demonstrated variable discriminative performance, particularly for heterogeneous patient populations and composite deterioration endpoints [6, 7]. A systematic review and meta-analysis by Guan et al. (2022) reported that the predictive accuracy of early warning scores in prehospital and ED settings varied considerably depending on the definition of deterioration and the patient subpopulation studied [8]. Covino et al. (2023) compared six early warning scores for predicting ICU admission and death in the ED and found that no single score consistently outperformed others across all outcome definitions [9].

In response to these limitations, there has been increasing interest in applying machine learning methods to ED deterioration prediction. ML-based models can incorporate higher-dimensional feature spaces, capture nonlinear interactions among clinical variables, and adapt to institution-specific data distributions [REF]. Several studies have demonstrated that models trained on structured EHR data — including vital signs, laboratory values, demographics, and clinical process metrics — can achieve moderate to high discriminative accuracy for outcomes such as ICU transfer, mechanical ventilation, and short-term mortality [REF]. More recently, multimodal approaches integrating electrocardiogram signals and clinical data have shown promise for identifying patients with cardiac-specific deterioration trajectories [10, 11].

However, the rapid growth of ML-based deterioration research has exposed a critical methodological challenge: the absence of standardized, reproducible dataset construction practices. The vast majority of published studies construct datasets through ad hoc processes that are difficult to replicate, insufficiently documented, and prone to subtle forms of data leakage [12, 13]. Kapoor and Narayanan (2023), in a widely cited analysis of reproducibility failures in ML-based science, identified temporal leakage — the inadvertent inclusion of future information in training features — as a pervasive and often undetected source of overoptimistic performance estimates [14]. In the clinical prediction context, temporal leakage can arise when outcome-adjacent variables (such as ICU transfer orders or post-admission laboratory results) are included in feature sets that are intended to represent information available at the time of prediction [15].

The challenge is compounded by inconsistencies in outcome definition. Studies of ED deterioration employ widely varying definitions of what constitutes an adverse outcome, with differences in the constituent events (e.g., whether vasopressor initiation or renal replacement therapy is included), the observation horizon (ranging from 6 hours to 30 days), and the handling of patients who deteriorate before versus after hospital admission [REF]. These definitional inconsistencies make cross-study comparison exceedingly difficult and limit the accumulation of evidence across research groups.

The availability of large, publicly accessible clinical databases has partially addressed data access barriers. The Medical Information Mart for Intensive Care (MIMIC) project, now in its fourth major version (MIMIC-IV), provides de-identified EHR data from a large academic medical center, including dedicated modules for ED visits (MIMIC-IV-ED), hospital admissions, ICU stays, and diagnostic electrocardiograms (MIMIC-IV-ECG) [16, 17, 18]. Several benchmarking efforts have been proposed for MIMIC-IV-ED data, including the work by Xie et al. (2022), who introduced a benchmark for ED prediction models with standardized preprocessing [19]. However, these efforts have generally focused on a limited set of prediction tasks and have not provided the end-to-end pipeline infrastructure needed for flexible, multi-outcome deterioration research.

This paper addresses the identified gaps by presenting a complete, modular dataset assembly pipeline for clinical deterioration research using MIMIC-IV. The contributions of this work are as follows:

First, the pipeline provides a **standardized cohort construction framework** that transforms raw MIMIC-IV ED, hospital, ICU, and ECG data into a unified analytic base with explicit temporal anchoring and admission linkage.

Second, it introduces a **harmonized clinical event log** that consolidates eight distinct clinical event types — ICU admission, vasopressor initiation, mechanical ventilation, renal replacement therapy, acute coronary syndromes, revascularization procedures, cardiac arrest, and death — into a single, temporally ordered structure with transparent provenance tracking.

Third, the pipeline implements a **configurable outcome derivation framework** that generates binary outcome labels and time-to-event variables across multiple time horizons (24-hour, 24–48-hour, and 72-hour windows), with both composite and component-level outcome definitions, including a novel ward failure endpoint capturing delayed ICU escalation.

Fourth, it provides a **multi-resolution feature engineering strategy** with three observation windows (1-hour, 6-hour, and 24-hour) that produce progressively richer feature sets while maintaining strict temporal boundaries to prevent label leakage.

Fifth, the pipeline includes **optional ECG feature integration** that links machine-derived electrocardiogram measurements from MIMIC-IV-ECG to ED visits, providing cardiac-specific covariates not available in standard structured EHR data.

Sixth, the system is designed for **extensibility and reproducibility**, with YAML-driven configuration, automated validation checks, and modular SQL-based transformations that can be adapted to alternative data sources or research questions.

The remainder of this paper is organized as follows. Section 2 reviews related work on early warning systems, ML-based deterioration prediction, and existing dataset construction practices. Section 3 provides a high-level overview of the pipeline architecture. Section 4 describes the materials and methods in detail. Section 5 characterizes the resulting datasets and discusses intended applications. Section 6 presents a discussion of strengths, limitations, and future directions.

---

## 2. Related Work

### 2.1 Early Warning Scores and Deterioration Prediction in the Emergency Department

The detection of clinical deterioration has historically relied on physiological early warning scores (EWS), which aggregate bedside vital signs — typically heart rate, respiratory rate, systolic blood pressure, oxygen saturation, temperature, and level of consciousness — into composite indices [4]. The National Early Warning Score (NEWS), developed by the Royal College of Physicians, and its updated version NEWS2, have been widely adopted in the United Kingdom and internationally as standardized tools for triggering clinical escalation [5]. The Modified Early Warning Score (MEWS) and the Rapid Acute Physiological Score (RAPS) represent alternative formulations with varying parameter selections and threshold definitions [REF].

Despite widespread adoption, the evidence for EWS performance in ED settings remains mixed. A narrative review by Panday et al. (2017) evaluated the prognostic value of early warning scores in the ED and acute medical unit, concluding that while EWS demonstrated moderate discriminative ability for mortality and ICU admission, their performance was inconsistent across patient populations and outcome definitions [20]. Spencer et al. (2019) conducted a comparative evaluation of multiple EWS in a single ED cohort and found that no score achieved consistently high sensitivity across all deterioration endpoints [21]. More recently, Guan et al. (2022) performed a systematic review and meta-analysis of EWS in prehospital and ED settings, reporting pooled areas under the receiver operating characteristic curve (AUROCs) ranging from 0.65 to 0.85 depending on the score, population, and outcome definition [8].

A fundamental limitation of aggregate scoring systems is their reliance on a small number of physiological variables, which constrains the information available for risk stratification. These scores were designed primarily for inpatient ward monitoring and may not adequately capture the dynamic, high-acuity nature of ED presentations, where rapid diagnostic workup, laboratory results, and electrocardiographic findings provide additional prognostic information [REF]. Furthermore, fixed scoring thresholds may not generalize across institutions with differing patient demographics and practice patterns [REF].

### 2.2 Machine Learning Approaches for Emergency Department Deterioration

The application of machine learning to ED deterioration prediction has grown substantially over the past decade. ML-based approaches offer the potential to incorporate higher-dimensional feature spaces, capture nonlinear interactions, and adapt to institution-specific distributions [REF]. Studies have applied logistic regression, gradient boosting, random forests, and deep learning architectures to predict outcomes including ICU admission, mechanical ventilation, vasopressor requirement, and short-term mortality using structured EHR data [REF].

Several studies have demonstrated that ML models can outperform traditional EWS for specific ED prediction tasks. Lee et al. (2024) developed a multimodal ML model for predicting in-hospital cardiac arrest using vital signs and EHR data, achieving improved performance over conventional early warning tools [22]. Feretzakis et al. (2024) applied automated machine learning (AutoML) to the MIMIC-IV-ED database for predicting patient disposition at triage, demonstrating the feasibility of ML-based decision support with minimal manual feature engineering [23]. Altintepe and Ozyoruk (2025) developed a 12-lead ECG-based deep learning model for predicting hospital admission among ED cardiac presentations using MIMIC-IV data [10].

Despite these advances, several methodological concerns have emerged. Many studies employ inconsistent outcome definitions, making cross-study comparisons difficult [REF]. Feature selection and preprocessing decisions are frequently underspecified, hindering reproducibility [REF]. Moreover, the temporal relationship between feature extraction and outcome observation is not always rigorously controlled, raising the possibility of information leakage that inflates apparent model performance [14, 15].

### 2.3 Public Clinical Datasets for Deterioration Research

The availability of large, de-identified clinical databases has been instrumental in enabling reproducible health informatics research. The MIMIC project, developed at the Massachusetts Institute of Technology and Beth Israel Deaconess Medical Center, has provided successive versions of openly accessible EHR data. MIMIC-IV, the current version, contains data from over 65,000 hospital admissions spanning 2008–2019, organized into modular components covering hospital-level data, ICU stays, the emergency department, and ancillary data including diagnostic ECGs [16, 17].

The MIMIC-IV-ED module provides structured data on over 400,000 ED visits, including triage assessments, vital sign measurements, medication administrations, and disposition information [17]. The MIMIC-IV-ECG module contains over 800,000 diagnostic electrocardiograms with machine-derived measurements, linked to patients in the clinical database via subject identifiers [18]. Together, these modules provide a rich data environment for constructing multi-domain deterioration datasets.

Several benchmarking initiatives have been proposed. Xie et al. (2022) introduced a benchmark for ED prediction models using MIMIC-IV-ED, covering tasks such as ED disposition, critical care admission, and hospitalization prediction [19]. Chen et al. (2023) proposed a multimodal clinical benchmark for emergency care (MC-BEC) that integrates structured data, clinical notes, and ECG signals from MIMIC-IV [24]. Partovi et al. (2022) developed MiPy, a framework for benchmarking ML models for hospital and ICU readmission prediction using MIMIC-IV [25].

While these efforts represent important steps toward standardized evaluation, they have generally focused on a specific set of prediction tasks and have not provided the comprehensive, configurable pipeline infrastructure needed for the broad range of deterioration research questions that arise in clinical practice.

### 2.4 Limitations of Prior Dataset Construction Practices

A critical gap in the current literature concerns the lack of transparency and rigor in dataset construction for clinical deterioration research. Several interrelated challenges can be identified.

The first challenge is **temporal leakage**. In the ED context, features must reflect information that would have been available at the time of clinical decision-making. Including post-decision variables — such as laboratory results ordered after a deterioration event, or vital signs recorded in the ICU — can artificially inflate model performance. Kapoor and Narayanan (2023) identified temporal leakage as one of the most prevalent sources of irreproducibility in ML-based science, noting that it is frequently difficult to detect from published methods sections alone [14]. Nestor et al. (2019) demonstrated that feature representations derived from EHR data are sensitive to temporal partitioning choices and that naive feature extraction can introduce subtle forms of leakage [15].

The second challenge is **outcome definition inconsistency**. There is no universally accepted definition of clinical deterioration in the ED setting. Some studies define deterioration as ICU admission alone; others include vasopressor use, ventilation, renal replacement therapy, or mortality. The time horizon for outcome assessment varies from 6 hours to 30 days, and the handling of patients who are directly admitted to the ICU from the ED (rather than deteriorating after ward admission) differs across studies [REF]. These inconsistencies limit the ability to compare results and accumulate evidence across research groups.

The third challenge is **reproducibility infrastructure**. Even when published studies use publicly available data such as MIMIC-IV, the dataset construction process is rarely provided as executable code with clear documentation. Ad hoc data extraction scripts, undocumented inclusion and exclusion criteria, and implicit assumptions about data structure all contribute to a situation in which nominally identical studies can produce substantively different datasets [REF].

The pipeline described in this paper is designed to address each of these challenges through a modular, configurable, and transparent approach to dataset construction, with explicit temporal controls, multiple outcome definitions, and automated validation.

---

## 3. System Overview and Pipeline Architecture

### 3.1 Design Philosophy

The dataset assembly pipeline is designed as a staged data processing system that transforms raw MIMIC-IV data into analysis-ready research datasets. The design is governed by four principles: (1) **modularity**, ensuring that each processing stage operates independently and can be modified without affecting other stages; (2) **temporal rigor**, enforcing strict time-window boundaries to prevent information leakage across feature extraction and outcome observation periods; (3) **configurability**, allowing researchers to adjust cohort criteria, feature windows, outcome definitions, and dataset specifications through external configuration files without modifying source code; and (4) **reproducibility**, providing deterministic SQL-based transformations with automated validation at each stage.

### 3.2 Pipeline Stages

The pipeline comprises six sequential stages, each producing a well-defined intermediate table that serves as input to subsequent stages. The overall architecture is illustrated below.

| Stage | Name | Input | Output | Description |
|-------|------|-------|--------|-------------|
| 1 | Base Cohort | MIMIC-IV-ED, MIMIC-IV Hospital | `base_ed_cohort` | Adult ED visits with demographic, temporal, and admission linkage data |
| 2 | Event Log | Base Cohort + ICU/Hospital data | `event_log` | Harmonized log of 8 clinical event types with unified temporal anchoring |
| 3 | Outcomes | Event Log + Base Cohort | `outcomes` | Binary outcome labels and time-to-event variables across multiple time horizons |
| 4 | Features | Base Cohort + ED/Hospital data | `features_w1`, `features_w6`, `features_w24` | Multi-resolution feature baskets with vital signs, labs, derived features, and missingness indicators |
| 5 | ECG Features | Base Cohort + MIMIC-IV-ECG | `ecg_features_w1`, `ecg_features_w6` | Machine-derived ECG measurements linked to ED visits within defined time windows |
| 6 | Datasets | All upstream tables | CSV files | Materialized, analysis-ready datasets with configurable outcome, feature window, and cohort filters |

### 3.3 Technology Stack

The pipeline is implemented using a hybrid SQL and Python architecture. Data transformations are expressed as parameterized SQL queries that operate directly within a PostgreSQL database containing the MIMIC-IV data. Python serves as the orchestration layer, responsible for configuration management, sequential stage execution, template rendering, logging, and automated validation. Configuration is managed through YAML files that specify database connection parameters, schema mappings, table names, cohort criteria, and pipeline behavior flags.

This architecture was chosen to leverage the expressive power and performance of SQL for relational data transformations while maintaining the flexibility and extensibility of Python for workflow orchestration and quality assurance. All SQL queries are parameterized through template placeholders (e.g., `{base_ed_cohort}`, `{ed_schema}`), enabling deployment across different database configurations without code modification.

### 3.4 Separation of Concerns

A deliberate architectural decision is the strict separation between data construction and downstream modeling. The pipeline produces analysis-ready datasets but does not perform any predictive modeling, hyperparameter tuning, or performance evaluation. This separation ensures that the dataset assembly process can be evaluated independently of any specific modeling approach, and that different research groups can apply the same standardized datasets to diverse analytical methods while maintaining comparability.

---

## 4. Materials and Methods

### 4.1 Data Sources

The pipeline draws upon four modules within the MIMIC-IV ecosystem, each providing distinct clinical data domains.

| Module | Description | Key Tables Used |
|--------|-------------|-----------------|
| **MIMIC-IV Hospital** (`mimiciv_hosp`) | Hospital-level data including admissions, patient demographics, laboratory events, diagnoses, and procedures | `patients`, `admissions`, `labevents`, `d_labitems`, `diagnoses_icd`, `d_icd_diagnoses`, `procedures_icd` |
| **MIMIC-IV ED** (`mimiciv_ed`) | Emergency department data including ED stays, triage assessments, vital signs, and medication dispensing | `edstays`, `triage`, `vitalsign`, `pyxis` |
| **MIMIC-IV ICU** (`mimiciv_icu`) | Intensive care unit data including ICU stays, input events, procedure events, and chart events | `icustays`, `inputevents`, `procedureevents`, `chartevents`, `d_items` |
| **MIMIC-IV ECG** (`mimiciv_ecg`) | Diagnostic electrocardiograms with machine-derived measurements | `record_list`, `machine_measurements` |

MIMIC-IV is a publicly available dataset sourced from the Beth Israel Deaconess Medical Center (BIDMC), a tertiary academic hospital in Boston, Massachusetts, United States. The database contains de-identified health records for patients admitted between 2008 and 2019 [16]. Access to the data requires completion of the Collaborative Institutional Training Initiative (CITI) program and a signed data use agreement through PhysioNet [REF].

### 4.2 Base ED Cohort Construction

The base cohort is constructed from the `edstays` table in MIMIC-IV-ED, which contains one record per ED visit. Each ED stay is linked to patient demographic information from the `patients` table in the hospital module via the `subject_id` identifier. The cohort construction process applies the following criteria and transformations.

Age at ED visit is computed using the MIMIC-IV anchor year methodology, which adjusts the recorded `anchor_age` by the difference between the year of the ED visit and the patient's `anchor_year`. Only adult patients (age ≥ 18 years at the time of ED presentation) are included. ED stays with missing or invalid timestamps (null `intime` or `outtime`, or `outtime` preceding `intime`) are excluded.

Each ED stay is augmented with the following attributes: ED length of stay in hours (computed from the difference between `outtime` and `intime`), an admission flag (`was_admitted`) indicating whether the ED visit is linked to a hospital admission via the `hadm_id` identifier, arrival transport mode, triage disposition, race and ethnicity, gender, and date of death (if applicable). Hospital admission linkage is performed via the `hadm_id` foreign key, which may be null for patients who are discharged directly from the ED without hospital admission.

The resulting base cohort table provides the temporal and relational foundation for all subsequent pipeline stages. Each row represents a unique ED stay identified by `stay_id`, with `ed_intime` serving as the primary temporal anchor for all downstream time-window calculations.

### 4.3 Event Log Construction

Clinical events are extracted from multiple source tables across the MIMIC-IV ICU, hospital, and diagnostic modules. Eight event types are defined, each implemented as a separate SQL extraction query that produces rows conforming to a unified seven-column schema: `subject_id`, `stay_id`, `hadm_id`, `event_type`, `event_time`, `source_table`, and `event_detail`. The event types and their extraction logic are summarized in Table 1.

**Table 1. Clinical Event Types and Extraction Logic**

| Event Type | Source Module | Extraction Method | Temporal Precision |
|------------|--------------|-------------------|-------------------|
| ICU Admission (`ICU_ADMIT`) | ICU (`icustays`) | Direct timestamp from ICU `intime` | Exact |
| Vasopressor Initiation (`PRESSOR_START`) | ICU (`inputevents`, `d_items`) | Earliest administration of norepinephrine, epinephrine, vasopressin, dopamine, phenylephrine, or dobutamine | Exact |
| Mechanical Ventilation (`VENT_START`) | ICU (`procedureevents`, `chartevents`) | Dual-source detection: procedure labels matching intubation/ventilation, or chart events for ventilator settings (vent mode, PEEP, tidal volume) | Exact |
| Renal Replacement Therapy (`RRT_START`) | ICU (`procedureevents`, `chartevents`) | Dual-source detection: procedure labels matching dialysis/CRRT/CVVH, or chart events for renal replacement parameters | Exact |
| Acute Coronary Syndrome (`ACS`) | Hospital (`diagnoses_icd`) | ICD-10 codes I21.x (STEMI), I22.x, I24.0, I20.0; ICD-9 codes 410.x, 411.1, 413.0 | Hospitalization-level (no timestamp) |
| Revascularization (`PCI`/`CABG`) | Hospital (`procedures_icd`) | PCI: ICD-10-PCS 0270x; ICD-9 00.66, 36.06–36.09. CABG: ICD-10-PCS 021x; ICD-9 36.1x | Hospitalization-level (no timestamp) |
| Cardiac Arrest (`CARDIAC_ARREST`) | Hospital (`diagnoses_icd`) | ICD-10 I46.x; ICD-9 427.5, 427.41, 427.42 | Hospitalization-level (no timestamp) |
| Death (`DEATH`) | Hospital (`admissions`) | `deathtime` from admissions table where deathtime ≥ ED arrival | Exact |

An important design consideration involves temporal precision. Events extracted from ICU-level data (ICU admission, vasopressors, ventilation, RRT) and hospital-level data (death) have exact timestamps derived from clinical documentation systems. Events identified through ICD diagnosis and procedure codes (ACS, revascularization, cardiac arrest) lack precise procedure timestamps in MIMIC-IV; these events are recorded with `NULL` event times and an `event_time_type` of `'none'`, indicating that they represent hospitalization-level occurrences rather than temporally precise events. This honest representation avoids fabricating false temporal precision by anchoring ICD-based events to arbitrary timepoints (e.g., ED arrival). The event log retains the `source_table` field and the new `event_time_type` column, enabling downstream users to distinguish between temporally precise and hospitalization-level events.

All event extractors apply a temporal filter requiring events to occur between ED arrival and seven days after ED discharge, capturing clinically relevant events during the index encounter while excluding unrelated future encounters. The individual event tables are harmonized into a unified event log through sequential insertion, with source tracking and indexing on `stay_id`, `event_type`, and `event_time`.

### 4.4 Outcome Definition Framework

Outcomes are derived from the harmonized event log through time-windowed aggregation. For each ED stay, the elapsed time between ED arrival (`ed_intime`) and each clinical event is computed in hours. Binary outcome labels are then generated by evaluating whether specific event types occurred within defined time windows. The outcome framework produces three categories of outcome variables.

**Component-level outcomes (24-hour window):** Individual binary indicators for each timed event type within 24 hours of ED arrival, including `icu_24h`, `pressor_24h`, `vent_24h`, `rrt_24h`, and `death_24h`. Extended windows (48-hour, 72-hour, and in-hospital) are also computed for selected event types.

**Hospitalization-level ICD outcomes:** Events identified through ICD codes — cardiac arrest, acute coronary syndrome, and revascularization — are represented as hospitalization-level binary indicators (`cardiac_arrest_hosp`, `acs_hosp`, `revasc_hosp`, `pci_hosp`, `cabg_hosp`) because MIMIC-IV does not provide precise procedure or diagnosis timestamps for these events. These outcomes indicate whether the event occurred at any point during the hospitalization linked to the ED visit, without asserting a specific temporal relationship to ED arrival.

**Composite deterioration outcomes:** The primary composite endpoint, `deterioration_24h`, is defined as the occurrence of any of the following within 24 hours of ED arrival: ICU admission, vasopressor initiation, mechanical ventilation, renal replacement therapy, or death. An analogous 48-hour composite is also computed. Notably, cardiac arrest is excluded from the time-windowed deterioration composites because it is an ICD-based event without a precise timestamp; it is instead available as a separate hospitalization-level indicator. This design ensures that composite outcomes maintain consistent temporal semantics.

**Ward failure outcome:** The `ward_failure_24_48h` endpoint captures a clinically distinct trajectory — patients who were not admitted to the ICU within the first 24 hours but were subsequently admitted between 24 and 48 hours. This outcome identifies cases of delayed escalation, in which patients initially appeared stable upon ward admission but subsequently required critical care. This endpoint is of particular clinical interest because it represents potentially preventable deterioration that current triage and monitoring systems may fail to detect.

**Coronary event outcomes (hospitalization-level):** The `coronary_event_hosp` composite captures acute coronary syndromes, percutaneous coronary intervention (PCI), and coronary artery bypass grafting (CABG) occurring during the hospitalization linked to the ED visit. Component-level coronary outcomes (`acs_hosp`, `revasc_hosp`, `pci_hosp`, `cabg_hosp`) are also provided. These are hospitalization-level indicators because ICD codes in MIMIC-IV do not carry procedure timestamps.

**Time-to-event variables:** For selected endpoints (ICU admission, death, and composite deterioration), the pipeline computes continuous time-to-event variables in hours from ED arrival, enabling survival analysis and time-dependent modeling approaches.

An essential design principle in the outcome framework is the prevention of label leakage. Outcome variables are derived exclusively from the event log, which is constructed independently of the feature engineering process. The temporal boundaries of outcome windows (e.g., 24 hours from ED arrival) are enforced at the SQL level, ensuring that the observation period for outcome assessment does not overlap with the feature extraction window in a manner that could introduce future information into the feature set.

### 4.5 Feature Engineering Strategy

Features are organized into three observation windows of increasing duration, each producing a distinct feature basket: W1 (first 1 hour from ED arrival), W6 (first 6 hours), and W24 (first 24 hours). This multi-resolution design reflects the clinical reality that information availability evolves over the course of an ED encounter — triage data and initial vital signs are available almost immediately, laboratory results typically become available within 1–6 hours, and extended observation metrics require longer dwell times.

**Table 2. Feature Baskets by Observation Window**

| Feature Category | W1 (1-hour) | W6 (6-hour) | W24 (24-hour) |
|------------------|:-----------:|:-----------:|:-------------:|
| **Demographics** | Age, gender, race, arrival transport | Age, gender, race, arrival transport | Age, gender, race, arrival transport |
| **Chief Complaint** | ✓ (raw text) | ✓ (raw text) | ✓ (raw text) |
| **Triage Assessment** | Acuity, pain score | — | — |
| **Vital Signs** | Single observation (triage/first charted) | Summary statistics (min, max, mean, std) | Summary statistics (min, max, mean, std) |
| **Laboratory Values** | — | First values (9 analytes) | First + max values (13 analytes) |
| **Derived Vitals** | Shock index, MAP | Coefficient of variation, HR range | Same as W6 |
| **Process Metrics** | — | ED LOS, time to first lab, time to first med | ED LOS, time to first lab, time to first med |
| **Prior Utilization** | — | Prior admits (1yr), prior ED visits (1yr) | Prior admits (1yr), prior ED visits (1yr) |
| **Missingness Indicators** | Temperature, HR, SBP | Lactate, troponin | Lactate, troponin |
| **ECG Features** (optional) | 15 variables | 15 variables | — |

**W1 (First-hour window).** The W1 feature basket captures the earliest available clinical information, consisting primarily of triage data and the first documented set of vital signs. Vital sign values are extracted using a fallback strategy: triage-recorded values are preferred, with first-charted ED vital signs used when triage data is missing. Derived features include the shock index (heart rate divided by systolic blood pressure) and mean arterial pressure (MAP), both of which have demonstrated prognostic value in acute care settings [REF]. Binary missingness indicators are generated for temperature, heart rate, and systolic blood pressure, as the absence of a vital sign measurement may itself be informatively associated with patient acuity [REF].

**W6 (Six-hour window).** The W6 feature basket extends the observation period to capture serial vital sign measurements and initial laboratory results. Vital signs are summarized using statistical aggregation functions — minimum, maximum, mean, and standard deviation — computed over all measurements recorded within the first six hours. Laboratory values include the first result for nine common analytes (lactate, troponin, creatinine, potassium, sodium, bicarbonate, white blood cell count, hemoglobin, and platelet count), with a flag indicating whether the troponin assay is high-sensitivity. ED process metrics capture timing variables (ED length of stay, time to first laboratory result, time to first medication administration), and prior utilization history counts hospital admissions and ED visits in the preceding year. Derived features include the coefficient of variation for systolic blood pressure and heart rate range, which capture hemodynamic variability that may not be apparent from mean values alone.

**W24 (Twenty-four-hour window).** The W24 feature basket provides the most comprehensive feature set, extending vital sign summaries over 24 hours and incorporating an expanded laboratory panel (adding glucose, blood urea nitrogen, bilirubin, and international normalized ratio). Additional derived laboratory features include the BUN-to-creatinine ratio and lactate delta (change from first to maximum value), which capture trajectory information that may be prognostically relevant.

All feature extraction queries enforce strict temporal boundaries through SQL `WHERE` clauses that restrict data to the specified window relative to the ED arrival time. Laboratory values are matched to ED stays via `hadm_id` linkage, with results restricted to the observation window to prevent incorporation of post-decision data.

### 4.6 ECG Feature Integration

The pipeline optionally integrates machine-derived electrocardiogram features from the MIMIC-IV-ECG module, which contains over 800,000 diagnostic 12-lead ECGs with automated measurements [18]. ECG features are extracted for two time windows: W1 (first ECG within 1 hour of ED arrival) and W6 (first ECG within 6 hours of ED arrival).

The ECG-to-ED linkage is performed by joining the `record_list` table (which contains `subject_id` and `ecg_time`) with the base ED cohort on `subject_id`. Candidate ECGs are identified as those with an `ecg_time` falling between ED arrival and the earlier of ED discharge or the window boundary. For each ED stay, the first qualifying ECG is selected using a deterministic ordering by `ecg_time`.

An important limitation of the ECG linkage is that the MIMIC-IV-ECG `record_list` table contains only `subject_id` and lacks `hadm_id` or `stay_id` fields. This means that ECGs cannot be directly attributed to a specific ED stay; instead, they are matched based on patient identity and temporal proximity. While this approach correctly identifies ECGs recorded during the ED encounter for the majority of patients, there is a theoretical possibility of misattribution for patients with concurrent or closely spaced encounters.

For each linked ECG, the following machine-derived measurements are extracted from the `machine_measurements` table:

**Table 3. ECG Feature Variables**

| Variable | Description | Unit |
|----------|-------------|------|
| `ecg_hr` | Heart rate derived from RR interval (60000 / RR interval) | bpm |
| `ecg_rr_interval` | RR interval (raw measurement) | ms |
| `ecg_qrs_dur` | QRS duration (qrs_end − qrs_onset) | ms |
| `ecg_pr` | PR interval (qrs_onset − p_onset) | ms |
| `ecg_qt` | QT interval proxy (t_end − qrs_onset) | ms |
| `ecg_p_onset`, `ecg_p_end` | P-wave timing boundaries | ms |
| `ecg_qrs_onset`, `ecg_qrs_end` | QRS complex timing boundaries | ms |
| `ecg_t_end` | T-wave offset | ms |
| `ecg_p_axis` | P-wave electrical axis | degrees |
| `ecg_qrs_axis` | QRS electrical axis | degrees |
| `ecg_t_axis` | T-wave electrical axis | degrees |
| `missing_ecg` | Binary indicator: 1 if no ECG available in window | — |

Each variable is suffixed with the window identifier (`_w1` or `_w6`). The derived timing intervals (QRS duration, PR interval, QT interval) are computed from the raw fiducial point measurements to ensure transparency in the derivation. A binary missingness indicator is included to facilitate analyses that account for the absence of ECG data.

ECG coverage varies across the cohort, as not all ED patients receive an electrocardiogram during their visit. The coverage rate is a function of clinical indication, institutional practice patterns, and the availability of ECGs in the MIMIC-IV-ECG module for the study period. Researchers should consider the potential for selection bias when incorporating ECG features, as patients who receive ECGs may differ systematically from those who do not.

### 4.7 Dataset Materialization

The final pipeline stage materializes analysis-ready datasets by joining the appropriate upstream tables and exporting the result as CSV files. Each dataset is defined by a specification that includes the feature window (W1, W6, or W24), the target outcome column, the cohort filter (all ED visits, admitted patients only, or non-admitted patients only), and whether to include ECG features.

The materialization process constructs a SQL query that joins the base ED cohort with the specified feature table, the outcome table, and optionally the ECG feature table. Cohort filters are applied as `WHERE` clauses (e.g., `was_admitted = 1` for admitted-only datasets). The target outcome column is aliased as `y` for consistent downstream use. Automatic missingness indicators are generated for features with null rates exceeding a configurable threshold (default: 10%).

The pipeline includes predefined dataset specifications covering the primary research questions, including early deterioration prediction (24-hour composite with W1 and W6 features), ward failure detection (24–48-hour delayed ICU admission with W6 features), coronary event prediction (72-hour composite with W6 features), and individual outcome endpoints (ICU admission, death, cardiac arrest, mechanical ventilation). Both ECG-inclusive and ECG-exclusive variants are provided for comparison analyses.

### 4.8 Validation and Quality Assurance

The pipeline incorporates automated validation at multiple levels to ensure data integrity and logical consistency.

**Temporal consistency checks** verify that no events in the event log precede the corresponding ED arrival time, that outcome time windows are monotonically ordered (e.g., any patient with `death_24h = 1` must also have `death_48h = 1` and `death_hosp = 1`), and that feature extraction timestamps fall within the specified observation windows.

**Row-count consistency checks** confirm that the base cohort, outcome table, and feature tables maintain a one-to-one relationship at the `stay_id` level, and that no stays are lost or duplicated during joins.

**Feature completeness checks** report the proportion of non-null values for core clinical variables (vital signs, laboratory values, ECG features), enabling researchers to assess data availability and missingness patterns before analysis.

**Dataset-level checks** evaluate the materialized datasets for completely missing features, constant features (which provide no discriminative information), and extreme outcome prevalence rates (below 1% or above 99%), which may indicate data quality issues or cohort definition problems.

All validation results are logged to timestamped log files, providing a complete audit trail of the pipeline execution. If intermediate validation is enabled in the configuration, each pipeline stage is validated before proceeding to the next, allowing early detection of data issues.

---

## 5. Dataset Characteristics and Applications

### 5.1 Cohort Composition

The base ED cohort comprises all adult (age ≥ 18) ED visits in the MIMIC-IV database with valid temporal boundaries. Each visit is characterized by demographic variables (age, gender, race), arrival mode (ambulance, walk-in, transfer), and triage disposition. The cohort includes both patients who are subsequently admitted to the hospital and those who are discharged directly from the ED, with an explicit admission flag enabling cohort stratification.

### 5.2 Event Rates and Outcome Prevalence

The unified event log captures clinical events across eight categories, providing the basis for outcome derivation. Event rates and outcome prevalence reflect the characteristics of a large urban academic medical center and may differ from other settings. The composite 24-hour deterioration endpoint aggregates multiple component events, and its prevalence is necessarily higher than that of any individual component. The ward failure endpoint (ICU admission at 24–48 hours without prior ICU admission in the first 24 hours) captures a relatively rare but clinically important trajectory. Coronary event outcomes (72-hour ACS, PCI, CABG) reflect the cardiac-specific subpopulation and are expected to have lower prevalence in an undifferentiated ED cohort.

### 5.3 Feature Availability Across Windows

Feature availability increases with window duration. The W1 feature basket provides near-complete coverage for triage vital signs, as triage assessment is a routine component of the ED workflow. The W6 and W24 feature baskets incorporate laboratory values, which are available only for patients who have blood drawn and results reported within the observation window. Vital sign measurement counts increase with window duration, providing richer information for statistical summarization. Missingness patterns may be informative — for example, the absence of a troponin measurement in the first six hours may reflect clinical judgment that a cardiac biomarker is not indicated, which is itself prognostically relevant.

### 5.4 ECG Coverage

ECG features are available for a subset of the cohort, as not all ED patients undergo electrocardiographic evaluation. The ECG coverage rate is expected to be higher for the W6 window than for W1, as the broader time window increases the probability of capturing an ECG recorded during the visit. The ECG missingness indicator (`missing_ecg_w1`, `missing_ecg_w6`) enables researchers to perform complete-case analyses, imputation-based analyses, or analyses that explicitly model ECG availability as a covariate.

### 5.5 Intended Applications

The materialized datasets are designed to support a range of downstream research applications, including but not limited to the following.

Development and validation of ML-based early warning models for composite and component-level deterioration endpoints, using progressively richer feature sets (W1, W6, W24) to evaluate the incremental value of additional observation time. Investigation of ward failure as a novel prediction target, with implications for post-admission monitoring and escalation protocols. Evaluation of coronary event prediction using structured clinical data and ECG features, supporting research in acute cardiac care informatics. Assessment of feature importance and interpretability in deterioration prediction, leveraging the transparent feature provenance provided by the pipeline. Methodological studies on the impact of observation window selection, outcome definition choices, and missingness handling strategies on model performance and clinical utility.

The pipeline does not prescribe specific modeling approaches, evaluation protocols, or reporting standards, leaving these decisions to downstream investigators. However, the temporal rigor of the dataset construction process ensures that features and outcomes are separated by design, reducing the risk of inadvertent leakage regardless of the analytical method employed.

---

## 6. Discussion

### 6.1 Strengths

The pipeline described in this paper addresses several critical gaps in the current landscape of clinical deterioration research. By providing a standardized, end-to-end dataset construction process with explicit temporal controls, the pipeline reduces the risk of subtle methodological errors — particularly temporal leakage — that have been identified as a pervasive source of overoptimistic results in clinical prediction studies [14]. The modular architecture allows individual pipeline stages to be independently modified, extended, or replaced, facilitating adaptation to alternative data sources or research questions. The multi-resolution feature engineering strategy (W1, W6, W24) enables systematic investigation of the trade-off between early prediction (with limited information) and later prediction (with richer data but reduced clinical actionability), a question of direct clinical relevance. The inclusion of a ward failure endpoint addresses an underexplored but clinically important outcome — delayed ICU escalation after initial ward admission — that may be particularly amenable to intervention through enhanced monitoring protocols.

The integration of machine-derived ECG features from MIMIC-IV-ECG represents an important extension beyond standard structured EHR variables. Electrocardiographic parameters such as QRS duration, QT interval, and axis deviations carry established prognostic significance in acute cardiac conditions [REF], and their incorporation into deterioration prediction models may capture physiological information not reflected in vital signs or laboratory values alone.

### 6.2 Importance of Temporal Rigor

The prevention of temporal leakage is a central design objective of the pipeline. Several architectural decisions serve this purpose. Feature extraction windows are defined with explicit time boundaries relative to the ED arrival time, implemented through SQL `WHERE` clauses that cannot be inadvertently overridden. Outcome labels are derived from a separate event log that is constructed independently of the feature tables. The use of multiple observation windows (1-hour, 6-hour, 24-hour) makes the temporal assumptions explicit, enabling researchers and reviewers to evaluate whether the assumed information availability is clinically realistic.

The treatment of ICD-based events (ACS, revascularization, cardiac arrest) as hospitalization-level outcomes with `NULL` event times represents a deliberate methodological choice. Rather than fabricating temporal precision by anchoring these events to the ED arrival time — which would create a false impression of temporal specificity — the pipeline honestly records that no timestamp is available. These events are consequently modeled as hospitalization-level binary indicators (e.g., `cardiac_arrest_hosp`, `acs_hosp`) and are excluded from time-windowed composites such as `deterioration_24h`. The event log retains the `source_table` and `event_time_type` columns, enabling downstream users to distinguish between temporally precise and hospitalization-level events and to apply their own analytical strategies accordingly.

### 6.3 Value for Interpretable Machine Learning Research

The transparent feature provenance provided by the pipeline — in which every feature can be traced to a specific source table, observation window, and aggregation method — supports research in interpretable and explainable ML for clinical decision support. Unlike opaque feature engineering processes that concatenate heterogeneous variables without clear documentation, the pipeline's structured approach enables clinicians to evaluate whether model-selected features are clinically plausible and whether their temporal provenance is consistent with the intended use case.

### 6.4 Limitations

Several limitations should be acknowledged. First, the pipeline operates on retrospective, observational data from a single academic medical center (Beth Israel Deaconess Medical Center). The resulting datasets reflect the patient demographics, clinical practices, and documentation patterns of this specific institution, and findings derived from these datasets may not generalize to other settings without external validation [REF].

Second, MIMIC-IV contains de-identified data with date-shifted timestamps, which precludes analyses that depend on absolute calendar dates or seasonal patterns. However, relative temporal relationships within an encounter (the basis for all pipeline time-window calculations) are preserved [16].

Third, the reliance on ICD codes for identifying acute coronary syndromes, revascularization procedures, and cardiac arrest introduces the possibility of coding inaccuracies and classification bias. ICD codes are assigned retrospectively and may not perfectly reflect the clinical timeline of events. Because MIMIC-IV does not provide timestamps for ICD-coded diagnoses and procedures, these events are modeled as hospitalization-level indicators rather than time-windowed outcomes, and they are excluded from composite time-windowed endpoints such as `deterioration_24h`. This approach sacrifices temporal granularity in favor of honest representation of the available data.

Fourth, ECG feature integration is limited by the `subject_id`-level linkage between MIMIC-IV-ECG and the clinical database. The absence of `hadm_id` or `stay_id` in the ECG `record_list` table means that ECGs are matched based on patient identity and temporal proximity rather than direct encounter-level linkage. While the temporal filtering reduces the risk of misattribution, it does not eliminate it entirely.

Fifth, the pipeline focuses exclusively on structured EHR data and machine-derived ECG measurements. It does not currently incorporate unstructured clinical text (nursing notes, physician documentation), raw ECG waveform data, or imaging data, all of which may contain additional prognostic information. Extension to multimodal data integration represents a direction for future work.

Sixth, the pipeline produces datasets at the ED-visit level, treating each visit as an independent observation. For patients with multiple ED visits during the study period, this approach does not account for within-patient correlation, which may need to be addressed in downstream modeling through clustered or hierarchical analytical methods.

### 6.5 Future Extensions

Several extensions to the pipeline are envisioned. Incorporation of clinical text features from MIMIC-IV-Note, using natural language processing methods to extract structured information from nursing and physician documentation, would provide additional clinical context not captured in structured data fields. Integration of raw ECG waveform data, processed through deep learning architectures, could complement the machine-derived ECG measurements currently included. Development of external validation pipelines using other publicly available databases (e.g., eICU Collaborative Research Database) would enable assessment of the generalizability of findings derived from MIMIC-IV data. Extension of the outcome framework to include longer-horizon endpoints (7-day, 30-day) and functional outcomes (length of stay, discharge disposition) would broaden the range of research questions that can be addressed.

---

## 7. Conclusion

This paper has presented a comprehensive, modular, and reproducible dataset assembly pipeline for clinical deterioration research using MIMIC-IV emergency department data. The pipeline addresses critical gaps in current dataset construction practices by providing standardized cohort definitions, harmonized event logs, configurable time-windowed outcome labels, multi-resolution feature engineering, optional ECG integration, and automated quality assurance. By separating data construction from downstream modeling and enforcing strict temporal boundaries, the pipeline reduces the risk of information leakage and supports rigorous, comparable research across investigator groups. The resulting datasets are intended to serve as a foundation for a broad range of research applications in acute care informatics, including early warning system development, ward failure detection, and cardiac-specific deterioration prediction. The pipeline code, configuration files, and documentation are provided to facilitate adoption, extension, and independent verification by the research community.

---

## References

[1] Guan G, Lee CMY, Begg S, Crombie A, Mnatzaganian G. The use of early warning system scores in prehospital and emergency department settings to predict clinical deterioration: A systematic review and meta-analysis. *PLoS ONE*. 2022;17(3):e0265559.

[2] Covino M, Sandroni C, Della Polla D, De Matteis G, et al. Predicting ICU admission and death in the Emergency Department: A comparison of six early warning scores. *Resuscitation*. 2023;188:109838.

[3] Panday RSN, Minderhoud TC, Alam N, Nannan Panday VS. Prognostic value of early warning scores in the emergency department (ED) and acute medical unit (AMU): a narrative review. *European Journal of Internal Medicine*. 2017;45:20–31.

[4] Royal College of Physicians. National Early Warning Score (NEWS) 2: Standardising the assessment of acute-illness severity in the NHS. London: RCP; 2017. [REF — verify edition/year]

[5] Alam N, Vegting IL, Houben E, van Berkel B, et al. Exploring the performance of the National Early Warning Score (NEWS) in a European emergency department. *Resuscitation*. 2015;90:111–115.

[6] Spencer W, Smith J, Date P, de Tonnerre E, et al. Determination of the best early warning scores to predict clinical outcomes of patients in the emergency department. *Emergency Medicine Journal*. 2019;36(12):716–721.

[7] Burgos-Esteban A, Gea-Caballero V, et al. Effectiveness of early warning scores for early severity assessment in outpatient emergency care: A systematic review. *Frontiers in Public Health*. 2022;10:894906.

[8] Guan G, Lee CMY, Begg S, Crombie A, Mnatzaganian G. The use of early warning system scores in prehospital and emergency department settings to predict clinical deterioration: A systematic review and meta-analysis. *PLoS ONE*. 2022;17(3):e0265559.

[9] Covino M, Sandroni C, Della Polla D, De Matteis G, et al. Predicting ICU admission and death in the Emergency Department: A comparison of six early warning scores. *Resuscitation*. 2023;188:109838.

[10] Altintepe A, Ozyoruk KB. 12-Lead Electrocardiogram–Based Deep-Learning Model for Hospital Admission Prediction in Emergency Department Cardiac Presentations: Retrospective Study. *JMIR Cardio*. 2025;9(1):e80569.

[11] Strodthoff N, Lopez Alcaraz JM, et al. Prospects for artificial intelligence-enhanced electrocardiogram as a unified screening tool for cardiac and non-cardiac conditions: an explorative study in emergency medicine. *European Heart Journal – Digital Health*. 2024;5(4):454–465.

[12] Guo LL, Pfohl SR, Fries J, Posada J, et al. Systematic review of approaches to preserve machine learning performance in the presence of temporal dataset shift in clinical medicine. *Applied Clinical Informatics*. 2021;12(4):808–815.

[13] Nestor B, McDermott MBA, Boag W, et al. Feature robustness in non-stationary health records: caveats to deployable model performance in common clinical machine learning tasks. *Proceedings of Machine Learning Research*. 2019;106:381–405.

[14] Kapoor S, Narayanan A. Leakage and the reproducibility crisis in machine-learning-based science. *Patterns*. 2023;4(9):100804.

[15] Nestor B, McDermott MBA, Boag W, et al. Feature robustness in non-stationary health records: caveats to deployable model performance in common clinical machine learning tasks. *Proceedings of Machine Learning Research*. 2019;106:381–405.

[16] Johnson AEW, Bulgarelli L, Shen L, Gayles A, et al. MIMIC-IV, a freely accessible electronic health record dataset. *Scientific Data*. 2023;10:1.

[17] Johnson A, Bulgarelli L, Pollard T, Celi LA, Mark R, Horng S. MIMIC-IV-ED. PhysioNet. 2021. Available at: https://physionet.org/content/mimic-iv-ed/

[18] Gow B, Pollard T, Nathanson LA, Johnson A, Moody B, et al. MIMIC-IV-ECG: Diagnostic Electrocardiogram Matched Subset. PhysioNet. 2023. Available at: https://physionet.org/content/mimic-iv-ecg/

[19] Xie F, Zhou J, Lee JW, Tan M, Li S, Rajnthern LSO, et al. Benchmarking emergency department prediction models with machine learning and public electronic health records. *Scientific Data*. 2022;9:658.

[20] Panday RSN, Minderhoud TC, Alam N, Nannan Panday VS. Prognostic value of early warning scores in the emergency department (ED) and acute medical unit (AMU): a narrative review. *European Journal of Internal Medicine*. 2017;45:20–31.

[21] Spencer W, Smith J, Date P, de Tonnerre E, et al. Determination of the best early warning scores to predict clinical outcomes of patients in the emergency department. *Emergency Medicine Journal*. 2019;36(12):716–721.

[22] Lee HY, Kuo PC, Qian F, Li CH, et al. Prediction of In-Hospital Cardiac Arrest in the Intensive Care Unit: Machine Learning-Based Multimodal Approach. *JMIR Medical Informatics*. 2024;12:e49142.

[23] Feretzakis G, Sakagianni A, Anastasiou A, et al. Machine learning in medical triage: A predictive model for emergency department disposition. *Applied Sciences*. 2024;14(15):6623.

[24] Chen E, Kansal A, Chen J, Jin BT, et al. Multimodal clinical benchmark for emergency care (MC-BEC): A comprehensive benchmark for evaluating foundation models in emergency medicine. *Advances in Neural Information Processing Systems*. 2023;36.

[25] Partovi A, Lukose D, Webb GI. MiPy: A Framework for Benchmarking Machine Learning Prediction of Unplanned Hospital and ICU Readmission in the MIMIC-IV Database. *Research Square* (preprint). 2022.

---

## Appendix A: Complete Feature Variable Listings

### A.1 W1 Feature Basket (First 1-Hour Window)

| Variable | Description | Type |
|----------|-------------|------|
| `stay_id` | ED stay identifier (key) | Integer |
| `age_at_ed` | Age at ED arrival | Continuous |
| `gender` | Patient gender | Categorical |
| `arrival_transport` | Arrival mode (ambulance, walk-in, etc.) | Categorical |
| `race` | Race/ethnicity | Categorical |
| `chiefcomplaint` | Chief complaint (raw text) | Text |
| `temp_w1` | Temperature (triage or first charted) | Continuous |
| `hr_w1` | Heart rate (triage or first charted) | Continuous |
| `rr_w1` | Respiratory rate (triage or first charted) | Continuous |
| `spo2_w1` | Oxygen saturation (triage or first charted) | Continuous |
| `sbp_w1` | Systolic blood pressure (triage or first charted) | Continuous |
| `dbp_w1` | Diastolic blood pressure (triage or first charted) | Continuous |
| `triage_pain` | Pain score at triage | Ordinal |
| `triage_acuity` | Triage acuity level (ESI) | Ordinal |
| `shock_index_w1` | Heart rate / systolic blood pressure | Continuous |
| `map_w1` | Mean arterial pressure: (SBP + 2×DBP) / 3 | Continuous |
| `missing_temp_w1` | Missing indicator for temperature | Binary |
| `missing_hr_w1` | Missing indicator for heart rate | Binary |
| `missing_sbp_w1` | Missing indicator for SBP | Binary |

### A.2 W6 Feature Basket (First 6-Hour Window)

| Variable | Description | Type |
|----------|-------------|------|
| `stay_id` | ED stay identifier (key) | Integer |
| `age_at_ed` | Age at ED arrival | Continuous |
| `gender` | Patient gender | Categorical |
| `arrival_transport` | Arrival mode | Categorical |
| `race` | Race/ethnicity | Categorical |
| `chiefcomplaint` | Chief complaint (raw text) | Text |
| `sbp_min_6h` | Minimum SBP in 6h | Continuous |
| `sbp_max_6h` | Maximum SBP in 6h | Continuous |
| `sbp_mean_6h` | Mean SBP in 6h | Continuous |
| `sbp_std_6h` | Standard deviation of SBP in 6h | Continuous |
| `dbp_min_6h` | Minimum DBP in 6h | Continuous |
| `hr_min_6h` | Minimum heart rate in 6h | Continuous |
| `hr_max_6h` | Maximum heart rate in 6h | Continuous |
| `hr_mean_6h` | Mean heart rate in 6h | Continuous |
| `hr_std_6h` | Standard deviation of heart rate in 6h | Continuous |
| `rr_max_6h` | Maximum respiratory rate in 6h | Continuous |
| `rr_mean_6h` | Mean respiratory rate in 6h | Continuous |
| `spo2_min_6h` | Minimum SpO2 in 6h | Continuous |
| `spo2_mean_6h` | Mean SpO2 in 6h | Continuous |
| `temp_max_6h` | Maximum temperature in 6h | Continuous |
| `temp_min_6h` | Minimum temperature in 6h | Continuous |
| `n_vitalsign_measurements_6h` | Count of vital sign observations | Integer |
| `lactate_first_6h` | First lactate value | Continuous |
| `troponin_first_6h` | First troponin value | Continuous |
| `is_hs_troponin_6h` | High-sensitivity troponin flag | Binary |
| `creatinine_first_6h` | First creatinine value | Continuous |
| `potassium_first_6h` | First potassium value | Continuous |
| `sodium_first_6h` | First sodium value | Continuous |
| `bicarbonate_first_6h` | First bicarbonate value | Continuous |
| `wbc_first_6h` | First white blood cell count | Continuous |
| `hemoglobin_first_6h` | First hemoglobin value | Continuous |
| `platelet_first_6h` | First platelet count | Continuous |
| `ed_los_hours` | ED length of stay (hours) | Continuous |
| `time_to_first_lab_hours` | Hours to first lab result | Continuous |
| `time_to_first_med_hours` | Hours to first medication | Continuous |
| `prev_admits_1yr` | Prior hospital admissions (1 year) | Integer |
| `prev_ed_visits_1yr` | Prior ED visits (1 year) | Integer |
| `sbp_cv_6h` | SBP coefficient of variation | Continuous |
| `hr_range_6h` | Heart rate range (max − min) | Continuous |
| `missing_lactate_6h` | Missing indicator for lactate | Binary |
| `missing_troponin_6h` | Missing indicator for troponin | Binary |

### A.3 W24 Feature Basket (First 24-Hour Window)

| Variable | Description | Type |
|----------|-------------|------|
| `stay_id` | ED stay identifier (key) | Integer |
| `age_at_ed` | Age at ED arrival | Continuous |
| `gender` | Patient gender | Categorical |
| `arrival_transport` | Arrival mode | Categorical |
| `race` | Race/ethnicity | Categorical |
| `chiefcomplaint` | Chief complaint (raw text) | Text |
| `sbp_min_24h` | Minimum SBP in 24h | Continuous |
| `sbp_max_24h` | Maximum SBP in 24h | Continuous |
| `sbp_mean_24h` | Mean SBP in 24h | Continuous |
| `sbp_std_24h` | Standard deviation of SBP in 24h | Continuous |
| `hr_min_24h` | Minimum heart rate in 24h | Continuous |
| `hr_max_24h` | Maximum heart rate in 24h | Continuous |
| `hr_mean_24h` | Mean heart rate in 24h | Continuous |
| `rr_max_24h` | Maximum respiratory rate in 24h | Continuous |
| `rr_mean_24h` | Mean respiratory rate in 24h | Continuous |
| `spo2_min_24h` | Minimum SpO2 in 24h | Continuous |
| `spo2_mean_24h` | Mean SpO2 in 24h | Continuous |
| `temp_max_24h` | Maximum temperature in 24h | Continuous |
| `n_vitalsign_measurements_24h` | Count of vital sign observations | Integer |
| `lactate_first_24h` | First lactate value | Continuous |
| `troponin_first_24h` | First troponin value | Continuous |
| `is_hs_troponin_24h` | High-sensitivity troponin flag | Binary |
| `creatinine_first_24h` | First creatinine value | Continuous |
| `potassium_first_24h` | First potassium value | Continuous |
| `sodium_first_24h` | First sodium value | Continuous |
| `bicarbonate_first_24h` | First bicarbonate value | Continuous |
| `wbc_first_24h` | First WBC count | Continuous |
| `hemoglobin_first_24h` | First hemoglobin value | Continuous |
| `platelet_first_24h` | First platelet count | Continuous |
| `glucose_first_24h` | First glucose value | Continuous |
| `bun_first_24h` | First BUN value | Continuous |
| `bilirubin_first_24h` | First bilirubin value | Continuous |
| `inr_first_24h` | First INR value | Continuous |
| `lactate_max_24h` | Maximum lactate in 24h | Continuous |
| `creatinine_max_24h` | Maximum creatinine in 24h | Continuous |
| `ed_los_hours` | ED length of stay (hours) | Continuous |
| `time_to_first_lab_hours` | Hours to first lab result | Continuous |
| `time_to_first_med_hours` | Hours to first medication | Continuous |
| `prev_admits_1yr` | Prior hospital admissions (1 year) | Integer |
| `prev_ed_visits_1yr` | Prior ED visits (1 year) | Integer |
| `bun_creatinine_ratio` | BUN / creatinine ratio | Continuous |
| `lactate_delta_24h` | Lactate change (max − first) | Continuous |

### A.4 ECG Features (W1 and W6 Windows)

| Variable (W1 suffix `_w1`, W6 suffix `_w6`) | Description | Unit |
|----------------------------------------------|-------------|------|
| `ecg_study_id` | ECG study identifier | Integer |
| `ecg_time` | Timestamp of ECG recording | Timestamp |
| `ecg_hours_from_ed` | Hours from ED arrival to ECG | Continuous |
| `ecg_hr` | ECG-derived heart rate | bpm |
| `ecg_rr_interval` | RR interval | ms |
| `ecg_qrs_dur` | QRS duration | ms |
| `ecg_pr` | PR interval | ms |
| `ecg_qt` | QT interval proxy | ms |
| `ecg_p_onset` | P-wave onset timing | ms |
| `ecg_p_end` | P-wave end timing | ms |
| `ecg_qrs_onset` | QRS onset timing | ms |
| `ecg_qrs_end` | QRS end timing | ms |
| `ecg_t_end` | T-wave end timing | ms |
| `ecg_p_axis` | P-wave axis | degrees |
| `ecg_qrs_axis` | QRS axis | degrees |
| `ecg_t_axis` | T-wave axis | degrees |
| `missing_ecg` | No ECG available in window | Binary |

---

## Appendix B: Outcome Variable Listing

| Outcome Variable | Definition | Time Horizon |
|------------------|------------|-------------|
| `icu_24h` | ICU admission within 24h of ED arrival | 24h |
| `icu_48h` | ICU admission within 48h of ED arrival | 48h |
| `pressor_24h` | Vasopressor initiation within 24h | 24h |
| `vent_24h` | Mechanical ventilation within 24h | 24h |
| `rrt_24h` | Renal replacement therapy within 24h | 24h |
| `cardiac_arrest_hosp` | Cardiac arrest during hospitalization (ICD-based) | Hospitalization |
| `death_24h` | Death within 24h | 24h |
| `death_48h` | Death within 48h | 48h |
| `death_72h` | Death within 72h | 72h |
| `death_hosp` | In-hospital death (any time) | Hospitalization |
| `deterioration_24h` | Composite: ICU ∨ pressors ∨ vent ∨ RRT ∨ death within 24h | 24h |
| `deterioration_48h` | Composite: as above, extended to 48h | 48h |
| `ward_failure_24_48h` | ICU admission at 24–48h, NOT admitted to ICU within first 24h | 24–48h |
| `acs_hosp` | Acute coronary syndrome during hospitalization (ICD-based) | Hospitalization |
| `revasc_hosp` | Revascularization (PCI or CABG) during hospitalization (ICD-based) | Hospitalization |
| `pci_hosp` | Percutaneous coronary intervention during hospitalization (ICD-based) | Hospitalization |
| `cabg_hosp` | Coronary artery bypass grafting during hospitalization (ICD-based) | Hospitalization |
| `coronary_event_hosp` | Composite: ACS ∨ PCI ∨ CABG during hospitalization (ICD-based) | Hospitalization |
| `time_to_icu` | Hours from ED arrival to first ICU admission | Continuous |
| `time_to_death` | Hours from ED arrival to death | Continuous |
| `time_to_deterioration` | Hours from ED arrival to first deterioration event | Continuous |

---

## Appendix C: Predefined Dataset Specifications

| Dataset Name | Feature Window | Outcome | Cohort | ECG |
|-------------|:-:|-----------|:------:|:---:|
| `ed_w6_det24_admitted` | W6 | `deterioration_24h` | Admitted | ✓ |
| `ed_w6_det24_all` | W6 | `deterioration_24h` | All | ✓ |
| `ed_w1_det24_admitted` | W1 | `deterioration_24h` | Admitted | ✓ |
| `ed_w6_ward_failure_admitted` | W6 | `ward_failure_24_48h` | Admitted | ✓ |
| `ed_w6_coronary_hosp_admitted` | W6 | `coronary_event_hosp` | Admitted | ✓ |
| `ed_w6_acs_hosp_admitted` | W6 | `acs_hosp` | Admitted | ✓ |
| `ed_w6_revasc_hosp_admitted` | W6 | `revasc_hosp` | Admitted | ✓ |
| `ed_w6_icu24_admitted` | W6 | `icu_24h` | Admitted | ✓ |
| `ed_w6_death24_admitted` | W6 | `death_24h` | Admitted | ✓ |
| `ed_w6_cardiac_arrest_admitted` | W6 | `cardiac_arrest_hosp` | Admitted | ✓ |
| `ed_w6_vent24_admitted` | W6 | `vent_24h` | Admitted | ✓ |
| `ed_w6_det24_no_ecg` | W6 | `deterioration_24h` | Admitted | ✗ |
| `ed_w24_det48_admitted` | W24 | `deterioration_48h` | Admitted | ✗ |

---

## Appendix D: Citation Verification Notes

The following references were verified via Google Scholar and PubMed searches. Statements marked with `[REF]` in the text body require additional literature support that the authors should verify and insert manually before submission. The verified references are listed in the References section above. The following claims remain marked for additional citation:

1. Section 1 — ML models incorporating higher-dimensional feature spaces for ED deterioration prediction (general claim) → `[REF]`
2. Section 1 — Studies demonstrating moderate to high AUC for structured EHR-based deterioration models → `[REF]`
3. Section 2.1 — MEWS and RAPS original definitions and validation → `[REF]`
4. Section 2.1 — EWS limitations regarding fixed thresholds and institutional generalizability → `[REF]`
5. Section 2.2 — General ML approaches outperforming EWS in ED settings → `[REF]`
6. Section 2.2 — Feature selection underspecification in published studies → `[REF]`
7. Section 2.4 — Inconsistencies in deterioration outcome definitions across studies → `[REF]`
8. Section 2.4 — Reproducibility infrastructure gaps in EHR-based ML studies → `[REF]`
9. Section 4.1 — CITI program and PhysioNet data use agreement → `[REF]`
10. Section 4.5 — Shock index prognostic value in acute care → `[REF]`
11. Section 4.5 — Informative missingness in clinical data → `[REF]`
12. Section 6.1 — ECG parameters (QRS, QT, axis) prognostic significance → `[REF]`
13. Section 6.4 — Single-center generalizability limitation → `[REF]`
