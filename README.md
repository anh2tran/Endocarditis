# Endocarditis (work in progress)
This repository contains the cleaned SAS and R scripts used in the **infective endocarditis** project.

>  Please note: While raw code can also be found in the SNDS portal folders (`ANH_EI` and `ENDOCARDITE/ANH`), this GitHub repository contains the **most updated and cleaned versions**. Use this as your primary reference.

---

## Folder Structure on SNDS Portal

- **Code location:** `ANH_EI/` and `ENDOCARDITE/ANH/`  
  *(Raw and in-progress SAS codes – not fully cleaned)*
  
- **Final tables for analysis:**  
  `ENDOCARDITE/ANH/cohort_july/`  
  ---> Please use only the tables from this folder. **Ask before using any others.**

-**2 cohorts that are usually used: app_all_groups for matching and tab_DPDR_EI for identify events, I stock them in `ANH_EI/cohort_stock`
---

## Exposed Risk Groups

To create the 4 exposed groups, refer to the SAS scripts:

- `creer_groupe_1.sas`  
- `creer_groupe_2.sas`  
- `creer_groupe_3.sas`  
- `creer_groupe_4.sas`

These scripts generate exposure groups used in the pre-matching step (`avant_appariement`).

---

## Import from SAS to R (for Matching)

Because the matched data (exposed and non-exposed) is very large, it must be split before importing into R.

- The R import and matching scripts are based on **Stéphane Le Vu’s matching package**, with modifications to ensure:
  - Non-exposed subjects are **alive at least until the matching date**

**Key files:**

- `r_importer_donnees.R`
- `matching_code_Stephane.R`

---

## Cleaning and Finalizing Matched Sets

Once matched sets are exported from R:

- Apply additional eligibility criteria to non-exposed individuals
- Perform **random sampling** for balanced comparisons

Use the SAS script:  
- `random_sampling_matched_sets.sas`

---

## Defining the Endpoint

- The outcome of interest is defined using a SAS macro:
  - `macro_definir_evt` (included in `random_sampling_matched_sets.sas`)

### Special Note on Censoring:
- Non-exposed individuals can become exposed during follow-up. They are **censored at the moment of exposure**.
- We may also consider **censoring at end of study** to minimize informative censoring bias (TBD).
- Final outcome variables:
  - `endpoint_final`
  - `date_ep_final`
- Traceability variables:
  - `censored_to_expose`
  - `date_censored_to_expose`  
  *(available in `matched_set_*.sas7bdat`)*

---

## Cumulative Incidence Curves (CIF)

- Preliminary plots were generated in SAS.
- For publication-quality figures, **prefer R-based visualizations**.

---

## Hazard Ratios

- Cause-specific **Cox proportional hazards models** are used to estimate hazard ratios.
- See `hazard ratio_all.sas` for implementation.

---

## Variable Dictionary

A variable list is available for reference — useful when reading or merging datasets.

---

## Notes

- This repository focuses on cleaned, reproducible scripts.
- Contact me before using raw tables from outside the `cohort_july` folder.
