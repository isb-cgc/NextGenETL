
# these tables all merge because there's only one record, but actual script
# should account for the possibility of multiple diagnoses
clinical_sql = f"""
    SELECT * 
    FROM `isb-project-zero.cda_icdc_raw.2025_03_case`
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_clinical_study_designation`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_cohort_id`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_cohort`
        USING(cohort_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_demographic`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_diagnosis_id`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_diagnosis`
        USING(diagnosis_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_enrollment_id`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_enrollment`
        USING(enrollment_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_canine_individual`
        USING(case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_prior_surgery`
        USING(enrollment_id)
"""

# note--this merging does create duplicate visit rows
visit_sql = f"""
    SELECT * 
    FROM `isb-project-zero.cda_icdc_raw.2025_03_visit`
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_visit_id`
      USING(visit_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_disease_extent`
      USING(visit_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_physical_exam`
      USING(visit_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_vital_signs`
      USING(visit_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_cycle_case_id_and_visit_id`
      USING(visit_id)
"""

merged_study_sql = f"""
    SELECT * 
    FROM `isb-project-zero.cda_icdc_raw.2025_03_program`
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_program_clinical_study_designation`
        USING(program_acronym)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_study`
        USING(clinical_study_designation)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_study_site`
        USING(clinical_study_designation)
"""

merged_cohort_sql = f"""
    SELECT * 
    FROM `isb-project-zero.cda_icdc_raw.2025_03_cohort`
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_study_arm_cohort_id`
      USING (cohort_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_study_arm` 
      USING (arm_id)
"""

per_sample_file_sql = f"""
    SELECT sf.file_uuid, sc.case_id, s.*, f.*
    FROM `isb-project-zero.cda_icdc_raw.2025_03_sample` s
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_sample_file_uuid` sf
      USING (sample_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_file` f
      ON sf.file_uuid = f.uuid
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_sample_case_id` sc 
      USING (sample_id)
"""

case_metadata_sql = f"""
    WITH file_counts AS (
      SELECT case_id, count(file_uuid) AS file_count
      FROM `isb-project-zero.cda_icdc_raw.2025_03_case_file_uuid`
      GROUP BY case_id
    )
    
    SELECT c.case_id, pcsd.program_acronym, p.program_name, csd.clinical_study_designation, s.clinical_study_id, s.clinical_study_name, s.accession_id, COALESCE(fc.file_count, 0) AS file_count
    FROM `isb-project-zero.cda_icdc_raw.2025_03_case` c
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_case_clinical_study_designation` csd
      USING (case_id)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_program_clinical_study_designation` pcsd
      USING (clinical_study_designation)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_program` p
      USING (program_acronym)
    LEFT JOIN `isb-project-zero.cda_icdc_raw.2025_03_study` s
      USING (clinical_study_designation)
    LEFT JOIN file_counts fc
      USING (case_id)
"""