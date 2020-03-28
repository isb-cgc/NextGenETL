"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import sys
import json
from common_etl.utils import get_programs_from_bq, load_config, has_fatal_error, pprint_json


def output_clinical_data_stats(clinical_data_fp, api_params):
    counts = {
        'total': 0,
        'no_clinical_fgs': 0
    }

    programs_with_field_group = {
        'none': set()
    }

    no_fg_case_barcodes = {}

    field_groups = api_params['EXPAND_FIELD_GROUPS'].split(',')

    for fg in field_groups:
        counts[fg] = 0
        programs_with_field_group[fg] = set()

    program_lookup_dict = get_programs_from_bq()
    print(program_lookup_dict)

    with open(clinical_data_fp, 'r') as file:
        for line in file:
            if counts['total'] % 100 == 0:
                print(counts['total'])
            counts['total'] += 1

            json_line = json.loads(line)
            program_name = program_lookup_dict[json_line['submitter_id']]

            if 'demographic' in json_line:
                counts['demographic'] += 1
                programs_with_field_group['demographic'].add(program_name)
            if 'diagnoses' in json_line:
                diagnoses = json_line['diagnoses'][0]
                counts['diagnoses'] += 1
                programs_with_field_group['diagnoses'].add(program_name)
                if 'annotations' in diagnoses:
                    counts['diagnoses.annotations'] += 1
                    programs_with_field_group['diagnoses.annotations'].add(program_name)
                if 'treatments' in diagnoses.keys():
                    counts['diagnoses.treatments'] += 1
                    programs_with_field_group['diagnoses.treatments'].add(program_name)
            if 'exposures' in json_line:
                counts['exposures'] += 1
                programs_with_field_group['exposures'].add(program_name)
            if 'family_histories' in json_line:
                counts['family_histories'] += 1
                programs_with_field_group['family_histories'].add(program_name)
            if 'follow_ups' in json_line:
                counts['follow_ups'] += 1
                programs_with_field_group['follow_ups'].add(program_name)
                if 'molecular_tests' in json_line['follow_ups'][0]:
                    programs_with_field_group['follow_ups.molecular_tests'].add(program_name)

            # Case has no clinical data field groups in API
            if 'demographic' not in json_line and 'family_histories' not in json_line \
                    and 'exposures' not in json_line and 'diagnoses' not in json_line \
                    and 'follow_ups' not in json_line:
                programs_with_field_group['none'].add(program_name)
                counts['no_clinical_fgs'] += 1

                if program_name not in no_fg_case_barcodes:
                    no_fg_case_barcodes[program_name] = set()
                no_fg_case_barcodes[program_name].add(json_line['submitter_id'])

        # OUTPUT RESULTS
        for fg in field_groups:
            print_field_group_check(fg, counts, programs_with_field_group)

        print("\nPrograms with no clinical data:")

        for program in no_fg_case_barcodes:
            no_fg_case_count = len(no_fg_case_barcodes[program])
            print('\n{} has {} cases with no clinical data.'.format(program, str(no_fg_case_count)))
            print('submitter_id (case_barcode) list:')
            print(no_fg_case_barcodes[program])


def print_field_group_check(fg_name, counts, fg_program_list):
    fg_pct = counts[fg_name] / (counts['total'] * 1.0) * 100

    print('For {}:'.format(fg_name))
    print('\tfound in {:.2f}% of cases'.format(fg_pct))
    print('\tprograms with {} field_group: {}'.format(fg_name, str(fg_program_list[fg_name])))


def check_gdc_webapp_data(gdc_dict, api_fp):
    row_match_count = 0
    row_not_match_count = 0
    with open(api_fp, 'r') as api_file:
        for row in api_file:
            row_match = True
            api_case_json = json.loads(row)
            case_id = api_case_json['case_id']

            gdc_case_json = gdc_dict[case_id]

            for fg in gdc_case_json.keys():
                if fg in api_case_json and gdc_case_json[fg] == api_case_json[fg]:
                    continue

                if fg not in api_case_json:
                    print("case_id {}: {} not in api case record".format(case_id, fg))
                    row_match = False
                    continue

                gdc_fg = gdc_case_json[fg]
                api_fg = api_case_json[fg]
                # find mis-matched values
                if isinstance(gdc_fg, list):
                    gdc_fg = gdc_fg[0]
                    api_fg = api_fg[0]

                for fg_key in gdc_fg.keys():
                    if fg_key not in api_fg:
                        row_match = False
                        print("case_id {}: API case version does not contain field {} in {}"
                              .format(case_id, fg_key, fg))
                    elif api_fg[fg_key] != gdc_fg[fg_key]:
                        row_match = False
                        print("case_id {}: field values mismatch for field {} in {}".format(case_id, fg_key, fg))
                        print("api: {}, webapp: {}".format(api_fg[fg_key], gdc_fg[fg_key]))
            if row_match:
                row_match_count += 1
            else:
                row_not_match_count += 1

    print("GDC Clinical file download (from webapp) results")
    print("Matching case count: {}".format(row_match_count))
    print("Non-matching case count: {}".format(row_not_match_count))


def create_gdc_cases_dict(gdc_fp):
    """
    Transform into a dict with case_ids as key
    :param gdc_fp:
    :return:
    """
    gdc_dict = {}
    with open(gdc_fp, 'r') as gdc_file:
        gdc_cases = json.load(gdc_file)

        for case in gdc_cases:
            case_id = case.pop('case_id')
            gdc_dict[case_id] = case

    return gdc_dict


def get_api_field_set(field_group_name):
    api_field_set = set()
    field_list = [
        "exposures.coal_dust_exposure",
        "exposures.updated_datetime",
        "exposures.years_smoked",
        "exposures.radon_exposure",
        "exposures.pack_years_smoked",
        "exposures.alcohol_days_per_week",
        "exposures.alcohol_drinks_per_day",
        "exposures.exposure_id",
        "exposures.asbestos_exposure",
        "exposures.type_of_smoke_exposure",
        "exposures.tobacco_smoking_onset_year",
        "exposures.age_at_onset",
        "exposures.height",
        "exposures.smoking_frequency",
        "exposures.exposure_type",
        "exposures.state",
        "exposures.tobacco_smoking_quit_year",
        "exposures.environmental_tobacco_smoke_exposure",
        "exposures.exposure_duration",
        "exposures.alcohol_history",
        "exposures.marijuana_use_per_week",
        "exposures.type_of_tobacco_used",
        "exposures.bmi",
        "exposures.weight",
        "exposures.tobacco_use_per_day",
        "exposures.created_datetime",
        "exposures.submitter_id",
        "exposures.secondhand_smoke_as_child",
        "exposures.cigarettes_per_day",
        "exposures.time_between_waking_and_first_smoke",
        "exposures.alcohol_intensity",
        "exposures.respirable_crystalline_silica_exposure",
        "exposures.tobacco_smoking_status",
        "diagnoses.ajcc_clinical_n",
        "diagnoses.masaoka_stage",
        "diagnoses.greatest_tumor_dimension",
        "diagnoses.percent_tumor_invasion",
        "diagnoses.diagnosis_id",
        "diagnoses.mitosis_karyorrhexis_index",
        "diagnoses.ajcc_clinical_m",
        "diagnoses.anaplasia_present",
        "diagnoses.primary_diagnosis",
        "diagnoses.primary_gleason_grade",
        "diagnoses.days_to_last_known_disease_status",
        "diagnoses.gross_tumor_weight",
        "diagnoses.year_of_diagnosis",
        "diagnoses.best_overall_response",
        "diagnoses.international_prognostic_index",
        "diagnoses.perineural_invasion_present",
        "diagnoses.margins_involved_site",
        "diagnoses.peripancreatic_lymph_nodes_tested",
        "diagnoses.weiss_assessment_score",
        "diagnoses.inpc_histologic_group",
        "diagnoses.micropapillary_features",
        "diagnoses.transglottic_extension",
        "diagnoses.figo_stage",
        "diagnoses.days_to_diagnosis",
        "diagnoses.progression_or_recurrence",
        "diagnoses.submitter_id",
        "diagnoses.ajcc_pathologic_m",
        "diagnoses.inrg_stage",
        "diagnoses.days_to_recurrence",
        "diagnoses.inss_stage",
        "diagnoses.metastasis_at_diagnosis",
        "diagnoses.ovarian_specimen_status",
        "diagnoses.cog_rhabdomyosarcoma_risk_group",
        "diagnoses.gastric_esophageal_junction_involvement",
        "diagnoses.site_of_resection_or_biopsy",
        "diagnoses.ajcc_staging_system_edition",
        "diagnoses.icd_10_code",
        "diagnoses.laterality",
        "diagnoses.gleason_grade_group",
        "diagnoses.age_at_diagnosis",
        "diagnoses.peritoneal_fluid_cytological_status",
        "diagnoses.ajcc_clinical_t",
        "diagnoses.days_to_last_follow_up",
        "diagnoses.anaplasia_present_type",
        "diagnoses.enneking_msts_tumor_site",
        "diagnoses.breslow_thickness",
        "diagnoses.lymph_nodes_tested",
        "diagnoses.goblet_cells_columnar_mucosa_present",
        "diagnoses.metastasis_at_diagnosis_site",
        "diagnoses.supratentorial_localization",
        "diagnoses.created_datetime",
        "diagnoses.ajcc_pathologic_stage",
        "diagnoses.non_nodal_tumor_deposits",
        "diagnoses.esophageal_columnar_metaplasia_present",
        "diagnoses.tumor_grade",
        "diagnoses.lymph_nodes_positive",
        "diagnoses.tumor_largest_dimension_diameter",
        "diagnoses.last_known_disease_status",
        "diagnoses.updated_datetime",
        "diagnoses.non_nodal_regional_disease",
        "diagnoses.pregnant_at_diagnosis",
        "diagnoses.irs_group",
        "diagnoses.ann_arbor_extranodal_involvement",
        "diagnoses.days_to_best_overall_response",
        "diagnoses.papillary_renal_cell_type",
        "diagnoses.burkitt_lymphoma_clinical_variant",
        "diagnoses.residual_disease",
        "diagnoses.medulloblastoma_molecular_classification",
        "diagnoses.tumor_regression_grade",
        "diagnoses.enneking_msts_grade",
        "diagnoses.vascular_invasion_present",
        "diagnoses.child_pugh_classification",
        "diagnoses.first_symptom_prior_to_diagnosis",
        "diagnoses.state",
        "diagnoses.enneking_msts_stage",
        "diagnoses.irs_stage",
        "diagnoses.esophageal_columnar_dysplasia_degree",
        "diagnoses.ajcc_clinical_stage",
        "diagnoses.ishak_fibrosis_score",
        "diagnoses.secondary_gleason_grade",
        "diagnoses.synchronous_malignancy",
        "diagnoses.gleason_patterns_percent",
        "diagnoses.lymph_node_involved_site",
        "diagnoses.tumor_depth",
        "diagnoses.morphology",
        "diagnoses.gleason_grade_tertiary",
        "diagnoses.ajcc_pathologic_t",
        "diagnoses.igcccg_stage",
        "diagnoses.inpc_grade",
        "diagnoses.largest_extrapelvic_peritoneal_focus",
        "diagnoses.figo_staging_edition_year",
        "diagnoses.lymphatic_invasion_present",
        "diagnoses.vascular_invasion_type",
        "diagnoses.wilms_tumor_histologic_subtype",
        "diagnoses.tumor_confined_to_organ_of_origin",
        "diagnoses.ovarian_surface_involvement",
        "diagnoses.cog_liver_stage",
        "diagnoses.treatments.days_to_treatment_start",
        "diagnoses.treatments.number_of_cycles",
        "diagnoses.treatments.updated_datetime",
        "diagnoses.treatments.treatment_outcome",
        "diagnoses.treatments.reason_treatment_ended",
        "diagnoses.treatments.chemo_concurrent_to_radiation",
        "diagnoses.treatments.treatment_arm",
        "diagnoses.treatments.treatment_type",
        "diagnoses.treatments.state",
        "diagnoses.treatments.treatment_effect",
        "diagnoses.treatments.treatment_anatomic_site",
        "diagnoses.treatments.treatment_or_therapy",
        "diagnoses.treatments.treatment_effect_indicator",
        "diagnoses.treatments.treatment_dose_units",
        "diagnoses.treatments.treatment_dose",
        "diagnoses.treatments.therapeutic_agents",
        "diagnoses.treatments.initial_disease_status",
        "diagnoses.treatments.days_to_treatment_end",
        "diagnoses.treatments.created_datetime",
        "diagnoses.treatments.treatment_frequency",
        "diagnoses.treatments.submitter_id",
        "diagnoses.treatments.regimen_or_line_of_therapy",
        "diagnoses.treatments.treatment_intent_type",
        "diagnoses.treatments.treatment_id",
        "diagnoses.classification_of_tumor",
        "diagnoses.margin_distance",
        "diagnoses.mitotic_count",
        "diagnoses.cog_renal_stage",
        "diagnoses.enneking_msts_metastasis",
        "diagnoses.ann_arbor_clinical_stage",
        "diagnoses.ann_arbor_pathologic_stage",
        "diagnoses.circumferential_resection_margin",
        "diagnoses.ann_arbor_b_symptoms",
        "diagnoses.tumor_stage",
        "diagnoses.iss_stage",
        "diagnoses.tumor_focality",
        "diagnoses.prior_treatment",
        "diagnoses.peripancreatic_lymph_nodes_positive",
        "diagnoses.annotations.legacy_created_datetime",
        "diagnoses.annotations.legacy_updated_datetime",
        "diagnoses.annotations.classification",
        "diagnoses.annotations.entity_submitter_id",
        "diagnoses.annotations.creator",
        "diagnoses.annotations.updated_datetime",
        "diagnoses.annotations.category",
        "diagnoses.annotations.case_id",
        "diagnoses.annotations.case_submitter_id",
        "diagnoses.annotations.created_datetime",
        "diagnoses.annotations.notes",
        "diagnoses.annotations.submitter_id",
        "diagnoses.annotations.annotation_id",
        "diagnoses.annotations.entity_id",
        "diagnoses.annotations.state",
        "diagnoses.annotations.entity_type",
        "diagnoses.annotations.status",
        "diagnoses.ajcc_pathologic_n",
        "diagnoses.method_of_diagnosis",
        "diagnoses.cog_neuroblastoma_risk_group",
        "diagnoses.tissue_or_organ_of_origin",
        "diagnoses.prior_malignancy",
        "demographic.submitter_id",
        "demographic.race",
        "demographic.days_to_death",
        "demographic.age_is_obfuscated",
        "demographic.weeks_gestation_at_birth",
        "demographic.updated_datetime",
        "demographic.country_of_residence_at_enrollment",
        "demographic.age_at_index",
        "demographic.vital_status",
        "demographic.cause_of_death_source",
        "demographic.year_of_birth",
        "demographic.created_datetime",
        "demographic.ethnicity",
        "demographic.cause_of_death",
        "demographic.premature_at_birth",
        "demographic.year_of_death",
        "demographic.occupation_duration_years",
        "demographic.state",
        "demographic.demographic_id",
        "demographic.days_to_birth",
        "demographic.gender",
        "family_histories.relatives_with_cancer_history_count",
        "family_histories.updated_datetime",
        "family_histories.relationship_age_at_diagnosis",
        "family_histories.submitter_id",
        "family_histories.relationship_gender",
        "family_histories.relative_with_cancer_history",
        "family_histories.created_datetime",
        "family_histories.relationship_primary_diagnosis",
        "family_histories.state",
        "family_histories.relationship_type",
        "family_histories.family_history_id",
        "submitter_diagnosis_ids",
        "consent_type",
        "updated_datetime",
        "lost_to_followup",
        "days_to_lost_to_followup",
        "submitter_analyte_ids",
        "follow_ups.updated_datetime",
        "follow_ups.imaging_result",
        "follow_ups.days_to_comorbidity",
        "follow_ups.hysterectomy_type",
        "follow_ups.menopause_status",
        "follow_ups.hormonal_contraceptive_use",
        "follow_ups.comorbidity",
        "follow_ups.dlco_ref_predictive_percent",
        "follow_ups.fev1_fvc_pre_bronch_percent",
        "follow_ups.fev1_ref_pre_bronch_percent",
        "follow_ups.diabetes_treatment_type",
        "follow_ups.hiv_viral_load",
        "follow_ups.aids_risk_factors",
        "follow_ups.state",
        "follow_ups.barretts_esophagus_goblet_cells_present",
        "follow_ups.recist_targeted_regions_sum",
        "follow_ups.weight",
        "follow_ups.karnofsky_performance_status",
        "follow_ups.disease_response",
        "follow_ups.body_surface_area",
        "follow_ups.fev1_ref_post_bronch_percent",
        "follow_ups.viral_hepatitis_serologies",
        "follow_ups.adverse_event_grade",
        "follow_ups.comorbidity_method_of_diagnosis",
        "follow_ups.submitter_id",
        "follow_ups.risk_factor_treatment",
        "follow_ups.follow_up_id",
        "follow_ups.scan_tracer_used",
        "follow_ups.hysterectomy_margins_involved",
        "follow_ups.days_to_follow_up",
        "follow_ups.pregnancy_outcome",
        "follow_ups.cdc_hiv_risk_factors",
        "follow_ups.molecular_tests.updated_datetime",
        "follow_ups.molecular_tests.test_analyte_type",
        "follow_ups.molecular_tests.pathogenicity",
        "follow_ups.molecular_tests.aa_change",
        "follow_ups.molecular_tests.state",
        "follow_ups.molecular_tests.variant_type",
        "follow_ups.molecular_tests.blood_test_normal_range_upper",
        "follow_ups.molecular_tests.second_exon",
        "follow_ups.molecular_tests.test_units",
        "follow_ups.molecular_tests.molecular_test_id",
        "follow_ups.molecular_tests.loci_count",
        "follow_ups.molecular_tests.antigen",
        "follow_ups.molecular_tests.exon",
        "follow_ups.molecular_tests.transcript",
        "follow_ups.molecular_tests.loci_abnormal_count",
        "follow_ups.molecular_tests.copy_number",
        "follow_ups.molecular_tests.zygosity",
        "follow_ups.molecular_tests.test_value",
        "follow_ups.molecular_tests.second_gene_symbol",
        "follow_ups.molecular_tests.molecular_consequence",
        "follow_ups.molecular_tests.clonality",
        "follow_ups.molecular_tests.biospecimen_type",
        "follow_ups.molecular_tests.gene_symbol",
        "follow_ups.molecular_tests.chromosome",
        "follow_ups.molecular_tests.locus",
        "follow_ups.molecular_tests.specialized_molecular_test",
        "follow_ups.molecular_tests.molecular_analysis_method",
        "follow_ups.molecular_tests.variant_origin",
        "follow_ups.molecular_tests.test_result",
        "follow_ups.molecular_tests.created_datetime",
        "follow_ups.molecular_tests.mismatch_repair_mutation",
        "follow_ups.molecular_tests.submitter_id",
        "follow_ups.molecular_tests.blood_test_normal_range_lower",
        "follow_ups.molecular_tests.ploidy",
        "follow_ups.molecular_tests.histone_family",
        "follow_ups.molecular_tests.cell_count",
        "follow_ups.molecular_tests.histone_variant",
        "follow_ups.molecular_tests.intron",
        "follow_ups.molecular_tests.laboratory_test",
        "follow_ups.molecular_tests.cytoband",
        "follow_ups.days_to_progression_free",
        "follow_ups.reflux_treatment_type",
        "follow_ups.fev1_fvc_post_bronch_percent",
        "follow_ups.hpv_positive_type",
        "follow_ups.ecog_performance_status",
        "follow_ups.cd4_count",
        "follow_ups.progression_or_recurrence",
        "follow_ups.evidence_of_recurrence_type",
        "follow_ups.progression_or_recurrence_anatomic_site",
        "follow_ups.recist_targeted_regions_number",
        "follow_ups.days_to_progression",
        "follow_ups.pancreatitis_onset_year",
        "follow_ups.risk_factor",
        "follow_ups.height",
        "follow_ups.haart_treatment_indicator",
        "follow_ups.adverse_event",
        "follow_ups.imaging_type",
        "follow_ups.hepatitis_sustained_virological_response",
        "follow_ups.immunosuppressive_treatment_type",
        "follow_ups.days_to_recurrence",
        "follow_ups.created_datetime",
        "follow_ups.days_to_imaging",
        "follow_ups.cause_of_response",
        "follow_ups.bmi",
        "follow_ups.nadir_cd4_count",
        "follow_ups.days_to_adverse_event",
        "follow_ups.progression_or_recurrence_type",
        "slide_ids",
        "summary.experimental_strategies.file_count",
        "summary.experimental_strategies.experimental_strategy",
        "summary.file_size",
        "summary.file_count",
        "summary.data_categories.data_category",
        "summary.data_categories.file_count",
        "days_to_index",
        "case_autocomplete",
        "state",
        "submitter_slide_ids",
        "project.releasable",
        "project.released",
        "project.intended_release_date",
        "project.name",
        "project.program.name",
        "project.program.dbgap_accession_number",
        "project.program.program_id",
        "project.disease_type",
        "project.state",
        "project.dbgap_accession_number",
        "project.primary_site",
        "project.project_id",
        "primary_site",
        "aliquot_ids",
        "files.updated_datetime",
        "files.imaging_date",
        "files.chip_id",
        "files.state",
        "files.acl",
        "files.data_category",
        "files.average_base_quality",
        "files.proportion_coverage_30x",
        "files.data_type",
        "files.file_name",
        "files.error_type",
        "files.access",
        "files.tumor_ploidy",
        "files.pairs_on_diff_chr",
        "files.platform",
        "files.experimental_strategy",
        "files.md5sum",
        "files.tags",
        "files.analysis.updated_datetime",
        "files.analysis.workflow_link",
        "files.analysis.workflow_version",
        "files.analysis.input_files.updated_datetime",
        "files.analysis.input_files.imaging_date",
        "files.analysis.input_files.chip_id",
        "files.analysis.input_files.data_category",
        "files.analysis.input_files.average_base_quality",
        "files.analysis.input_files.proportion_coverage_30x",
        "files.analysis.input_files.data_type",
        "files.analysis.input_files.error_type",
        "files.analysis.input_files.mean_coverage",
        "files.analysis.input_files.access",
        "files.analysis.input_files.state",
        "files.analysis.input_files.file_size",
        "files.analysis.input_files.platform",
        "files.analysis.input_files.experimental_strategy",
        "files.analysis.input_files.md5sum",
        "files.analysis.input_files.proportion_targets_no_coverage",
        "files.analysis.input_files.proportion_coverage_10X",
        "files.analysis.input_files.proportion_reads_duplicated",
        "files.analysis.input_files.file_id",
        "files.analysis.input_files.average_insert_size",
        "files.analysis.input_files.submitter_id",
        "files.analysis.input_files.plate_name",
        "files.analysis.input_files.read_pair_number",
        "files.analysis.input_files.proportion_base_mismatch",
        "files.analysis.input_files.channel",
        "files.analysis.input_files.average_read_length",
        "files.analysis.input_files.contamination_error",
        "files.analysis.input_files.file_name",
        "files.analysis.input_files.magnification",
        "files.analysis.input_files.proportion_reads_mapped",
        "files.analysis.input_files.proportion_coverage_30X",
        "files.analysis.input_files.tumor_ploidy",
        "files.analysis.input_files.contamination",
        "files.analysis.input_files.data_format",
        "files.analysis.input_files.msi_status",
        "files.analysis.input_files.plate_well",
        "files.analysis.input_files.chip_position",
        "files.analysis.input_files.msi_score",
        "files.analysis.input_files.pairs_on_diff_chr",
        "files.analysis.input_files.tumor_purity",
        "files.analysis.input_files.created_datetime",
        "files.analysis.input_files.revision",
        "files.analysis.input_files.total_reads",
        "files.analysis.input_files.stain_type",
        "files.analysis.input_files.proportion_coverage_10x",
        "files.analysis.input_files.state_comment",
        "files.analysis.analysis_type",
        "files.analysis.workflow_start_datetime",
        "files.analysis.workflow_end_datetime",
        "files.analysis.created_datetime",
        "files.analysis.submitter_id",
        "files.analysis.workflow_type",
        "files.analysis.state",
        "files.analysis.metadata.read_groups.updated_datetime",
        "files.analysis.metadata.read_groups.library_preparation_kit_vendor",
        "files.analysis.metadata.read_groups.library_preparation_kit_catalog_number",
        "files.analysis.metadata.read_groups.read_group_name",
        "files.analysis.metadata.read_groups.target_capture_kit_target_region",
        "files.analysis.metadata.read_groups.library_strategy",
        "files.analysis.metadata.read_groups.library_strand",
        "files.analysis.metadata.read_groups.is_paired_end",
        "files.analysis.metadata.read_groups.target_capture_kit_version",
        "files.analysis.metadata.read_groups.adapter_sequence",
        "files.analysis.metadata.read_groups.RIN",
        "files.analysis.metadata.read_groups.state",
        "files.analysis.metadata.read_groups.base_caller_version",
        "files.analysis.metadata.read_groups.library_selection",
        "files.analysis.metadata.read_groups.days_to_sequencing",
        "files.analysis.metadata.read_groups.sequencing_date",
        "files.analysis.metadata.read_groups.fragment_standard_deviation_length",
        "files.analysis.metadata.read_groups.lane_number",
        "files.analysis.metadata.read_groups.target_capture_kit_name",
        "files.analysis.metadata.read_groups.spike_ins_concentration",
        "files.analysis.metadata.read_groups.read_length",
        "files.analysis.metadata.read_groups.spike_ins_fasta",
        "files.analysis.metadata.read_groups.submitter_id",
        "files.analysis.metadata.read_groups.library_name",
        "files.analysis.metadata.read_groups.library_preparation_kit_name",
        "files.analysis.metadata.read_groups.library_preparation_kit_version",
        "files.analysis.metadata.read_groups.target_capture_kit_vendor",
        "files.analysis.metadata.read_groups.adapter_name",
        "files.analysis.metadata.read_groups.sequencing_center",
        "files.analysis.metadata.read_groups.instrument_model",
        "files.analysis.metadata.read_groups.rin",
        "files.analysis.metadata.read_groups.fragment_maximum_length",
        "files.analysis.metadata.read_groups.flow_cell_barcode",
        "files.analysis.metadata.read_groups.read_group_id",
        "files.analysis.metadata.read_groups.read_group_qcs.updated_datetime",
        "files.analysis.metadata.read_groups.read_group_qcs.kmer_content",
        "files.analysis.metadata.read_groups.read_group_qcs.fastq_name",
        "files.analysis.metadata.read_groups.read_group_qcs.workflow_link",
        "files.analysis.metadata.read_groups.read_group_qcs.per_base_sequence_quality",
        "files.analysis.metadata.read_groups.read_group_qcs.workflow_start_datetime",
        "files.analysis.metadata.read_groups.read_group_qcs.adapter_content",
        "files.analysis.metadata.read_groups.read_group_qcs.per_sequence_quality_score",
        "files.analysis.metadata.read_groups.read_group_qcs.overrepresented_sequences",
        "files.analysis.metadata.read_groups.read_group_qcs.per_tile_sequence_quality",
        "files.analysis.metadata.read_groups.read_group_qcs.per_base_n_content",
        "files.analysis.metadata.read_groups.read_group_qcs.state",
        "files.analysis.metadata.read_groups.read_group_qcs.sequence_length_distribution",
        "files.analysis.metadata.read_groups.read_group_qcs.workflow_version",
        "files.analysis.metadata.read_groups.read_group_qcs.per_sequence_gc_content",
        "files.analysis.metadata.read_groups.read_group_qcs.read_group_qc_id",
        "files.analysis.metadata.read_groups.read_group_qcs.per_base_sequence_content",
        "files.analysis.metadata.read_groups.read_group_qcs.workflow_end_datetime",
        "files.analysis.metadata.read_groups.read_group_qcs.created_datetime",
        "files.analysis.metadata.read_groups.read_group_qcs.total_sequences",
        "files.analysis.metadata.read_groups.read_group_qcs.submitter_id",
        "files.analysis.metadata.read_groups.read_group_qcs.percent_gc_content",
        "files.analysis.metadata.read_groups.read_group_qcs.workflow_type",
        "files.analysis.metadata.read_groups.read_group_qcs.sequence_duplication_levels",
        "files.analysis.metadata.read_groups.read_group_qcs.encoding",
        "files.analysis.metadata.read_groups.read_group_qcs.basic_statistics",
        "files.analysis.metadata.read_groups.target_capture_kit_catalog_number",
        "files.analysis.metadata.read_groups.multiplex_barcode",
        "files.analysis.metadata.read_groups.fragment_mean_length",
        "files.analysis.metadata.read_groups.platform",
        "files.analysis.metadata.read_groups.single_cell_library",
        "files.analysis.metadata.read_groups.includes_spike_ins",
        "files.analysis.metadata.read_groups.target_capture_kit",
        "files.analysis.metadata.read_groups.to_trim_adapter_sequence",
        "files.analysis.metadata.read_groups.size_selection_range",
        "files.analysis.metadata.read_groups.created_datetime",
        "files.analysis.metadata.read_groups.fragment_minimum_length",
        "files.analysis.metadata.read_groups.base_caller_name",
        "files.analysis.metadata.read_groups.number_expect_cells",
        "files.analysis.metadata.read_groups.experiment_name",
        "files.analysis.analysis_id",
        "files.proportion_targets_no_coverage",
        "files.metadata_files.updated_datetime",
        "files.metadata_files.file_name",
        "files.metadata_files.file_id",
        "files.metadata_files.data_category",
        "files.metadata_files.created_datetime",
        "files.metadata_files.data_type",
        "files.metadata_files.file_size",
        "files.metadata_files.error_type",
        "files.metadata_files.access",
        "files.metadata_files.submitter_id",
        "files.metadata_files.state",
        "files.metadata_files.data_format",
        "files.metadata_files.type",
        "files.metadata_files.state_comment",
        "files.metadata_files.md5sum",
        "files.proportion_reads_duplicated",
        "files.file_id",
        "files.read_pair_number",
        "files.submitter_id",
        "files.plate_name",
        "files.magnification",
        "files.proportion_base_mismatch",
        "files.channel",
        "files.average_read_length",
        "files.type",
        "files.contamination_error",
        "files.archive.md5sum",
        "files.archive.updated_datetime",
        "files.archive.file_name",
        "files.archive.data_category",
        "files.archive.archive_id",
        "files.archive.created_datetime",
        "files.archive.data_type",
        "files.archive.revision",
        "files.archive.error_type",
        "files.archive.submitter_id",
        "files.archive.state",
        "files.archive.data_format",
        "files.archive.state_comment",
        "files.archive.file_size",
        "files.proportion_coverage_10X",
        "files.proportion_reads_mapped",
        "files.proportion_coverage_30X",
        "files.center.center_id",
        "files.center.center_type",
        "files.center.name",
        "files.center.short_name",
        "files.center.namespace",
        "files.center.code",
        "files.origin",
        "files.contamination",
        "files.data_format",
        "files.downstream_analyses.updated_datetime",
        "files.downstream_analyses.workflow_link",
        "files.downstream_analyses.workflow_version",
        "files.downstream_analyses.analysis_type",
        "files.downstream_analyses.workflow_start_datetime",
        "files.downstream_analyses.workflow_end_datetime",
        "files.downstream_analyses.created_datetime",
        "files.downstream_analyses.output_files.updated_datetime",
        "files.downstream_analyses.output_files.imaging_date",
        "files.downstream_analyses.output_files.chip_id",
        "files.downstream_analyses.output_files.data_category",
        "files.downstream_analyses.output_files.average_base_quality",
        "files.downstream_analyses.output_files.proportion_coverage_30x",
        "files.downstream_analyses.output_files.data_type",
        "files.downstream_analyses.output_files.error_type",
        "files.downstream_analyses.output_files.mean_coverage",
        "files.downstream_analyses.output_files.access",
        "files.downstream_analyses.output_files.state",
        "files.downstream_analyses.output_files.file_size",
        "files.downstream_analyses.output_files.platform",
        "files.downstream_analyses.output_files.experimental_strategy",
        "files.downstream_analyses.output_files.md5sum",
        "files.downstream_analyses.output_files.proportion_targets_no_coverage",
        "files.downstream_analyses.output_files.proportion_coverage_10X",
        "files.downstream_analyses.output_files.proportion_reads_duplicated",
        "files.downstream_analyses.output_files.file_id",
        "files.downstream_analyses.output_files.average_insert_size",
        "files.downstream_analyses.output_files.submitter_id",
        "files.downstream_analyses.output_files.plate_name",
        "files.downstream_analyses.output_files.read_pair_number",
        "files.downstream_analyses.output_files.proportion_base_mismatch",
        "files.downstream_analyses.output_files.channel",
        "files.downstream_analyses.output_files.average_read_length",
        "files.downstream_analyses.output_files.contamination_error",
        "files.downstream_analyses.output_files.file_name",
        "files.downstream_analyses.output_files.magnification",
        "files.downstream_analyses.output_files.proportion_reads_mapped",
        "files.downstream_analyses.output_files.proportion_coverage_30X",
        "files.downstream_analyses.output_files.tumor_ploidy",
        "files.downstream_analyses.output_files.contamination",
        "files.downstream_analyses.output_files.data_format",
        "files.downstream_analyses.output_files.msi_status",
        "files.downstream_analyses.output_files.plate_well",
        "files.downstream_analyses.output_files.chip_position",
        "files.downstream_analyses.output_files.msi_score",
        "files.downstream_analyses.output_files.pairs_on_diff_chr",
        "files.downstream_analyses.output_files.tumor_purity",
        "files.downstream_analyses.output_files.created_datetime",
        "files.downstream_analyses.output_files.revision",
        "files.downstream_analyses.output_files.total_reads",
        "files.downstream_analyses.output_files.stain_type",
        "files.downstream_analyses.output_files.proportion_coverage_10x",
        "files.downstream_analyses.output_files.state_comment",
        "files.downstream_analyses.submitter_id",
        "files.downstream_analyses.workflow_type",
        "files.downstream_analyses.state",
        "files.downstream_analyses.analysis_id",
        "files.msi_status",
        "files.revision",
        "files.index_files.updated_datetime",
        "files.index_files.imaging_date",
        "files.index_files.chip_id",
        "files.index_files.data_category",
        "files.index_files.average_base_quality",
        "files.index_files.proportion_coverage_30x",
        "files.index_files.data_type",
        "files.index_files.error_type",
        "files.index_files.mean_coverage",
        "files.index_files.access",
        "files.index_files.state",
        "files.index_files.file_size",
        "files.index_files.platform",
        "files.index_files.experimental_strategy",
        "files.index_files.md5sum",
        "files.index_files.proportion_targets_no_coverage",
        "files.index_files.proportion_coverage_10X",
        "files.index_files.proportion_reads_duplicated",
        "files.index_files.file_id",
        "files.index_files.average_insert_size",
        "files.index_files.submitter_id",
        "files.index_files.plate_name",
        "files.index_files.read_pair_number",
        "files.index_files.proportion_base_mismatch",
        "files.index_files.channel",
        "files.index_files.average_read_length",
        "files.index_files.contamination_error",
        "files.index_files.file_name",
        "files.index_files.magnification",
        "files.index_files.proportion_reads_mapped",
        "files.index_files.proportion_coverage_30X",
        "files.index_files.tumor_ploidy",
        "files.index_files.contamination",
        "files.index_files.data_format",
        "files.index_files.msi_status",
        "files.index_files.plate_well",
        "files.index_files.chip_position",
        "files.index_files.msi_score",
        "files.index_files.pairs_on_diff_chr",
        "files.index_files.tumor_purity",
        "files.index_files.created_datetime",
        "files.index_files.revision",
        "files.index_files.total_reads",
        "files.index_files.stain_type",
        "files.index_files.proportion_coverage_10x",
        "files.index_files.state_comment",
        "files.plate_well",
        "files.chip_position",
        "files.average_insert_size",
        "files.msi_score",
        "files.mean_coverage",
        "files.tumor_purity",
        "files.created_datetime",
        "files.file_size",
        "files.total_reads",
        "files.stain_type",
        "files.proportion_coverage_10x",
        "files.state_comment",
        "diagnosis_ids",
        "portion_ids",
        "sample_ids",
        "disease_type",
        "days_to_consent",
        "case_id",
        "samples.updated_datetime",
        "samples.tumor_descriptor",
        "samples.distance_normal_to_tumor",
        "samples.time_between_clamping_and_freezing",
        "samples.tumor_code",
        "samples.portions.updated_datetime",
        "samples.portions.analytes.normal_tumor_genotype_snp_match",
        "samples.portions.analytes.updated_datetime",
        "samples.portions.analytes.analyte_volume",
        "samples.portions.analytes.well_number",
        "samples.portions.analytes.concentration",
        "samples.portions.analytes.aliquots.updated_datetime",
        "samples.portions.analytes.aliquots.selected_normal_wxs",
        "samples.portions.analytes.aliquots.selected_normal_low_pass_wgs",
        "samples.portions.analytes.aliquots.concentration",
        "samples.portions.analytes.aliquots.center.center_id",
        "samples.portions.analytes.aliquots.center.center_type",
        "samples.portions.analytes.aliquots.center.name",
        "samples.portions.analytes.aliquots.center.short_name",
        "samples.portions.analytes.aliquots.center.namespace",
        "samples.portions.analytes.aliquots.center.code",
        "samples.portions.analytes.aliquots.analyte_type",
        "samples.portions.analytes.aliquots.state",
        "samples.portions.analytes.aliquots.amount",
        "samples.portions.analytes.aliquots.analyte_type_id",
        "samples.portions.analytes.aliquots.selected_normal_wgs",
        "samples.portions.analytes.aliquots.aliquot_quantity",
        "samples.portions.analytes.aliquots.no_matched_normal_targeted_sequencing",
        "samples.portions.analytes.aliquots.no_matched_normal_wgs",
        "samples.portions.analytes.aliquots.selected_normal_targeted_sequencing",
        "samples.portions.analytes.aliquots.created_datetime",
        "samples.portions.analytes.aliquots.annotations.legacy_created_datetime",
        "samples.portions.analytes.aliquots.annotations.legacy_updated_datetime",
        "samples.portions.analytes.aliquots.annotations.classification",
        "samples.portions.analytes.aliquots.annotations.entity_submitter_id",
        "samples.portions.analytes.aliquots.annotations.creator",
        "samples.portions.analytes.aliquots.annotations.updated_datetime",
        "samples.portions.analytes.aliquots.annotations.category",
        "samples.portions.analytes.aliquots.annotations.case_id",
        "samples.portions.analytes.aliquots.annotations.case_submitter_id",
        "samples.portions.analytes.aliquots.annotations.created_datetime",
        "samples.portions.analytes.aliquots.annotations.notes",
        "samples.portions.analytes.aliquots.annotations.submitter_id",
        "samples.portions.analytes.aliquots.annotations.annotation_id",
        "samples.portions.analytes.aliquots.annotations.entity_id",
        "samples.portions.analytes.aliquots.annotations.state",
        "samples.portions.analytes.aliquots.annotations.entity_type",
        "samples.portions.analytes.aliquots.annotations.status",
        "samples.portions.analytes.aliquots.submitter_id",
        "samples.portions.analytes.aliquots.aliquot_volume",
        "samples.portions.analytes.aliquots.source_center",
        "samples.portions.analytes.aliquots.no_matched_normal_low_pass_wgs",
        "samples.portions.analytes.aliquots.aliquot_id",
        "samples.portions.analytes.aliquots.no_matched_normal_wxs",
        "samples.portions.analytes.analyte_type_id",
        "samples.portions.analytes.spectrophotometer_method",
        "samples.portions.analytes.analyte_id",
        "samples.portions.analytes.created_datetime",
        "samples.portions.analytes.annotations.legacy_created_datetime",
        "samples.portions.analytes.annotations.legacy_updated_datetime",
        "samples.portions.analytes.annotations.classification",
        "samples.portions.analytes.annotations.entity_submitter_id",
        "samples.portions.analytes.annotations.creator",
        "samples.portions.analytes.annotations.updated_datetime",
        "samples.portions.analytes.annotations.category",
        "samples.portions.analytes.annotations.case_id",
        "samples.portions.analytes.annotations.case_submitter_id",
        "samples.portions.analytes.annotations.created_datetime",
        "samples.portions.analytes.annotations.notes",
        "samples.portions.analytes.annotations.submitter_id",
        "samples.portions.analytes.annotations.annotation_id",
        "samples.portions.analytes.annotations.entity_id",
        "samples.portions.analytes.annotations.state",
        "samples.portions.analytes.annotations.entity_type",
        "samples.portions.analytes.annotations.status",
        "samples.portions.analytes.submitter_id",
        "samples.portions.analytes.analyte_type",
        "samples.portions.analytes.ribosomal_rna_28s_16s_ratio",
        "samples.portions.analytes.state",
        "samples.portions.analytes.analyte_quantity",
        "samples.portions.analytes.a260_a280_ratio",
        "samples.portions.analytes.amount",
        "samples.portions.center.center_id",
        "samples.portions.center.center_type",
        "samples.portions.center.name",
        "samples.portions.center.short_name",
        "samples.portions.center.namespace",
        "samples.portions.center.code",
        "samples.portions.slides.submitter_id",
        "samples.portions.slides.updated_datetime",
        "samples.portions.slides.percent_tumor_nuclei",
        "samples.portions.slides.percent_monocyte_infiltration",
        "samples.portions.slides.percent_inflam_infiltration",
        "samples.portions.slides.percent_necrosis",
        "samples.portions.slides.percent_follicular_component",
        "samples.portions.slides.percent_sarcomatoid_features",
        "samples.portions.slides.bone_marrow_malignant_cells",
        "samples.portions.slides.percent_lymphocyte_infiltration",
        "samples.portions.slides.annotations.legacy_created_datetime",
        "samples.portions.slides.annotations.legacy_updated_datetime",
        "samples.portions.slides.annotations.classification",
        "samples.portions.slides.annotations.entity_submitter_id",
        "samples.portions.slides.annotations.creator",
        "samples.portions.slides.annotations.updated_datetime",
        "samples.portions.slides.annotations.category",
        "samples.portions.slides.annotations.case_id",
        "samples.portions.slides.annotations.case_submitter_id",
        "samples.portions.slides.annotations.created_datetime",
        "samples.portions.slides.annotations.notes",
        "samples.portions.slides.annotations.submitter_id",
        "samples.portions.slides.annotations.annotation_id",
        "samples.portions.slides.annotations.entity_id",
        "samples.portions.slides.annotations.state",
        "samples.portions.slides.annotations.entity_type",
        "samples.portions.slides.annotations.status",
        "samples.portions.slides.section_location",
        "samples.portions.slides.state",
        "samples.portions.slides.prostatic_chips_total_count",
        "samples.portions.slides.number_proliferating_cells",
        "samples.portions.slides.prostatic_chips_positive_count",
        "samples.portions.slides.percent_neutrophil_infiltration",
        "samples.portions.slides.slide_id",
        "samples.portions.slides.prostatic_involvement_percent",
        "samples.portions.slides.percent_tumor_cells",
        "samples.portions.slides.created_datetime",
        "samples.portions.slides.percent_eosinophil_infiltration",
        "samples.portions.slides.percent_granulocyte_infiltration",
        "samples.portions.slides.percent_normal_cells",
        "samples.portions.slides.percent_rhabdoid_features",
        "samples.portions.slides.percent_stromal_cells",
        "samples.portions.slides.tissue_microarray_coordinates",
        "samples.portions.portion_id",
        "samples.portions.is_ffpe",
        "samples.portions.portion_number",
        "samples.portions.created_datetime",
        "samples.portions.annotations.legacy_created_datetime",
        "samples.portions.annotations.legacy_updated_datetime",
        "samples.portions.annotations.classification",
        "samples.portions.annotations.entity_submitter_id",
        "samples.portions.annotations.creator",
        "samples.portions.annotations.updated_datetime",
        "samples.portions.annotations.category",
        "samples.portions.annotations.case_id",
        "samples.portions.annotations.case_submitter_id",
        "samples.portions.annotations.created_datetime",
        "samples.portions.annotations.notes",
        "samples.portions.annotations.submitter_id",
        "samples.portions.annotations.annotation_id",
        "samples.portions.annotations.entity_id",
        "samples.portions.annotations.state",
        "samples.portions.annotations.entity_type",
        "samples.portions.annotations.status",
        "samples.portions.submitter_id",
        "samples.portions.creation_datetime",
        "samples.portions.state",
        "samples.portions.weight",
        "samples.growth_rate",
        "samples.sample_id",
        "samples.longest_dimension",
        "samples.is_ffpe",
        "samples.initial_weight",
        "samples.time_between_excision_and_freezing",
        "samples.freezing_method",
        "samples.intermediate_dimension",
        "samples.distributor_reference",
        "samples.biospecimen_laterality",
        "samples.days_to_collection",
        "samples.state",
        "samples.composition",
        "samples.tissue_type",
        "samples.sample_type",
        "samples.method_of_sample_procurement",
        "samples.passage_count",
        "samples.days_to_sample_procurement",
        "samples.created_datetime",
        "samples.sample_type_id",
        "samples.pathology_report_uuid",
        "samples.tumor_code_id",
        "samples.diagnosis_pathologically_confirmed",
        "samples.oct_embedded",
        "samples.annotations.legacy_created_datetime",
        "samples.annotations.legacy_updated_datetime",
        "samples.annotations.classification",
        "samples.annotations.entity_submitter_id",
        "samples.annotations.creator",
        "samples.annotations.updated_datetime",
        "samples.annotations.category",
        "samples.annotations.case_id",
        "samples.annotations.case_submitter_id",
        "samples.annotations.created_datetime",
        "samples.annotations.notes",
        "samples.annotations.submitter_id",
        "samples.annotations.annotation_id",
        "samples.annotations.entity_id",
        "samples.annotations.state",
        "samples.annotations.entity_type",
        "samples.annotations.status",
        "samples.submitter_id",
        "samples.catalog_reference",
        "samples.shortest_dimension",
        "samples.current_weight",
        "samples.biospecimen_anatomic_site",
        "samples.tissue_collection_type",
        "samples.preservation_method",
        "submitter_aliquot_ids",
        "created_datetime",
        "annotations.legacy_created_datetime",
        "annotations.legacy_updated_datetime",
        "annotations.classification",
        "annotations.entity_submitter_id",
        "annotations.creator",
        "annotations.updated_datetime",
        "annotations.category",
        "annotations.case_id",
        "annotations.case_submitter_id",
        "annotations.created_datetime",
        "annotations.notes",
        "annotations.submitter_id",
        "annotations.annotation_id",
        "annotations.entity_id",
        "annotations.state",
        "annotations.entity_type",
        "annotations.status",
        "submitter_portion_ids",
        "submitter_id",
        "analyte_ids",
        "submitter_sample_ids",
        "index_date",
        "tissue_source_site.name",
        "tissue_source_site.code",
        "tissue_source_site.project",
        "tissue_source_site.bcr_id",
        "tissue_source_site.tissue_source_site_id"
    ]

    for field in field_list:
        if field_group_name == '':
            fg_depth = 0
        else:
            fg_depth = len(field_group_name.split("."))

        split_field = field.split(".")

        if len(split_field) == fg_depth + 1 and field.startswith(field_group_name):
            api_field_set.add(split_field[-1])

    return api_field_set


def map_cde_id_to_field_name(gdc_schema_fp):
    field_group_mappings = {
        'case': '',
        'clinical': '',
        'demographic': 'demographic',
        'diagnosis': 'diagnoses',
        'exposure': 'exposures',
        'family_history': 'family_histories',
        'follow_up': 'follow_ups',
        'molecular_test': 'molecular_tests',
        'treatment': 'treatments'
    }

    with open(gdc_schema_fp, 'r') as gdc_file:
        gdc_schema_json = json.load(gdc_file)  # todo: this could just make a url request, doesn't need to be a file

    for fg in field_group_mappings.keys():
        no_cde_id_fields = set()
        cde_id_dict = dict()

        api_fg_name = field_group_mappings[fg]
        schema_fg_name = fg
        schema_field_dict = gdc_schema_json[schema_fg_name]["properties"]

        api_field_set = get_api_field_set(api_fg_name)
        schema_field_set = set(schema_field_dict.keys())

        field_set_membership = {
            "api_only": api_field_set - schema_field_set,
            "schema_only": schema_field_set - api_field_set,
            "combined": api_field_set & schema_field_set
        }

        for field in field_set_membership['combined']:
            field_properties = None

            if 'common' in schema_field_dict[field]:
                field_properties = schema_field_dict[field]['common']

            if (field_properties
                    and "termDef" in field_properties
                    and "cde_id" in field_properties["termDef"]
                    and field_properties["termDef"]['cde_id']):
                cde_obj = field_properties["termDef"]
                cde_id = field_properties["termDef"]['cde_id']

                cde_obj.pop("term_url")
                cde_obj.pop("cde_id")
                cde_obj["api_field_name"] = field
                cde_id_dict[cde_id] = cde_obj
            else:
                no_cde_id_fields.add(field)

        # do something with no_cde_id_fields, cde_id_dict
        print("\n\n-- Analysis result for {} field group --\n".format(fg))
        print("Set membership:")
        for key in field_set_membership:
            print("\t{}: {}".format(key, str(field_set_membership[key])))

        print("\ncde_id lookup result:")
        print("\tMatching fields across sets where no cde_id entry exists: {}".format(str(no_cde_id_fields)))

        print("\tCDE_ID mapping result:")
        if not cde_id_dict:
            print("\t\t{}")
        for key in cde_id_dict:
            print("\t\t{}: {}".format(key, str(cde_id_dict[key])))


def main(args):
    webapp_data_fp = '../temp/clinical.cases_selection.2020-03-26.json'
    api_data_fp = '../temp/clinical_data.jsonl'

    yaml_headers = ('api_and_file_params', 'bq_params')

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        try:
            api_params, bq_params = load_config(yaml_file, yaml_headers)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    map_cde_id_to_field_name('../temp/json_schema_from_gdc_full.json')

    # output_clinical_data_stats(api_data_fp, api_params)

    # gdc_dict = create_gdc_cases_dict(webapp_data_fp)

    # check_gdc_webapp_data(gdc_dict, api_data_fp)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv)
    else:
        main(('check_clinical_data.py', '../temp/ClinicalBQBuild.yaml'))
