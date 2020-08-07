import requests
import json

# use dict [study_id:aliquot_submitter_id -> aliquot_id]
# use dict [aliquot_id -> gdc_case_id]
GLOBAL_STUDY_ID_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_DICT = {}
GLOBAL_ALIQUOT_ID_TO_GDC_CASE_ID_DICT = {}
PDC_END_POINT = 'https://pdc.cancer.gov/graphql'

# Useful functions in Ron's code
# query_to_get_biospecimenData_on_one_study
# GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID

def build_mapping_dicts_one_study(study_id):
    query = '{ biospecimenPerStudy(study_id: "' + \
            study_id + '"' + \
            ') { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status' \
            ' case_status sample_status project_name sample_type disease_type primary_site pool taxon} }'
    
    response = requests.post(PDC_END_POINT, json={'query': query})

    if response.ok:
        json_res = response.json()

        if 'errors' in json_res:
            print('Cannot build mapping dicts for study = ' + study_id)
        else:
            biospecimen_per_study_list = json_res[u'data'][u'biospecimenPerStudy']

            for i in range(len(biospecimen_per_study_list)):
                aliquot_id = biospecimen_per_study_list[i][u'aliquot_id']
                gdc_case_id = biospecimen_per_study_list[i][u'case_id']
                study_id_aliquot_submitter_id = study_id + ":" + biospecimen_per_study_list[i][u'aliquot_submitter_id']
                GLOBAL_STUDY_ID_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_DICT[study_id_aliquot_submitter_id] = aliquot_id
                GLOBAL_ALIQUOT_ID_TO_GDC_CASE_ID_DICT[aliquot_id] = gdc_case_id

    return


def pull_quant_matrix_one_study(study_submitter_id):
    quant_log2_ratio_query = ('{ quantDataMatrix(study_submitter_id: \"'
                              + study_submitter_id + '\" data_type: \"log2_ratio\") }')

    quant_res = requests.post(PDC_END_POINT, json={'query': quant_log2_ratio_query})

    if quant_res.ok:
        json_res = quant_res.json()

        if 'errors' in json_res:
            print('No quant matrix for study_submitter_id = ' + study_submitter_id)
            return None
        else:
            print('Found quant matrix for study_submitter_id = ' + study_submitter_id)
            return json_res[u'data'][u'quantDataMatrix']


def map_quant_matrix_one_study(study_id, quant_matrix):
    first_row_data = quant_matrix[0]
    aliquot_id_row = []
    aliquot_id_row.append(u'Aliquot ID')
    gdc_case_id_row = []
    gdc_case_id_row.append(u'GDC Case ID')
    for i in range(1, len(first_row_data)):
        aliquot_submitter_id = first_row_data[i].split(":")[1]
        study_id_and_aliquot_submitter_id = study_id + ':' + aliquot_submitter_id
        aliquot_id = GLOBAL_STUDY_ID_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_DICT.get(study_id_and_aliquot_submitter_id,
                                                                                 None)
        if aliquot_id:
            gdc_case_id = GLOBAL_ALIQUOT_ID_TO_GDC_CASE_ID_DICT.get(aliquot_id, None)
        else:
            gdc_case_id = None
        aliquot_id_row.append(aliquot_id)
        gdc_case_id_row.append(gdc_case_id)
    quant_matrix.insert(0, aliquot_id_row)
    quant_matrix.insert(0, gdc_case_id_row)
    print ("Test")


def generate_quant_matrix_table_one_study(quant_matrix, study_id):
    # TODO: get all study metadata for the study
    return


def pull_and_map_quant_matrix_one_study(study_id, study_submitter_id):
    my_quant_matrix = pull_quant_matrix_one_study(study_submitter_id)
    if my_quant_matrix:
        build_mapping_dicts_one_study(study_id)
        my_quant_matrix = map_quant_matrix_one_study(study_id, my_quant_matrix)
        my_quant_matrix_table = generate_quant_matrix_table_one_study(my_quant_matrix, study_id)
        print("Test my_quant_matrix_table")

def pull_and_map_quant_matrix_all_studies():
    all_progs_query = """{allPrograms{
        program_id
        program_submitter_id
        name
        projects {
            project_id
            project_submitter_id
            name
            studies {
                study_id
                submitter_id_name
                study_submitter_id
                analytical_fraction
                experiment_type
                acquisition_type
            } 
        }
    }}"""

    response = requests.post(PDC_END_POINT, json={'query': all_progs_query})

    studies = []

    if response.ok:
        json_res = response.json()
        for program in json_res['data']['allPrograms']:
            for project in program['projects']:
                for study in project['studies']:
                    study_dict = study.copy()
                    study_dict['program_id'] = program['program_id']
                    study_dict['program_submitter_id'] = program['program_submitter_id']
                    study_dict['program_name'] = program['name']
                    study_dict['project_id'] = project['project_id']
                    study_dict['project_submitter_id'] = project['project_submitter_id']
                    study_dict['project_name'] = project['name']
                    studies.append(study_dict)
    else:
        response.raise_for_status()

    study_sub_has_quant = []
    study_sub_no_quant = []

    for study in studies:
        submitter_id = study['study_submitter_id']
        study_id = study['study_id']

        quant_matrix = pull_quant_matrix_one_study(submitter_id)

        if quant_matrix:
            build_mapping_dicts_one_study(study_id)
            quant_matrix = map_quant_matrix_one_study(quant_matrix)
            quant_matrix_table = generate_quant_matrix_table_one_study(quant_matrix, study_id)
            study['quant_res'] = quant_matrix
            study_sub_no_quant.append(submitter_id)
        else:
            study_sub_has_quant.append(submitter_id)

    print("study_sub_ids with quant results: {}".format(study_sub_has_quant))
    print("study_sub_ids NO quant results: {}".format(study_sub_no_quant))


def main():
    # pull_and_map_quant_matrix_all_studies()

    # single hard-coded study test...
    my_study_id = u'96296fd1-89a4-11ea-b1fd-0aad30af8a83'
    my_study_submitter_id = u'CPTAC HNSCC Discovery Study - Proteome'
    pull_and_map_quant_matrix_one_study(my_study_id, my_study_submitter_id)


if __name__ == '__main__':
    main()
