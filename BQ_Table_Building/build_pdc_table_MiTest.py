import requests
import json


def run_query(endpoint, query):
    request = requests.post(endpoint + query)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}"
                        .format(request.status_code, query))

# Useful functions in Ron's code
# query_to_get_biospecimenData_on_one_study
# GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID

def build_mapping_dicts_one_study():
    # query_to_get_biospecimenData_on_one_study

    endpoint = 'https://pdc.cancer.gov/graphql'
    study_id = u'96296fd1-89a4-11ea-b1fd-0aad30af8a83'
    query = '{ biospecimenPerStudy(study_id: "' + study_id + '"' + ') { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon} }'
    
    response = requests.post(endpoint, json={'query': query})

    if response.ok:
        json_res = response.json()

        if 'errors' in json_res:
            print('no')
        else:
            print('yes')
            print (json_res)

    return

def pull_quant_matrix_one_study():
    endpoint = 'https://pdc.cancer.gov/graphql'

    my_study_submitted_id = u'CPTAC HNSCC Discovery Study - Proteome'
    my_study_id = u'96296fd1-89a4-11ea-b1fd-0aad30af8a83'

    quant_log2_ratio_query = ('{ quantDataMatrix(study_submitter_id: \"'
                              + my_study_submitted_id + '\" data_type: \"log2_ratio\") }')

    quant_res = requests.post(endpoint, json={'query': quant_log2_ratio_query})

    if quant_res.ok:
        json_res = quant_res.json()

        if 'errors' in json_res:
            print('no')
        else:
            print('yes')
            first_row_data = json_res[u'data'][u'quantDataMatrix'][0]
            for i in range(1, len(first_row_data)):
                aliquot_submitter_id = first_row_data[i].split(":")[1]
                study_id_and_aliquot_submitter_id = my_study_id + ':' + aliquot_submitter_id
                print(first_row_data[i])
            # replace header from "internal_id:aliquot_submitter_id" to "study_id:aliquot_submitter_id"
            # use dict [study_id:aliquot_submitter_id -> aliquot_id]
            # use dict [aliquot_id -> gdc_case_id]
            print (json_res)


def pull_quant_matrix_all_studies():
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

    study_id_query = """{study (study_id: "96296e87-89a4-11ea-b1fd-0aad30af8a83") { 
        study_id 
        study_submitter_id 
        program_id 
        project_id 
        study_name 
        program_name 
        project_name 
        disease_type 
        primary_site 
        analytical_fraction 
        experiment_type 
        cases_count 
        aliquots_count 
        filesCount { 
            data_category 
            file_type 
            files_count 
        } 
    }}"""

    get_all_studies_query = """{programsProjectsStudies {
        program_submitter_id
        name
        program_id
        projects{
            project_id
            project_submitter_id
            name
            studies{
                study_id
                study_submitter_id
                submitter_id_name
                study_name
            }
        }
    }}"""

    endpoint = 'https://pdc.cancer.gov/graphql'

    response = requests.post(endpoint, json={'query': all_progs_query})

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

        quant_log2_ratio_query = ('{ quantDataMatrix(study_submitter_id: \"'
                                  + submitter_id + '\" data_type: \"log2_ratio\") }')

        quant_res = requests.post(endpoint, json={'query': quant_log2_ratio_query})

        if quant_res.ok:
            json_res = quant_res.json()

            if 'errors' in json_res:
                print('no')
                study_sub_no_quant.append(submitter_id)
            else:
                print('yes')
                study['quant_res'] = json_res
                study_sub_has_quant.append(submitter_id)

    print("study_sub_ids with quant results: {}".format(study_sub_has_quant))
    print("study_sub_ids NO quant results: {}".format(study_sub_no_quant))

def main():
    # pull_quant_matrix_all_studies()
    build_mapping_dicts_one_study()
    pull_quant_matrix_one_study()

if __name__ == '__main__':
    main()
