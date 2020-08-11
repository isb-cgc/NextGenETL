"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import requests
import json


def run_query(endpoint, query):
    request = requests.post(endpoint + query)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}"
                        .format(request.status_code, query))


def main():

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

    print(study_dict)
    return

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


if __name__ == '__main__':
    main()
