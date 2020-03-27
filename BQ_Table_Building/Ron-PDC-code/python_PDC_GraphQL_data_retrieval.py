
# invocation:
# 
# generic:
#    python python_PDC_GraphQL_data_retrieval.py
#  
# %%%%%%%%%%%%%%%%%%%

# ON MY GDIT MAC:

#  Before using "activate" and "deactivate' to bring up / shut down my anaconda3 env
# we need to source 
# the .bash_profile file:
#
# bash-3.2$ source /Users/ronaldtaylor/.bash_profile

# to enter the anaconda3 environment:
#
# bash-3.2$ source activate base

# To exit:
# 
# bash-3.2$ source deactivate


# %%%%%%%%%%%%%%%%%%%

# ON MY PERSONAL MAC:

#  Before using "activate" and "deactivate' to bring up / shut down my anaconda2 env
# we need to source 
# the .bash_profile file:
#
# bash-3.2$ source /users/rtaylor/.bash_profile


# to enter the anaconda2 environment, where I have installed the venn diagram module:
#
# bash-3.2$ source activate base

# bash-3.2$ source deactivate

# %%%%%%%%%%%%%%%%%%%

# (base) bash-3.2$ conda list
# packages in environment at /anaconda2:
#  < list shown>

# (base) bash-4.2$ conda list | grep matplotlib
# matplotlib                2.2.2            py27h0e671d2_1  
# matplotlib-venn           0.11.5                    <pip>

# to exit the anaconda2 environment:
#
# bash-3.2$ source deactivate



import sys

# for use of sleep(sec)
import time

from google.cloud import bigquery
from google.cloud import storage

# import re
# import numpy as np
# import scipy.stats as stats
# import pylab as pl

# from io import StringIO

# use this on personal laptop
# sys.path.append('/GDITwork/PythonWork')

# import pandas as pd
# import seaborn as sns

# from shutil import copyfile


#Get details about a single file
import requests
import json

# The URL for our API calls
url = 'https://pdc.esacinc.com/graphql'

# query to get file metadata



 # %%%

# once filled in and dicts are created, the below dicts are imported with "pdc." prefix
GLOBAL_STUDY_ID_LIST = []
GLOBAL_STUDY_NAME_LIST = []   
GLOBAL_STUDY_SUBMITTER_ID_LIST = []
GLOBAL_STUDY_SUBMITTER_NAME_LIST = []

GLOBAL_PROJECT_ID_LIST = []
GLOBAL_PROJECT_NAME_LIST = []

GLOBAL_PROGRAM_ID_LIST = []
GLOBAL_PROGRAM_NAME_LIST = []

GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT = {}
GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT   = {}

# 3/9/20
GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT = {}
GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT = {}


GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT   = {}
GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT   = {}

# The GraphQL call programsProjectsStudies() currently does NOT return values for the
# fields below. We must use study() to get the values.
#
GLOBAL_STUDY_NAME_TO_DISEASE_TYPE_DICT   = {}
GLOBAL_STUDY_NAME_TO_EXPERIMENT_TYPE_DICT   = {}   
GLOBAL_STUDY_NAME_TO_PRIMARY_SITE_DICT   = {}
GLOBAL_STUDY_NAME_TO_CASES_COUNT_DICT   = {}
GLOBAL_STUDY_NAME_TO_ALIQUOTS_COUNT_DICT   = {}

GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT = {}
GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT = {}
 
GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT = {}
GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT = {}

GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT = {}
GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT = {}


GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT = {}
GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT = {}

GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT = {}
GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT = {}

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# once filled in and dicts are created, the below dicts are imported with "pdc_aliquot." prefix,
# using a slightly different lower-case spelling for each dict.

# added Sunday 2/16/20
GLOBAL_DICT_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST = {}
# added Sunday 2/16/20
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST = {}

GLOBAL_DICT_STUDY_ID_TO_ALIQUOT_ID_LIST = {}
GLOBAL_DICT_STUDY_ID_TO_CASE_ID_LIST = {}   


GLOBAL_DICT_STUDY_NAME_TO_ALIQUOT_ID_LIST = {}
GLOBAL_DICT_STUDY_NAME_TO_CASE_ID_LIST = {}   

GLOBAL_DICT_CASE_ID_TO_STUDY_ID_LIST = {}
GLOBAL_DICT_CASE_ID_TO_STUDY_NAME_LIST = {}

GLOBAL_DICT_SAMPLE_ID_TO_STUDY_ID_LIST = {}
GLOBAL_DICT_SAMPLE_ID_TO_STUDY_NAME_LIST = {}   

GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_ID_LIST = {}   
GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_NAME_LIST = {}   

GLOBAL_DICT_CASE_ID_TO_ALIQUOT_ID_LIST = {}
GLOBAL_DICT_CASE_ID_TO_SAMPLE_ID_LIST = {}

GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID = {}
GLOBAL_DICT_ALIQUOT_ID_TO_SAMPLE_ID = {}

GLOBAL_DICT_SAMPLE_ID_TO_CASE_ID = {}   

GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_TYPE = {}   
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PRIMARY_SITE = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_STATUS = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_SUBMITTER_ID = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_CASE_STATUS = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_DISEASE_TYPE = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_POOL = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PROJECT_NAME = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_STATUS = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID = {}
GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_TAXON = {}

# %%%%%

dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped = {}

# %%%%%

# Use "NO" when the dictionaries are being created or recreated using
#    query_to_fill_in_global_lists_and_dictionaries_using_programsProjectsStudies()  
#    query_to_fill_in_study_name_disease_type_primary_site_cases_count_aliquots_count_in_global_lists_and_dictionaries_using_study()
#    dict_outfile_name = "AA_LATEST_dictionaries_for_pdc_proteomics_data.py"
#    store_project_tables_in_python_dictionaries(project_name, dict_file_name, char_limit)
#
# import_pdc_dictionaries = "NO"
import_pdc_dictionaries = "YES"

# import_pdc_biospecimen_aliquot_dictionaries = "NO"
import_pdc_biospecimen_aliquot_dictionaries = "YES"

# %%%%%%%%%%%%%%%

# import_pdc_file_name                = "AA_Mon_1_27_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
# import_pdc_file_name_without_suffix = "AA_Mon_1_27_20_LATEST_dictionaries_for_pdc_proteomics_data"

# import_pdc_file_name                = "AA_LATEST_dictionaries_for_pdc_proteomics_data.py"
# import_pdc_file_name_without_suffix = "AA_LATEST_dictionaries_for_pdc_proteomics_data"

# import_pdc_file_name                = "dictionaries_for_pdc_proteomics_data.py"
# import_pdc_file_name_without_suffix = "dictionaries_for_pdc_proteomics_data"

# import_pdc_file_name                = "pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
# import_pdc_file_name_without_suffix = "pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data"


import_pdc_file_name                = "pdc_AA_3_17_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
import_pdc_file_name_without_suffix = "pdc_AA_3_17_20_LATEST_dictionaries_for_pdc_proteomics_data"

import_pdc_aliquot_file_name                = "pdc_biospecimen_data_dicts.py"
import_pdc_aliquot_file_name_without_suffix = "pdc_biospecimen_data_dicts"

if import_pdc_biospecimen_aliquot_dictionaries == "YES":
   print("")
   print("Importing dictionaries from " + import_pdc_aliquot_file_name + " as pdc_aliquot ...")

   # Mon 2/10/20
   #
   import pdc_biospecimen_data_dicts as pdc_aliquot
   

if import_pdc_dictionaries == "YES":
   print("")
   print("Importing dictionaries from " + import_pdc_file_name + " as pdc ...")

   # Mon 2/10/20
   #
   # This does not work:
   # import import_file_name_without_suffix as pdc
   #
   # We must use an actual file name (without the suffix), like so:
   #
   # import AA_Mon_1_27_20_LATEST_dictionaries_for_pdc_proteomics_data as pdc
   # import AA_LATEST_dictionaries_for_pdc_proteomics_data as pdc
   # import dictionaries_for_pdc_proteomics_data as pdc
   import pdc_AA_3_17_20_LATEST_dictionaries_for_pdc_proteomics_data as pdc

   

def query_to_get_file_metadata():

  query = '''{
    fileMetadata(file_id: "00046804-1b57-11e9-9ac1-005056921935") {
      file_name
      file_size
      md5sum
      # deprecated element - Rajesh  Mon Jan 6th 2020
      #      folder_name
      file_location
      file_submitter_id
      fraction_number
      experiment_type
      aliquots {
        aliquot_id
        aliquot_submitter_id
        label

        sample_id
        sample_submitter_id
      }
    }
  }'''


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")                  
      print (json.dumps(jData, indent=4, sort_keys=True))
  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def query_to_get_summary_PDC_stats_across_all_programs():

  
  # get ?query={pdcDataStats {program study spectra protein project program peptide data_size data_label data_file}}

  query = '''{pdcDataStats {program study spectra protein project program peptide data_size data_label data_file}}'''

  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData_dict = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      print (json.dumps(jData_dict, indent=4, sort_keys=True))


# OUTPUT:      
# Answer returned:
#
# {
#     "data": {
#         "pdcDataStats": [
#             {
#                 "data_file": 55317,
#                 "data_label": "test4",
#                 "data_size": 12,
#                 "peptide": 991578,
#                 "program": 4,
#                 "project": 4,
#                 "protein": 14667,
#                 "spectra": 64720831,
#                 "study": 20
#             },
#             {
#                 "data_file": 55317,
#                 "data_label": "test4",
#                 "data_size": 12,
#                 "peptide": 991578,
#                 "program": 4,
#                 "project": 4,
#                 "protein": 14667,
#                 "spectra": 64720831,
#                 "study": 20
#             }
#         ]
#    }
# }
# (base) bash-3.2$ 
      
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# printing  a dict as a string:
#
# dict = {'Name': 'Zara', 'Age': 7};
# print "Equivalent String : %s" % str (dict)
# 
# When we run above program, it produces following result −
# 
# Equivalent String : {'Age': 7, 'Name': 'Zara'}


def query_to_get_all_cases(outfile):

  print ("query_to_get_all_cases() STARTED\n")

  fileout = open(outfile,'w')
  
  # get ?query={allCases {case_id case_submitter_id project_submitter_id disease_type primary_site}}

  query = '''{allCases {case_id case_submitter_id project_submitter_id disease_type primary_site}}'''

  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    

  fileout.write("\n")                
  fileout.write("%%%%%%%%%%%%%%\n")
  fileout.write("The query built is:\n")                    
  fileout.write(query)
  fileout.write("\n")
  fileout.write("%%%%%%%%%%%%%%\n")
  fileout.write("\n")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData_dict = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      
      # print (json.dumps(jData_dict, indent=4, sort_keys=True))

      fileout.write ("\n")
      fileout.write ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      fileout.write ("Answer returned:\n\n")
      
      fileout.write(json.dumps(jData_dict, indent=4, sort_keys=True))      

      fileout.write ("\n")
      fileout.write ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      
# what gets returned in jData_dict for this type of GraphQL PDC query, as shown by use of
#      for x in jData_dict:
#         print("%s: %s" % (x, jData_dict[x]) )
#
#  data: {'allCases': [{'case_id': '0065cb8d-63d6-11e8-bcf1-0a2705229b82', 'case_submitter_id': '05BR016', 'project_submitter_id': 'CPTAC-2', 'disease_type': 'Breast Invasive Carcinoma', 'primary_site': 'Breast'}, {'case_id': '0067a0e0-63d8-11e8-bcf1-0a2705229b82', 'case_submitter_id': 'TCGA-61-1911', 'project_submitter_id': 'CPTAC-TCGA', 'disease_type': 'Ovarian Serous Cystadenocarcinoma', 'primary_site': 'Ovary'}, ... <CONTINUED LIST> ] }
#
# We have one dict entry, with a key of "data" and a list of cases as its data item.

      top_level_dict_size = len(jData_dict)
      all_cases_entry_dict = jData_dict["data"]
      case_list = all_cases_entry_dict["allCases"]
      len_of_case_list = len(case_list)

      print("top_level_dict_size = " + str(top_level_dict_size) )
      print("len_of_case_list    = " + str(len_of_case_list) )      

# top_level_dict_size = 1
# len_of_case_list    = 1575

# Each case in the case_list is a dict in this format:
# {'case_id': '0065cb8d-63d6-11e8-bcf1-0a2705229b82', 'case_submitter_id': '05BR016', 'project_submitter_id': 'CPTAC-2', 'disease_type': 'Breast Invasive Carcinoma', 'primary_site': 'Breast'}

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("case walk thru\n")      
      case_num = 0
      for case in case_list:
         case_num += 1
         case_submitter_id = case["case_submitter_id"]
         print(str(case_num) + ") case_submitter_id = " + case_submitter_id)


      
  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

  fileout.close()
  print ("query_to_get_all_cases() ENDED\n")

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# get ?query={allPrograms {program_id program_submitter_id name sponsor start_date end_date program_manager projects {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type} cases{ case_id case_submitter_id project_submitter_id external_case_id tissue_source_site_code days_to_lost_to_followup disease_type index_date lost_to_followup primary_site count demographics{ demographic_id ethnicity gender demographic_submitter_id race cause_of_death days_to_birth days_to_death vital_status year_of_birth year_of_death } samples { sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type tumor_code tumor_code_id tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type } } project {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type }} diagnoses{ diagnosis_id tissue_or_organ_of_origin age_at_diagnosis primary_diagnosis tumor_grade tumor_stage diagnosis_submitter_id classification_of_tumor days_to_last_follow_up days_to_last_known_disease_status days_to_recurrence last_known_disease_status morphology progression_or_recurrence site_of_resection_or_biopsy vital_status days_to_birth days_to_death prior_malignancy ajcc_clinical_m ajcc_clinical_n ajcc_clinical_stage ajcc_clinical_t ajcc_pathologic_m ajcc_pathologic_n ajcc_pathologic_stage ajcc_pathologic_t ann_arbor_b_symptoms ann_arbor_clinical_stage ann_arbor_extranodal_involvement ann_arbor_pathologic_stage best_overall_response burkitt_lymphoma_clinical_variant cause_of_death circumferential_resection_margin colon_polyps_history days_to_best_overall_response days_to_diagnosis days_to_hiv_diagnosis days_to_new_event figo_stage hiv_positive hpv_positive_type hpv_status iss_stage laterality ldh_level_at_diagnosis ldh_normal_range_upper lymph_nodes_positive lymphatic_invasion_present method_of_diagnosis new_event_anatomic_site new_event_type overall_survival perineural_invasion_present prior_treatment progression_free_survival progression_free_survival_event residual_disease vascular_invasion_present year_of_diagnosis }} } }}


def query_to_get_programs():


  query = '''{allPrograms {program_id program_submitter_id name sponsor start_date end_date program_manager projects {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type} cases{ case_id case_submitter_id project_submitter_id external_case_id tissue_source_site_code days_to_lost_to_followup disease_type index_date lost_to_followup primary_site count demographics{ demographic_id ethnicity gender demographic_submitter_id race cause_of_death days_to_birth days_to_death vital_status year_of_birth year_of_death } samples { sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type tumor_code tumor_code_id tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type } } project {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type }} diagnoses{ diagnosis_id tissue_or_organ_of_origin age_at_diagnosis primary_diagnosis tumor_grade tumor_stage diagnosis_submitter_id classification_of_tumor days_to_last_follow_up days_to_last_known_disease_status days_to_recurrence last_known_disease_status morphology progression_or_recurrence site_of_resection_or_biopsy vital_status days_to_birth days_to_death prior_malignancy ajcc_clinical_m ajcc_clinical_n ajcc_clinical_stage ajcc_clinical_t ajcc_pathologic_m ajcc_pathologic_n ajcc_pathologic_stage ajcc_pathologic_t ann_arbor_b_symptoms ann_arbor_clinical_stage ann_arbor_extranodal_involvement ann_arbor_pathologic_stage best_overall_response burkitt_lymphoma_clinical_variant cause_of_death circumferential_resection_margin colon_polyps_history days_to_best_overall_response days_to_diagnosis days_to_hiv_diagnosis days_to_new_event figo_stage hiv_positive hpv_positive_type hpv_status iss_stage laterality ldh_level_at_diagnosis ldh_normal_range_upper lymph_nodes_positive lymphatic_invasion_present method_of_diagnosis new_event_anatomic_site new_event_type overall_survival perineural_invasion_present prior_treatment progression_free_survival progression_free_survival_event residual_disease vascular_invasion_present year_of_diagnosis }} } }} '''
  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData_dict = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      
      print (json.dumps(jData_dict, indent=4, sort_keys=True))

      # We have one dict entry, with a key of "data", and another dic as its data, at the top level.      

      top_level_dict_size = len(jData_dict)
      all_programs_entry_dict = jData_dict["data"]
      program_list = all_programs_entry_dict["allPrograms"]
      len_of_program_list = len(program_list)

      print("top_level_dict_size = " + str(top_level_dict_size) )
      print("len_of_program_list    = " + str(len_of_program_list) )      

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      print (json.dumps(jData_dict, indent=4, sort_keys=True))


      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("program walk thru\n")      
      num = 0
      for program in program_list:
         num += 1
         name       = program["name"]                  
         program_id = program["program_id"]         
         program_submitter_id = program["program_submitter_id"]
         print(str(num) + ") name = " + name)         
         print("     program_id = " + program_id + ", program_submitter_id = " + program_submitter_id)

  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()


# OUTPUT:
# program walk thru
# 
# 1) program_submitter_id = Clinical Proteomic Tumor Analysis Consortium
# 2) program_submitter_id = PG25730263
# 3) program_submitter_id = International Cancer Proteogenome Consortium
# 4) program_submitter_id = Pediatric Brain Tumor Atlas - CBTTC
# 5) program_submitter_id = GMKFPRP

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


def print_global_dicts_and_lists():

   print ("print_global_dicts_and_lists() STARTED\n")                            

   
   print("")   
   print("GLOBAL_PROGRAM_ID_LIST =\n" + str(GLOBAL_PROGRAM_ID_LIST) )
   print("")

   print("")   
   print("GLOBAL_PROGRAM_NAME_LIST =\n" + str(GLOBAL_PROGRAM_NAME_LIST) )
   print("")
   
   print("")   
   print("GLOBAL_PROJECT_ID_LIST =\n" + str(GLOBAL_PROJECT_ID_LIST) )
   print("")

   print("")   
   print("GLOBAL_PROJECT_NAME_LIST =\n" + str(GLOBAL_PROJECT_NAME_LIST) )
   print("")

   print("")   
   print("GLOBAL_STUDY_ID_LIST =\n" + str(GLOBAL_STUDY_ID_LIST) )
   print("")

   print("")   
   print("GLOBAL_STUDY_SUBMITTER_ID_LIST =\n" + str(GLOBAL_STUDY_SUBMITTER_ID_LIST) )
   print("")

   print("")   
   print("GLOBAL_STUDY_SUBMITTER_NAME_LIST =\n" + str(GLOBAL_STUDY_SUBMITTER_NAME_LIST) )
   print("")


   print("")   
   print("GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT =\n" + str(GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT) )
   print("")

   print("")   
   print("GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") program_id=" + key + ", project_id_list=" + value)
   print("")

   print("")   
   print("GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") program_id=" + key + ", project_name_list=" + value)
   print("")


   print("")   
   print("GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT.items():
     counter += 1
     print(str(counter) + ") program_id=" + key + ", program_name=" + value)
   print("")

   print("")   
   print("GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT.items():
     counter += 1
     print(str(counter) + ") program_name=" + key + ", program_id=" + value)
   print("")

   print("")   
   print("GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT.items():
     counter += 1
     print(str(counter) + ") project_id=" + key + ", project_name=" + value)
   print("")

   print("")   
   print("GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT.items():
     counter += 1
     print(str(counter) + ") project_name=" + key + ", project_id=" + value)
   print("")

   print("")   
   print("GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") program_id=" + key + ", study_id_list=" + value)
   print("")

   print("")   
   print("GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") program_id=" + key + ", study_submitter_id_list=" + value)
   print("")

   print("")   
   print("GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") project_id=" + key + ", study_id_list=" + value)
   print("")

   print("")   
   print("GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT contents:")
   counter = 0
   for key, value in GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.items():
     counter += 1
     print(str(counter) + ") project_id=" + key + ", study_submitter_id_list=" + value)
   print("")

   
   print("")   
   print("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT =\n" + str(GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT) )
   print("")
   print("")   
   print("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT contents:")
   counter = 0
   for key, value in GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT.items():
     counter += 1
     print(str(counter) + ") study_id=" + key + ", study_submitter_name=" + value)
   print("")

   
   print("")   
   print("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT =\n" + str(GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT) )
   print("")
   print("")   
   print("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT contents:")
   counter = 0
   for key, value in GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT.items():
     counter += 1
     print(str(counter) + ") study_id=" + key + ", study_submitter_id=" + value)
   print("")


   print("")   
   print("GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT contents:")
   counter = 0
   for key, value in GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT.items():
     counter += 1
     print(str(counter) + ") study_id=" + key + ", study_name=" + value)
   print("")

   print("")   
   print("GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT contents:")
   counter = 0
   for key, value in GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT.items():
     counter += 1
     print(str(counter) + ") study_id=" + key + ", study_name=" + value)
   print("")



   # 3/9/20
   print("")   
   print("GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT =\n" + str(GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT) )
   print("")
   print("")   
   counter = 0
   for key, value in GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT.items():
     counter += 1
     print(str(counter) + ") study_name=" + key + ", study_submitter_id=" + value)
   print("")

   # 3/9/20
   print("")   
   print("GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT =\n" + str(GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT) )
   print("")
   print("")   
   print("GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT contents:")
   counter = 0
   for key, value in GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT.items():
     counter += 1
     print(str(counter) + ") study_submitter_id=" + key + ", study_name=" + value)
   print("")


   
   print ("print_global_dicts_and_lists() ENDED\n")


   
# %%%%%%%%%%%%%%%%%%%%%%      

# Python Programming provides us a very simple way to deal with multi-line strings. To print a multi-line string in Python we use triple quotes. By triple quotes, I mean set of quotation marks enclosed within each other. These quotation marks could either be double quotes or single quotes.
#
# Example of Triple Quote.
# 
# Let me show you, how you can use the triplet of quotation marks to print a multi-line string in Python Programming.
#
# print(“ “ “ What’s up Internet!
# I’m Manish from RebellionRider.com. 
# Keep watching these tutorials.
# And, you will be the master of “Python Programming” very soon. ” ” ”)
# 
# Triple Quotation and the Interpreter
# Let’s understand why didn’t we get the syntax error with the above code? Once Python interpreter encounters the opening triplet of the quotation mark, it understands that the user is trying to enter a multi-line string. In multi-line string mode the interpreter halts the translation of the statement until it finds the closing triplet of the quotation mark. As a result of this, we don’t get any errors.

# Can we use a triplet of single quotes instead of double quotes?
# 
# Of course, you can! You can use either single quotes or double quotes. The choice is yours. Just make sure to use the same opening and closing quotes to enclose your multi-line string.

# %%%%%%%%%%%%%%%%%%%%%%


# GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT =
# {'b8da9eeb-57b8-11e8-b07a-00a098d917f8': 'TCGA_Breast_Cancer_Proteome', 'b93bb1e9-57b8-11e8-b07a-00a098d917f8': 'TCGA_Breast_Cancer_Phosphoproteome', 'b998098f-57b8-11e8-b07a-00a098d917f8': 'TCGA_Colon_Cancer_Proteome', 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8': 'TCGA_Ovarian_JHU_Glycoproteome', 'ba4e17a5-57b8-11e8-b07a-00a098d917f8': 'TCGA_Ovarian_JHU_Proteome', 'baa8ae46-57b8-11e8-b07a-00a098d917f8': 'TCGA_Ovarian_PNNL_Proteome', 'bb076b33-57b8-11e8-b07a-00a098d917f8': 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive', 'ad18f195-f3c0-11e8-a44b-0a9c39d33490': 'PCT_SWATH_Kidney', 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe': 'HBV-Related Hepatocellular Carcinoma - Proteome', '37dfda3f-1132-11ea-9bfa-0a42f3c845fe': 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', '58be6db8-f1f7-11e9-9a07-0a80fada099c': 'Pediatric Brain Cancer Pilot Study - Proteome', '58be6cbb-f1f7-11e9-9a07-0a80fada099c': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome'}


# GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT contents:
# 1) study_id=b8da9eeb-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Breast_Cancer_Proteome
# 2) study_id=b93bb1e9-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Breast_Cancer_Phosphoproteome
# 3) study_id=b998098f-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Colon_Cancer_Proteome
# 4) study_id=b9f2ccc5-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Ovarian_JHU_Glycoproteome
# 5) study_id=ba4e17a5-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Ovarian_JHU_Proteome
# 6) study_id=baa8ae46-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Ovarian_PNNL_Proteome
# 7) study_id=bb076b33-57b8-11e8-b07a-00a098d917f8, study_submitter_name=TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive
# 8) study_id=ad18f195-f3c0-11e8-a44b-0a9c39d33490, study_submitter_name=PCT_SWATH_Kidney
# 9) study_id=f14e4c61-106f-11ea-9bfa-0a42f3c845fe, study_submitter_name=HBV-Related Hepatocellular Carcinoma - Proteome
# 10) study_id=37dfda3f-1132-11ea-9bfa-0a42f3c845fe, study_submitter_name=HBV-Related Hepatocellular Carcinoma - Phosphoproteome
# 11) study_id=58be6db8-f1f7-11e9-9a07-0a80fada099c, study_submitter_name=Pediatric Brain Cancer Pilot Study - Proteome
# 12) study_id=58be6cbb-f1f7-11e9-9a07-0a80fada099c, study_submitter_name=Pediatric Brain Cancer Pilot Study - Phosphoproteome


# GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT =
# {'b8da9eeb-57b8-11e8-b07a-00a098d917f8': 'S015-1', 'b93bb1e9-57b8-11e8-b07a-00a098d917f8': 'S015-2', 'b998098f-57b8-11e8-b07a-00a098d917f8': 'S016-1', 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8': 'S020-1', 'ba4e17a5-57b8-11e8-b07a-00a098d917f8': 'S020-2', 'baa8ae46-57b8-11e8-b07a-00a098d917f8': 'S020-3', 'bb076b33-57b8-11e8-b07a-00a098d917f8': 'S020-4', 'ad18f195-f3c0-11e8-a44b-0a9c39d33490': 'ST25730263', 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe': 'HBV-Related Hepatocellular Carcinoma - Proteome', '37dfda3f-1132-11ea-9bfa-0a42f3c845fe': 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', '58be6db8-f1f7-11e9-9a07-0a80fada099c': 'Pediatric Brain Cancer Pilot Study - Proteome', '58be6cbb-f1f7-11e9-9a07-0a80fada099c': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome'}


# GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT contents:
# 1) study_id=b8da9eeb-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S015-1
# 2) study_id=b93bb1e9-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S015-2
# 3) study_id=b998098f-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S016-1
# 4) study_id=b9f2ccc5-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S020-1
# 5) study_id=ba4e17a5-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S020-2
# 6) study_id=baa8ae46-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S020-3
# 7) study_id=bb076b33-57b8-11e8-b07a-00a098d917f8, study_submitter_id=S020-4
# 8) study_id=ad18f195-f3c0-11e8-a44b-0a9c39d33490, study_submitter_id=ST25730263
# 9) study_id=f14e4c61-106f-11ea-9bfa-0a42f3c845fe, study_submitter_id=HBV-Related Hepatocellular Carcinoma - Proteome
# 10) study_id=37dfda3f-1132-11ea-9bfa-0a42f3c845fe, study_submitter_id=HBV-Related Hepatocellular Carcinoma - Phosphoproteome
# 11) study_id=58be6db8-f1f7-11e9-9a07-0a80fada099c, study_submitter_id=Pediatric Brain Cancer Pilot Study - Proteome
# 12) study_id=58be6cbb-f1f7-11e9-9a07-0a80fada099c, study_submitter_id=Pediatric Brain Cancer Pilot Study - Phosphoproteome


#  ?query={ study (study_id: "{study_id}") { study_id study_submitter_id program_id project_id study_name program_name project_name disease_type primary_site analytical_fraction experiment_type cases_count aliquots_count filesCount { data_category file_type files_count } } }


def query_to_get_info_on_one_study(study_id):

  print ("query_to_get_info_on_one_study() STARTED\n")                                  

  query = '{ study (study_id: "' + study_id + '"'  +  ''') { study_id study_submitter_id program_id project_id study_name program_name project_name disease_type primary_site analytical_fraction experiment_type cases_count aliquots_count filesCount { data_category file_type files_count } } }'''

  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned - for info for one study:\n")
      print("study_id used = " + study_id)
      print (json.dumps(jData, indent=4, sort_keys=True))
  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

  print ("query_to_get_info_on_one_study() ENDED\n")

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

  
def query_to_get_info_on_one_study_and_store_in_outfile(fileout, study_id):

  query = '{ study (study_id: "' + study_id + '"'  +  ''') { study_id study_submitter_id program_id project_id study_name program_name project_name disease_type primary_site analytical_fraction experiment_type cases_count aliquots_count filesCount { data_category file_type files_count } } }'''

  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData = json.loads(response.content)
      fileout.write("\n")
      fileout.write("%%%%%%%\n")
      fileout.write("\n")      
      fileout.write(json.dumps(jData, indent=4, sort_keys=True))
      fileout.write("\n")
      fileout.write("\n")      
      fileout.write("%%%%%%%\n")
      fileout.write("\n")    
  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

  # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%  


def query_to_fill_in_global_lists_and_dictionaries_using_allPrograms():

  query = '''{allPrograms {program_id program_submitter_id name sponsor start_date end_date program_manager projects {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type} } } }'''


  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData_dict = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      
      print (json.dumps(jData_dict, indent=4, sort_keys=True))

      # We have one dict entry, with a key of "data", and another dic as its data, at the top level.
      
      top_level_dict_size = len(jData_dict)
      all_programs_entry_dict = jData_dict["data"]
      program_list = all_programs_entry_dict["allPrograms"]
      len_of_program_list = len(program_list)

      print("top_level_dict_size = " + str(top_level_dict_size) )
      print("len_of_program_list    = " + str(len_of_program_list) )      

      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("program walk thru\n")      
      num = 0
      print("")               
      for program_dict in program_list:
         studies_list  = []
         projects_list = []
         
         num += 1
         program_name       = program_dict["name"]                  
         program_id         = program_dict["program_id"]
         GLOBAL_PROGRAM_ID_LIST.append(program_id)
         GLOBAL_PROGRAM_NAME_LIST.append(program_name)
         GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT[program_id] = program_name
         GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT[program_name] = program_id

         program_submitter_id = program_dict["program_submitter_id"]
         
         projects_list = program_dict.get("projects", "null")
         # Note, for one program there are zero projects. However, instead of getting a null back,
         # we get back an empty list. So - have to check for that, too.
         
         print(str(num) + ") program_name = " + program_name)         
         print("     program_id = " + program_id + ", program_submitter_id = " + program_submitter_id)
         print("     projects list = " + str(projects_list) )
         project_num = 0

         if projects_list != "null" and len(projects_list) > 0:
           print("     Projects:")                              
           for project_dict in projects_list:             
              project_num += 1
              project_id = project_dict["project_id"]              
              project_name = project_dict["name"]
              GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT[project_id] = project_name
              GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT[project_name] = project_id
              GLOBAL_PROJECT_ID_LIST.append(project_id)
              GLOBAL_PROJECT_NAME_LIST.append(project_name)

              old_list = GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT.get(program_id, "null")
              if old_list != "null":
                new_list = old_list + "::" + project_id
              else:
                new_list = project_id
              GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT[program_id] = new_list

              old_list = GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT.get(program_id, "null")
              if old_list != "null":
                new_list = old_list + "::" + project_name
              else:
                new_list = project_name
              GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT[program_id] = new_list
                            
              print("     " + str(project_num) + ") project_name = " + project_name )
              studies_list = project_dict.get("studies", "null")
              print("     " + "studies_list = " + str(studies_list) )

              if studies_list != None:
               for study_dict in studies_list:
                study_id                  = study_dict["study_id"]
                submitter_id_name         = study_dict["submitter_id_name"]                              
                study_submitter_id        = study_dict["study_submitter_id"]
                #
                # study_name is not returned by this particular query
                # study_name                = study_dict["study_name"]
                #
                # GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]           = study_name
                #
                GLOBAL_STUDY_ID_LIST.append(study_id)
                GLOBAL_STUDY_SUBMITTER_NAME_LIST.append(submitter_id_name)
                GLOBAL_STUDY_SUBMITTER_ID_LIST.append(study_submitter_id)                

                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id] = submitter_id_name
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]   = study_submitter_id

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list
         else:
              print("     " + "This program has ZERO projects.")
              print("     " + "So - we check for studies at the program level.")
              studies_list = program_dict.get("studies")
              print("     " + "studies_list at program level = " + str(studies_list) )
              if studies_list != None:
               for study_dict in studies_list:
                study_id                  = study_dict["study_id"]
                submitter_id_name         = study_dict["submitter_id_name"]                              
                study_submitter_id        = study_dict["study_submitter_id"]

                # This API call does not return study_name.
                # study_name                = study_dict["study_name"]                
                #
                GLOBAL_STUDY_ID_LIST.append(study_id)
                GLOBAL_STUDY_SUBMITTER_NAME_LIST.append(submitter_id_name)
                GLOBAL_STUDY_SUBMITTER_ID_LIST.append(study_submitter_id)                

                # This API call does not return study_name.                
                # GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]           = study_name
                
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id] = submitter_id_name
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]   = study_submitter_id


                # 3/9/20
                # study_name is not returned by this particular query                
                # GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT[study_name]   = study_submitter_id
                # 3/9/20
                # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT[study_submitter_id]   = study_name
                

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list
              
         print("")

  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

# example projects list returned:
#     projects list = [{'project_id': 'edb4ca56-f1e8-11e9-9a07-0a80fada099c', 'project_submitter_id': 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study', 'name': 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study', 'studies': [{'study_id': '58be6db8-f1f7-11e9-9a07-0a80fada099c', 'submitter_id_name': 'Pediatric Brain Cancer Pilot Study - Proteome', 'study_submitter_id': 'Pediatric Brain Cancer Pilot Study - Proteome', 'analytical_fraction': 'Proteome', 'experiment_type': 'TMT11', 'acquisition_type': 'DDA'}, {'study_id': '58be6cbb-f1f7-11e9-9a07-0a80fada099c', 'submitter_id_name': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome', 'study_submitter_id': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome', 'analytical_fraction': 'Phosphoproteome', 'experiment_type': 'TMT11', 'acquisition_type': 'DDA'}]}]

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def query_to_fill_in_global_lists_and_dictionaries_using_programsProjectsStudies():

  query = '''{programsProjectsStudies {program_id program_submitter_id name sponsor start_date end_date program_manager projects {project_id project_submitter_id name studies { study_id study_submitter_id submitter_id_name study_name program_name project_name program_id project_id project_submitter_id disease_type primary_site analytical_fraction experiment_type acquisition_type cases_count aliquots_count} }}}'''


  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    

  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData_dict = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")
      
      print (json.dumps(jData_dict, indent=4, sort_keys=True))

      # We have one dict entry, with a key of "data", and another dic as its data, at the top level.

      top_level_dict_size = len(jData_dict)
      all_programs_entry_dict = jData_dict["data"]
      program_list = all_programs_entry_dict["programsProjectsStudies"]
      len_of_program_list = len(program_list)

      print("top_level_dict_size = " + str(top_level_dict_size) )
      print("len_of_program_list    = " + str(len_of_program_list) )      

      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("program walk thru\n")      
      num = 0
      print("")               
      for program_dict in program_list:
         studies_list  = []
         projects_list = []
         
         num += 1
         program_name       = program_dict["name"]                  
         program_id         = program_dict["program_id"]
         GLOBAL_PROGRAM_ID_LIST.append(program_id)
         GLOBAL_PROGRAM_NAME_LIST.append(program_name)
         GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT[program_id] = program_name
         GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT[program_name] = program_id

         program_submitter_id = program_dict["program_submitter_id"]
         
         projects_list = program_dict.get("projects", "null")
         # Note, for one program there are zero projects. However, instead of getting a null back,
         # we get back an empty list. So - have to check for that, too.
         
         print(str(num) + ") program_name = " + program_name)         
         print("     program_id = " + program_id + ", program_submitter_id = " + program_submitter_id)
         print("     projects list = " + str(projects_list) )
         project_num = 0

         if projects_list != "null" and len(projects_list) > 0:
           print("     Projects:")                              
           for project_dict in projects_list:             
              project_num += 1
              project_id = project_dict["project_id"]              
              project_name = project_dict["name"]
              GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT[project_id] = project_name
              GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT[project_name] = project_id
              GLOBAL_PROJECT_ID_LIST.append(project_id)
              GLOBAL_PROJECT_NAME_LIST.append(project_name)

              old_list = GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT.get(program_id, "null")
              if old_list != "null":
                new_list = old_list + "::" + project_id
              else:
                new_list = project_id
              GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT[program_id] = new_list

              old_list = GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT.get(program_id, "null")
              if old_list != "null":
                new_list = old_list + "::" + project_name
              else:
                new_list = project_name
              GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT[program_id] = new_list
                            
              print("     " + str(project_num) + ") project_name = " + project_name )
              studies_list = project_dict.get("studies", "null")
              print("     " + "studies_list = " + str(studies_list) )

              if studies_list != None:
               for study_dict in studies_list:
                study_id                  = study_dict["study_id"]
                submitter_id_name         = study_dict["submitter_id_name"]                              
                study_submitter_id        = study_dict["study_submitter_id"]
                study_name                = study_dict.get("study_name")
                if study_name == None:
                  study_name = "not_present"
                #
                GLOBAL_STUDY_ID_LIST.append(study_id)
                GLOBAL_STUDY_SUBMITTER_NAME_LIST.append(submitter_id_name)
                GLOBAL_STUDY_SUBMITTER_ID_LIST.append(study_submitter_id)                

                print("DEBUG1: study_name = '" + str(study_name) +"'")

                # This API call apparently does not return study_name; we get it elsewhere, thru another study call.
                # GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id] = study_name

                
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id] = submitter_id_name
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]   = study_submitter_id

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list
         else:
              print("     " + "This program has ZERO projects.")
              print("     " + "So - we check for studies at the program level.")
              studies_list = program_dict.get("studies")
              print("     " + "studies_list at program level = " + str(studies_list) )
              if studies_list != None:
               for study_dict in studies_list:
                study_id                  = study_dict["study_id"]
                submitter_id_name         = study_dict["submitter_id_name"]                              
                study_submitter_id        = study_dict["study_submitter_id"]

                study_name                = study_dict.get("study_name")
                if study_name == None:
                  study_name = "not_present"
                
                print("DEBUG2: study_name = '" + str(study_name) +"'")
                
                GLOBAL_STUDY_ID_LIST.append(study_id)
                GLOBAL_STUDY_SUBMITTER_NAME_LIST.append(submitter_id_name)
                GLOBAL_STUDY_SUBMITTER_ID_LIST.append(study_submitter_id)                

                # This API call apparently does not return study_name; we get it elsewhere, thru another study call.
                # GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]           = study_name                
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id] = submitter_id_name
                GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]   = study_submitter_id

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + study_id
                else:
                  new_list = study_id
                GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT[program_id] = new_list

                old_list = GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.get(program_id, "null")
                if old_list != "null":
                   new_list = old_list + "::" + submitter_id_name
                else:
                  new_list = submitter_id_name
                GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT[program_id] = new_list
              
         print("")

  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()

# example projects list returned:
#     projects list = [{'project_id': 'edb4ca56-f1e8-11e9-9a07-0a80fada099c', 'project_submitter_id': 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study', 'name': 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study', 'studies': [{'study_id': '58be6db8-f1f7-11e9-9a07-0a80fada099c', 'submitter_id_name': 'Pediatric Brain Cancer Pilot Study - Proteome', 'study_submitter_id': 'Pediatric Brain Cancer Pilot Study - Proteome', 'analytical_fraction': 'Proteome', 'experiment_type': 'TMT11', 'acquisition_type': 'DDA'}, {'study_id': '58be6cbb-f1f7-11e9-9a07-0a80fada099c', 'submitter_id_name': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome', 'study_submitter_id': 'Pediatric Brain Cancer Pilot Study - Phosphoproteome', 'analytical_fraction': 'Phosphoproteome', 'experiment_type': 'TMT11', 'acquisition_type': 'DDA'}]}]


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def store_global_dicts_and_lists_to_python_file(outfile_name):

   print ("store_global_dicts_and_lists_to_python_file(outfile) STARTED\n")

   file_out = open(outfile_name,'w')

   file_out.write("\n" )
   file_out.write("# Initialization of dictionaries\n" )
   file_out.write("\n" )
   file_out.write("\n" )   
   file_out.write("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT = {}" + "\n" )


   # 3/9/20
   file_out.write("GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT = {}" + "\n" )
   # 3/9/20   
   file_out.write("GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT = {}" + "\n" )   

      
   file_out.write("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT   = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT  = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT  = {}" + "\n" )

   file_out.write("\n" )   
   file_out.write("GLOBAL_STUDY_NAME_TO_DISEASE_TYPE_DICT      = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_NAME_TO_EXPERIMENT_TYPE_DICT   = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_NAME_TO_PRIMARY_SITE_DICT      = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_NAME_TO_CASES_COUNT_DICT       = {}" + "\n" )
   file_out.write("GLOBAL_STUDY_NAME_TO_ALIQUOTS_COUNT_DICT    = {}" + "\n" )

   file_out.write("\n" )   
   file_out.write("GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT  = {}" + "\n" )
   file_out.write("GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT  = {}" + "\n" )

   file_out.write("\n" )   
   file_out.write("GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT  = {}" + "\n" )   
   file_out.write("GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT  = {}" + "\n" )   

   file_out.write("\n" )
   file_out.write("# possibly one to many\n" )            
   file_out.write("GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT   = {}" + "\n" )
   file_out.write("# possibly one to many\n" )            
   file_out.write("GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT   = {}" + "\n" )   

   file_out.write("\n" )
   file_out.write("# possibly one to many\n" )         
   file_out.write("GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT   = {}" + "\n" )
   file_out.write("# possibly one to many\n" )            
   file_out.write("GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT   = {}" + "\n" )   

   file_out.write("\n" )   
   file_out.write("# possibly one to many\n" )         
   file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT   = {}" + "\n" )
   file_out.write("# possibly one to many\n" )         
   file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT   = {}" + "\n" )   

   file_out.write("\n" )   
   file_out.write("# %%%%%%%%%%%%%%%%%%%\n" )   
   file_out.write("\n" )
   file_out.write("\n" )      

   file_out.write("GLOBAL_PROGRAM_ID_LIST = " + str(GLOBAL_PROGRAM_ID_LIST) + "\n" )
   file_out.write("\n" )               
   file_out.write("GLOBAL_PROGRAM_NAME_LIST = " + str(GLOBAL_PROGRAM_NAME_LIST) + "\n" )
   file_out.write("\n" )               

   file_out.write("GLOBAL_PROJECT_ID_LIST = " + str(GLOBAL_PROJECT_ID_LIST) + "\n" )
   file_out.write("\n" )               
   file_out.write("GLOBAL_PROJECT_NAME_LIST = " + str(GLOBAL_PROJECT_NAME_LIST) + "\n" )
   file_out.write("\n" )               

   
   file_out.write("GLOBAL_STUDY_ID_LIST = " + str(GLOBAL_STUDY_ID_LIST) + "\n" )
   file_out.write("\n" )               
   file_out.write("GLOBAL_STUDY_NAME_LIST = " + str(GLOBAL_STUDY_NAME_LIST) + "\n" )
   file_out.write("\n" )               
   file_out.write("GLOBAL_STUDY_SUBMITTER_ID_LIST = " + str(GLOBAL_STUDY_SUBMITTER_ID_LIST) + "\n" )
   file_out.write("\n" )               
   file_out.write("GLOBAL_STUDY_SUBMITTER_NAME_LIST = " + str(GLOBAL_STUDY_SUBMITTER_NAME_LIST) + "\n" )         
   file_out.write("\n" )            


   file_out.write("\n" )
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")      
   file_out.write("\n")
   file_out.write("\n" )         

   counter = 0
   for k, v in GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0 
   for k, v in GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROGRAM_NAME_TO_PROGRAM_ID_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROGRAM_ID_TO_PROJECT_ID_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROGRAM_ID_TO_PROJECT_NAME_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_PROGRAM_ID_TO_STUDY_ID_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_PROGRAM_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")
   
   counter = 0 
   for k, v in GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_NAME_TO_PROJECT_ID_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")
   
   counter = 0    
   for k, v in GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_ID_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0   
   for k, v in GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   # 3/9/20
   counter = 0   
   for k, v in GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   # 3/9/20   
   counter = 0   
   for k, v in GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_DISEASE_TYPE_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_DISEASE_TYPE_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_EXPERIMENT_TYPE_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_EXPERIMENT_TYPE_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_PRIMARY_SITE_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_PRIMARY_SITE_DICT['" + k + "'] = '" + v + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_CASES_COUNT_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_CASES_COUNT_DICT['" + k + "'] = '" + str(v) + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")

   counter = 0    
   for k, v in GLOBAL_STUDY_NAME_TO_ALIQUOTS_COUNT_DICT.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      file_out.write("# " + str(counter) + "\n")
      file_out.write("GLOBAL_STUDY_NAME_TO_ALIQUOTS_COUNT_DICT['" + k + "'] = '" + str(v) + "'" + "\n")
   file_out.write("\n")
   file_out.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")   
   file_out.write("\n")
   
   file_out.close()

   print("The PDC data retrieved has been stored in Python dictionaries in the file")
   print("          " + outfile_name)
   print("")
   print ("store_global_dicts_and_lists_to_python_file(outfile) ENDED\n")
  
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


      # SAMPLE QUERY:         
      # The query built is:
      # { study (study_id: "b8da9eeb-57b8-11e8-b07a-00a098d917f8") { study_id study_submitter_id program_id project_id study_name program_name project_name } }
      # 
      # SAMPLE QUERY RESPONSE RETURNED:         
      # {
      #    "data": {
      #        "study": [
      #          {
      #                 "program_id": "c3408a52-f1e8-11e9-9a07-0a80fada099c",
      #                 "program_name": "International Cancer Proteogenome Consortium",
      #                 "project_id": "095cf1fe-0f93-11ea-9bfa-0a42f3c845fe",
      #                 "project_name": "Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma",
      #                 "study_id": "f14e4c61-106f-11ea-9bfa-0a42f3c845fe",
      #                 "study_name": "HBV-Related Hepatocellular Carcinoma - Proteome",
      #                 "study_submitter_id": "HBV-Related Hepatocellular Carcinoma - Proteome"
      #           }
      #        ]
      #    }
      # }
      #
      # study_data_dict = 
      # {'study': [{'study_id': 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe', 'study_submitter_id': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_id': 'c3408a52-f1e8-11e9-9a07-0a80fada099c', 'project_id': '095cf1fe-0f93-11ea-9bfa-0a42f3c845fe', 'study_name': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_name': 'International Cancer Proteogenome Consortium', 'project_name': 'Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma'}]}


      # study_field_dict_list = 
      # [{'study_id': 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe', 'study_submitter_id': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_id': 'c3408a52-f1e8-11e9-9a07-0a80fada099c', 'project_id': '095cf1fe-0f93-11ea-9bfa-0a42f3c845fe', 'study_name': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_name': 'International Cancer Proteogenome Consortium', 'project_name': 'Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma'}]


      # study_field_dict = 
      # {'study_id': 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe', 'study_submitter_id': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_id': 'c3408a52-f1e8-11e9-9a07-0a80fada099c', 'project_id': '095cf1fe-0f93-11ea-9bfa-0a42f3c845fe', 'study_name': 'HBV-Related Hepatocellular Carcinoma - Proteome', 'program_name': 'International Cancer Proteogenome Consortium', 'project_name': 'Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma'}



#  ?query={programsProjectsStudies {program_id program_submitter_id name sponsor start_date end_date program_manager projects {project_id project_submitter_id name studies { study_id study_submitter_id submitter_id_name study_name program_name project_name program_id project_id project_submitter_id disease_type primary_site analytical_fraction experiment_type acquisition_type cases_count aliquots_count} }}}

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
      

def query_to_fill_in_study_name_disease_type_primary_site_cases_count_aliquots_count_in_global_lists_and_dictionaries_using_study():

    print ("query_to_fill_in_study_name_disease_type_primary_site_cases_count_aliquots_count_in_global_lists_and_dictionaries_using_study() STARTED\n")

    GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT.clear()
    GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT.clear()

    global GLOBAL_STUDY_NAME_LIST
    
    # We retrieve and store into a GLOBAL dict the study_name and other data for all studies, using the study_id that we have.

    print("GLOBAL_STUDY_ID_LIST: " + str(GLOBAL_STUDY_ID_LIST) )

    study_counter = 0    
    for study_id in GLOBAL_STUDY_ID_LIST:
       study_counter += 1      
       study_submitter_name = GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id]
       study_submitter_id   = GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]
       print("")
       print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
       print(str(study_counter) + ") study_id = " + study_id)
       # print("  study_name           = " + study_name)                            
       print("  study_submitter_name = " + study_submitter_name)
       print("  study_submitter_id   = " + study_submitter_id)
      

       # Note: I do not ask for a return of filesCount - which is a dictionary itself with 3 members, according to the PDC Study API page. We don't need it at present.
       # See
       #   https://pdc.esacinc.com/data-dictionary/publicapi-documentation/#!/Study/study
       #
       query = '{ study (study_id: "' + study_id + '"'  +  ''') { study_id study_submitter_id program_id project_id study_name program_name project_name cases_count aliquots_count experiment_type disease_type primary_site} }'''       

       print("")                
       print("%%%%%%%%%%%%%%")
       print("The query built is:")                    
       print(query)
       print("")
       print("%%%%%%%%%%%%%%")
       print("")                    

       response = requests.post(url, json={'query': query})

       if(response.ok):
         #If the response was OK then print the returned JSON
         jData_dict = json.loads(response.content)

         print(json.dumps(jData_dict, indent=4, sort_keys=True))         

         study_data_dict = jData_dict["data"]

         # print("")                           
         # print("study_data_dict = ")
         # print(study_data_dict)
         # print("")

         study_field_dict_list = study_data_dict["study"]
         
         # print("")                           
         # print("study_field_dict_list = ")
         # print(study_field_dict_list)
         # print("")

         # There is only one item in study_field_dict_list, a single dict having the study field info.
         #
         study_field_dict = study_field_dict_list[0]

         # Example":
         # study_field_dict = 
         #  {'study_id': 'b998098f-57b8-11e8-b07a-00a098d917f8', 'study_submitter_id': 'S016-1', 'program_id': '10251935-5540-11e8-b664-00a098d917f8', 'project_id': '48af5040-5546-11e8-b664-00a098d917f8', 'study_name': 'TCGA_Colon_Cancer_Proteome', 'program_name': 'Clinical Proteomic Tumor Analysis Consortium', 'project_name': 'CPTAC2 Retrospective'}

         
         print("")                           
         print("study_field_dict = ")
         print(study_field_dict)
         print("")

         study_name = study_field_dict["study_name"]
         experiment_type = study_field_dict["experiment_type"]
         disease_type = study_field_dict["disease_type"]
         aliquots_count = study_field_dict["aliquots_count"]
         cases_count = study_field_dict["cases_count"]
         primary_site = study_field_dict["primary_site"]                

         print("study_name      = " + study_name)
         print("experiment_type = " + experiment_type)
         print("disease_type    = " + disease_type)
         print("cases_count     = " + str(cases_count) )
         print("aliquots_count  = " + str(aliquots_count) )
         print("primary_site    = " + str(primary_site) )
         print("")

         GLOBAL_STUDY_NAME_LIST.append(study_name)       
         #
         GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id] = study_name
         GLOBAL_STUDY_NAME_TO_EXPERIMENT_TYPE_DICT[study_name] = experiment_type
         GLOBAL_STUDY_NAME_TO_DISEASE_TYPE_DICT[study_name] = disease_type
         GLOBAL_STUDY_NAME_TO_CASES_COUNT_DICT[study_name] = cases_count
         GLOBAL_STUDY_NAME_TO_ALIQUOTS_COUNT_DICT[study_name] = aliquots_count         
         GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT[study_name] = study_id

         # 3/9/20
         GLOBAL_STUDY_NAME_TO_STUDY_SUBMITTER_ID_DICT[study_name] = study_submitter_id
         # 3/9/20         
         GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT[study_submitter_id] = study_name
         

       else:
         # If response code is not ok (200), print the resulting http error code with description
         print ("Answer returned: ERROR - status shown below.\n")                          
         response.raise_for_status()
         

    print ("query_to_fill_in_study_name_disease_type_primary_site_cases_count_aliquots_count_in_global_lists_and_dictionaries_using_study() ENDED\n")         

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  
def query_to_get_info_on_all_studies():

    print ("query_to_get_info_on_all_studies() STARTED\n")

    study_output_file = "study_output_info_file.txt"
    outfile = open(study_output_file,'w')

    print("pdc.GLOBAL_STUDY_ID_LIST: " + str(pdc.GLOBAL_STUDY_ID_LIST) )
    
    study_counter = 0
    for study_id in pdc.GLOBAL_STUDY_ID_LIST:
       study_counter += 1      
       study_submitter_name = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id]
       study_submitter_id   = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]
       study_name           = pdc.GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]
       print("")
       print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
       print(str(study_counter) + ") study_id = " + study_id)
       print("  study_name           = " + study_name)                            
       print("  study_submitter_name = " + study_submitter_name)
       print("  study_submitter_id   = " + study_submitter_id)

       outfile.write("\n")
       outfile.write("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
       outfile.write(str(study_counter) + ") study_id = " + study_id + "\n")
       outfile.write("  study_name           = " + study_name + "\n")              
       outfile.write("  study_submitter_name = " + study_submitter_name + "\n")
       outfile.write("  study_submitter_id   = " + study_submitter_id + "\n")

       query_to_get_info_on_one_study_and_store_in_outfile(outfile, study_id)
    print("")  
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("")

    outfile.close()

    print("output has been stored in: " + study_output_file )
    print("")    
    print ("query_to_get_info_on_all_studies() ENDED\n")
    

    
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def query_to_get_info_on_one_case():
      
# get ?query={case (case_submitter_id: "{case_submitter_id}") { case_id case_submitter_id project_submitter_id external_case_id tissue_source_site_code days_to_lost_to_followup disease_type index_date lost_to_followup primary_site count demographics{ demographic_id ethnicity gender demographic_submitter_id race cause_of_death days_to_birth days_to_death vital_status year_of_birth year_of_death } samples { sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type tumor_code tumor_code_id tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type } } project {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type }} diagnoses{ diagnosis_id tissue_or_organ_of_origin age_at_diagnosis primary_diagnosis tumor_grade tumor_stage diagnosis_submitter_id classification_of_tumor days_to_last_follow_up days_to_last_known_disease_status days_to_recurrence last_known_disease_status morphology progression_or_recurrence site_of_resection_or_biopsy vital_status days_to_birth days_to_death prior_malignancy ajcc_clinical_m ajcc_clinical_n ajcc_clinical_stage ajcc_clinical_t ajcc_pathologic_m ajcc_pathologic_n ajcc_pathologic_stage ajcc_pathologic_t ann_arbor_b_symptoms ann_arbor_clinical_stage ann_arbor_extranodal_involvement ann_arbor_pathologic_stage best_overall_response burkitt_lymphoma_clinical_variant cause_of_death circumferential_resection_margin colon_polyps_history days_to_best_overall_response days_to_diagnosis days_to_hiv_diagnosis days_to_new_event figo_stage hiv_positive hpv_positive_type hpv_status iss_stage laterality ldh_level_at_diagnosis ldh_normal_range_upper lymph_nodes_positive lymphatic_invasion_present method_of_diagnosis new_event_anatomic_site new_event_type overall_survival perineural_invasion_present prior_treatment progression_free_survival progression_free_survival_event residual_disease vascular_invasion_present year_of_diagnosis } }}


# SAMPLE CASE:
#                "case_id": "ff24e095-63d7-11e8-bcf1-0a2705229b82",
#                "case_submitter_id": "TCGA-61-1910",
#                "disease_type": "Ovarian Serous Cystadenocarcinoma",
#                "primary_site": "Ovary",
#                "project_submitter_id": "CPTAC-TCGA"

  case_submitter_id = "TCGA-61-1910"
  
#  query = '''{case (case_submitter_id: "{case_submitter_id}") { case_id case_submitter_id project_submitter_id external_case_id tissue_source_site_code days_to_lost_to_followup disease_type index_date lost_to_followup primary_site count demographics{ demographic_id ethnicity gender demographic_submitter_id race cause_of_death days_to_birth days_to_death vital_status year_of_birth year_of_death } samples { sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type tumor_code tumor_code_id tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type } } project {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type }} diagnoses{ diagnosis_id tissue_or_organ_of_origin age_at_diagnosis primary_diagnosis tumor_grade tumor_stage diagnosis_submitter_id classification_of_tumor days_to_last_follow_up days_to_last_known_disease_status days_to_recurrence last_known_disease_status morphology progression_or_recurrence site_of_resection_or_biopsy vital_status days_to_birth days_to_death prior_malignancy ajcc_clinical_m ajcc_clinical_n ajcc_clinical_stage ajcc_clinical_t ajcc_pathologic_m ajcc_pathologic_n ajcc_pathologic_stage ajcc_pathologic_t ann_arbor_b_symptoms ann_arbor_clinical_stage ann_arbor_extranodal_involvement ann_arbor_pathologic_stage best_overall_response burkitt_lymphoma_clinical_variant cause_of_death circumferential_resection_margin colon_polyps_history days_to_best_overall_response days_to_diagnosis days_to_hiv_diagnosis days_to_new_event figo_stage hiv_positive hpv_positive_type hpv_status iss_stage laterality ldh_level_at_diagnosis ldh_normal_range_upper lymph_nodes_positive lymphatic_invasion_present method_of_diagnosis new_event_anatomic_site new_event_type overall_survival perineural_invasion_present prior_treatment progression_free_survival progression_free_survival_event residual_disease vascular_invasion_present year_of_diagnosis } }}'''

  query = '{case (case_submitter_id: "' + case_submitter_id + '"' + ''') { case_id case_submitter_id project_submitter_id external_case_id tissue_source_site_code days_to_lost_to_followup disease_type index_date lost_to_followup primary_site count demographics{ demographic_id ethnicity gender demographic_submitter_id race cause_of_death days_to_birth days_to_death vital_status year_of_birth year_of_death } samples { sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type tumor_code tumor_code_id tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type } } project {project_id project_submitter_id name studies {study_id submitter_id_name study_submitter_id analytical_fraction experiment_type acquisition_type }} diagnoses{ diagnosis_id tissue_or_organ_of_origin age_at_diagnosis primary_diagnosis tumor_grade tumor_stage diagnosis_submitter_id classification_of_tumor days_to_last_follow_up days_to_last_known_disease_status days_to_recurrence last_known_disease_status morphology progression_or_recurrence site_of_resection_or_biopsy vital_status days_to_birth days_to_death prior_malignancy ajcc_clinical_m ajcc_clinical_n ajcc_clinical_stage ajcc_clinical_t ajcc_pathologic_m ajcc_pathologic_n ajcc_pathologic_stage ajcc_pathologic_t ann_arbor_b_symptoms ann_arbor_clinical_stage ann_arbor_extranodal_involvement ann_arbor_pathologic_stage best_overall_response burkitt_lymphoma_clinical_variant cause_of_death circumferential_resection_margin colon_polyps_history days_to_best_overall_response days_to_diagnosis days_to_hiv_diagnosis days_to_new_event figo_stage hiv_positive hpv_positive_type hpv_status iss_stage laterality ldh_level_at_diagnosis ldh_normal_range_upper lymph_nodes_positive lymphatic_invasion_present method_of_diagnosis new_event_anatomic_site new_event_type overall_survival perineural_invasion_present prior_treatment progression_free_survival progression_free_survival_event residual_disease vascular_invasion_present year_of_diagnosis } }}'''
  print("")                
  print("%%%%%%%%%%%%%%")
  print("The query built is:")                    
  print(query)
  print("")
  print("%%%%%%%%%%%%%%")
  print("")                    


  response = requests.post(url, json={'query': query})

  if(response.ok):
      #If the response was OK then print the returned JSON
      jData = json.loads(response.content)

      print ("\n")
      print ("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
      print ("Answer returned:\n")                  
      print (json.dumps(jData, indent=4, sort_keys=True))
  else:
      # If response code is not ok (200), print the resulting http error code with description
      print ("Answer returned: ERROR - status shown below.\n")                          
      response.raise_for_status()




        
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    # Swagger PDC API documentation
    # --> https://pdc.esacinc.com/data-dictionary/publicapi-documentation/

# CODE FROM PAUL - which I am trying to break it out into first step

def query_to_find_the_studies_for_a_case():

  
    tcga_case_id = 'TCGA-61-1911'

    # Retrieve submitter_id_names for studies linked to sample (this is not ideal, study_submitter_id would be better)
    studies_with_case = query_pdc(api='uiExperimentFileCount', query_field='case_submitter_id', query=tcga_case_id,
                                  fields=['acquisition_type',
                                          'submitter_id_name',
                                          'experiment_type',
                                          'files_count'])

    print("")                
    print("%%%%%%%%%%%%%%")
    print("studies_with_case data returned is:")                    
    print( str(studies_with_case) )
    print("")
    print("%%%%%%%%%%%%%%")
    print("")                    


    # samples:
    # Case Submitter IDs:   01BR001,  01BR008
    
    # Cases:
    #  C3N-00223


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# CODE FROM PAUL

# How can I see all files that are available for a case?

def query_to_find_all_files_available_for_a_case():
      
    tcga_case_id = 'TCGA-61-1911'

    # Swagger PDC API documentation
    # --> https://pdc.esacinc.com/data-dictionary/publicapi-documentation/

    # Retrieve submitter_id_names for studies linked to sample (this is not ideal, study_submitter_id would be better)
    studies_with_case = query_pdc(api='uiExperimentFileCount', query_field='case_submitter_id', query=tcga_case_id,
                                  fields=['acquisition_type',
                                          'submitter_id_name',
                                          'experiment_type',
                                          'files_count'])

    submitter_id_names = [x['submitter_id_name'] for x in studies_with_case]

    # Now retrieve all studies
    # fields to return from programsProjectsStudies call
    fields = '''program_submitter_id
    projects {
      project_id
      project_submitter_id
      name
      studies {
        study_id
        study_submitter_id
        submitter_id_name
      }
    }'''.split('\n')

    all_programs = query_pdc(api='programsProjectsStudies', fields=fields)

    # Now, map study_submitter_id's
    study_submitter_ids = []
    # Loop all programs, projects and studies for the submitter_id_names that you want
    for program in all_programs:
        for project in program['projects']:
            for study in project['studies']:
                if study['submitter_id_name'] in submitter_id_names:
                    try:
                        study_submitter_ids.append(study['study_submitter_id'])
                    except KeyError:
                        print('missing identifier')

    files = {}
    # If you want to see just the gene-level expression data, you can add data_category: "Protein Assembly"
    for study_submitter_id in study_submitter_ids:
        files[study_submitter_id] = query_pdc(api='filesPerStudy', query_field='study_submitter_id',
                                              query=study_submitter_id,
                                              fields=['file_id', 'file_type', 'file_location', 'md5sum', 'file_size',
                                                      'data_category'])
    for study, f in files.items():
        print('study {} includes {} files'.format(study, len(f)))
    return

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# (base) bash-3.2$ ls -l study*quant*.txt
# -rw-r--r--  1 ronaldtaylor  wheel      2666 Jan 22 13:16 study10_quantDataMatrix_in_JSON_format.txt
# -rw-r--r--  1 ronaldtaylor  wheel      2080 Jan 22 13:27 study11_quantDataMatrix_in_JSON_format.txt

# -rw-r--r--  1 ronaldtaylor  wheel  32299026 Jan 22 12:38 study1_quantDataMatrix_in_JSON_format.txt
# { quantDataMatrix(study_submitter_id: "CPTAC GBM Discovery Study - Proteome" data_type: "log2_ratio" attempt: 0) }

# -rw-r--r--  1 ronaldtaylor  wheel      1831 Jan 22 11:15 study2_quantDataMatrix_in_JSON_format.txt
# -rw-r--r--  1 ronaldtaylor  wheel      3878 Jan 22 11:30 study3_quantDataMatrix_in_JSON_format.txt
# -rw-r--r--  1 ronaldtaylor  wheel      2891 Jan 22 11:40 study4_quantDataMatrix_in_JSON_format.txt
# -rw-r--r--  1 ronaldtaylor  wheel      2666 Jan 22 11:50 study5_quantDataMatrix_in_JSON_format.txt

# -rw-r--r--  1 ronaldtaylor  wheel  76185527 Jan 22 12:15 study6_quantDataMatrix_in_JSON_format.txt
# { quantDataMatrix(study_submitter_id: "S037-2" data_type: "log2_ratio" attempt: 0) }

# -rw-r--r--  1 ronaldtaylor  wheel      2666 Jan 22 12:25 study7_quantDataMatrix_in_JSON_format.txt

# -rw-r--r--  1 ronaldtaylor  wheel  24056486 Jan 22 12:46 study8_quantDataMatrix_in_JSON_format.txt
# { quantDataMatrix(study_submitter_id: "S038-1" data_type: "log2_ratio" attempt: 0) }

# -rw-r--r--  1 ronaldtaylor  wheel  24206640 Jan 22 13:06 study9_quantDataMatrix_in_JSON_format.txt
# { quantDataMatrix(study_submitter_id: "S038-2" data_type: "log2_ratio" attempt: 0) }

# (base) bash-3.2$

# 1/22/20
# manually created
# The key here is the study_submiter_id
#
dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped["CPTAC GBM Discovery Study - Proteome"] = "done"
dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped["S037-2"] = "done"
dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped["S038-1"] = "done"
dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped["S038-2"] = "done"


def query_to_get_quantDataMatrix_on_all_studies(out_directory):

    print ("query_to_get_quantDataMatrix_on_all_studies() STARTED\n")

    study_counter = 1    

    # GLOBAL_STUDY_ID_LIST is read in from a dictionaries file as pdc.GLOBAL_STUDY_ID_LIST
    #
    study_output_record_file = "run_record_of_study_quant_data_matrix_file_download" + ".txt"
    outfile = open(study_output_record_file,'w')    

    for study_id in pdc.GLOBAL_STUDY_ID_LIST:

      study_submitter_name = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id]
      study_submitter_id   = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]
      study_name           = pdc.GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]       
      print("")
      print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
      print(str(study_counter) + ") study_id = " + study_id)
      print("  study_name           = " + study_name)                            
      print("  study_submitter_name = " + study_submitter_name)
      print("  study_submitter_id   = " + study_submitter_id)

      already_exists_boolean = dict_study_which_already_has_quantDataMatrix_filled_in_so_can_be_skipped.get(study_submitter_id, "not_returned_yet")
      if already_exists_boolean == "not_returned_yet":
       outfile.write("\n")
       outfile.write("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
       outfile.write(str(study_counter) + ") study_id = " + study_id + "\n")
       outfile.write("  study_name           = " + study_name + "\n")              
       outfile.write("  study_submitter_name = " + study_submitter_name + "\n")
       outfile.write("  study_submitter_id   = " + study_submitter_id + "\n")


       data_type = "log2_ratio"
       starting_attempt_number = 0
       numOfAliquots = "NOT_USED"
       numOfGenes    = "NOT_USED"
       sleep_duration_in_sec = 300
       #
       study_output_filename = "study" + str(study_counter) + "_quantDataMatrix_in_JSON_format.txt"
       out_file_loc = out_directory + "/" + study_output_filename
       #
       print("")
       print("Calling query_to_get_quantDataMatrix_on_one_study_and_store_in_outfile(study_output_filename, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec, study_counter, study_name) ...")
       print("")             
       query_to_get_quantDataMatrix_on_one_study_and_store_in_outfile(out_file_loc, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec, study_counter, study_name)
       study_counter += 1

      else:
       print("")
       print("This study is skipped because we have already retrieved its quantDataMatrix.")             
       print("")        
       
    outfile.close()       
    print("")  
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("")

    print ("query_to_get_quantDataMatrix_on_all_studies() ENDED\n")    

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Returns quant data matrix
#
# get ?query={ quantDataMatrix(study_submitter_id: "{study_submitter_id}" data_type: "{data_type}" attempt: {attempt} numOfAliquot: {numOfAliquot} numOfGene: {numOfGene}) }

# Returns quant data matrix for a study submitter ID.

# This API is asynchronous and requires multiple API requests to get
# the expected matrix. Please refer to this wiki link for tips on
# using this API. The API takes a long time to execute because of the
# huge volume of data.

#Fields:
#
#     study_submitter_id: Study Submitter ID, example: S046-1
#     date_type: Data type, example: log2_ratio
#     attempt: Number of attempt, example 0,1,2 in order
#     numOfAliquot: Number of aliquots
#     numOfGene: Number of genes

# GLOBAL_STUDY_SUBMITTER_ID_LIST = ['CPTAC GBM Discovery Study - Proteome', 'CPTAC GBM Discovery Study - Phosphoproteome', 'CPTAC GBM Discovery Study - CompRef Proteome', 'CPTAC GBM Discovery Study - CompRef Phosphoproteome', 'S037-1', 'S037-2', 'S037-3', 'S038-1', 'S038-2', 'S038-3', 'S039-1', 'S039-2', 'S015-1', 'S015-2', 'S016-1', 'S020-1', 'S020-2', 'S020-3', 'S020-4', 'ST25730263', 'HBV-Related Hepatocellular Carcinoma - Proteome', 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', 'Pediatric Brain Cancer Pilot Study - Proteome', 'Pediatric Brain Cancer Pilot Study - Phosphoproteome']

# GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['10251935-5540-11e8-b664-00a098d917f8'] = 'CPTAC GBM Discovery Study - Proteome::CPTAC GBM Discovery Study - Phosphoproteome::CPTAC GBM Discovery Study - CompRef Proteome::CPTAC GBM Discovery Study - CompRef Phosphoproteome::Prospective_Colon_VU_Proteome::Prospective_Colon_PNNL_Proteome_Qeplus::Prospective_Colon_PNNL_Phosphoproteome_Lumos::Prospective_Ovarian_JHU_Proteome::Prospective_Ovarian_PNNL_Proteome_Qeplus::Prospective_Ovarian_PNNL_Phosphoproteome_Lumos::Prospective_Breast_BI_Proteome::Prospective_Breast_BI_Phosphoproteome::TCGA_Breast_Cancer_Proteome::TCGA_Breast_Cancer_Phosphoproteome::TCGA_Colon_Cancer_Proteome::TCGA_Ovarian_JHU_Glycoproteome::TCGA_Ovarian_JHU_Proteome::TCGA_Ovarian_PNNL_Proteome::TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'
# GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['1a4a4346-f231-11e8-a44b-0a9c39d33490'] = 'PCT_SWATH_Kidney'
# GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['c3408a52-f1e8-11e9-9a07-0a80fada099c'] = 'HBV-Related Hepatocellular Carcinoma - Proteome::HBV-Related Hepatocellular Carcinoma - Phosphoproteome'
# GLOBAL_PROJECT_ID_TO_STUDY_SUBMITTER_ID_LIST_DICT['c3408b38-f1e8-11e9-9a07-0a80fada099c'] = 'Pediatric Brain Cancer Pilot Study - Proteome::Pediatric Brain Cancer Pilot Study - Phosphoproteome'

# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['267d6671-0e78-11e9-a064-0a9c39d33490'] = 'CPTAC3-Discovery'
# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['48653303-5546-11e8-b664-00a098d917f8'] = 'CPTAC2 Confirmatory'
# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['48af5040-5546-11e8-b664-00a098d917f8'] = 'CPTAC2 Retrospective'
# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['d282b2d7-f238-11e8-a44b-0a9c39d33490'] = 'Quantitative digital maps of tissue biopsies'
# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['095cf1fe-0f93-11ea-9bfa-0a42f3c845fe'] = 'Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma'
# GLOBAL_PROJECT_ID_TO_PROJECT_NAME_DICT['edb4ca56-f1e8-11e9-9a07-0a80fada099c'] = 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study'

# Thur 1/16/20
#
# In the quantDataMatrix() call, Paul told me to simply omit the
# numOfAliquot and numOfGene vars in the call. That will return the
# full matrix, for all gene rows, and all aliquot columns.
#
# The quantDataMatrix() call takes a long time - maybe 5 or 6 min -
# and does many joins. So the idea is that you reset the attempt var -
# increment by one each time, for each repeated call, until you get a
# huge response back (in terms of size), indicating that the call
# finished. You start with "attempt" set to 0. Then you sleep for,
# say, 5 min and then issue the call again with "attempt" set to
# 1. The repeated calls check the server cache to see if the query has
# finished. If so, they return the result. If not, you get a msg
# saying
#
# "Increment attempt and try again!"
#
# So - my Python code must sleep between attempts for some N minutes each time.

# get ?query={ quantDataMatrix(study_submitter_id: "{study_submitter_id}" data_type: "{data_type}" attempt: {attempt} numOfAliquot: {numOfAliquot} numOfGene: {numOfGene}) }


# Currently we do not use the NumOfAliquots and NumOfGenes. Leaving them out simply returns the full data matrix, Paul Rudnick said, which is what we want.  
#  
#  query = '{ quantDataMatrix(study_submitter_id: "' + study_submitter_id + '"' + ' data_type: "' + data_type + '"' + ' attempt: "' + str(num_of_attempt) + '"' + ' numOfAliquot: "' + str(numOfAliquots) + '"' + ' numOfGene: "' + str(numOfGenes) + '"' + ') }'



def query_to_get_quantDataMatrix_on_one_study_and_store_in_outfile(outfile, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec, study_number_in_loop, study_name):
  
  print ("")
  print ("query_to_get_quanDataMatrix_on_one_study_and_store_in_outfile(outfile, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec) STARTED\n")
  
  fileout = open(outfile,'w')

  data_matrix_was_returned = False
  attempt_num = 0 

  # This is an arbitrary lower boundary
  MIN_SIZE_BOUND_OF_A_DATA_MATRIX = 1000
  
  while data_matrix_was_returned == False:
    
     query = '{ quantDataMatrix(study_submitter_id: "' + study_submitter_id + '"' + ' data_type: "' + data_type + '"' + ' attempt: ' + str(attempt_num) + ') }'
    
     print("")                
     print("%%%%%%%%%%%%%%")
     print("attempt number (loop iteration number) = " + str(attempt_num) )
     print("")     
     print("The quantDataMatrix() query built is:")                    
     print(query)
     print("")
     print("%%%%%%%%%%%%%%")
     print("")                    
  
     fileout.write("\n")                
     fileout.write("%%%%%%%%%\n")
     fileout.write("attempt number (loop iteration number) = " + str(attempt_num) + "\n" )
     fileout.write("\n")  
     fileout.write("The quantDataMatrix() query used is:\n")                    
     fileout.write(query)
     fileout.write("\n")
     fileout.write("%%%%%%%%%\n")
     fileout.write("\n")                    
  
     response = requests.post(url, json={'query': query})

     if(response.ok):
        #If the response was OK then print the returned JSON
        jData = json.loads(response.content)

        len_of_jData = len(str(jData))
        print("")     
        print("len_of_jData string returned = " + str(len_of_jData) )                    
        print("")
        fileout.write("\n")      
        fileout.write("len_of_jData string returned = " + str(len_of_jData) + "\n")
        fileout.write("\n")
      
        if len_of_jData < MIN_SIZE_BOUND_OF_A_DATA_MATRIX:
          print("")
          print("%%%%%%%")
          print("STUDY #" + str(study_number_in_loop) +  " study_name = '" + study_name + "'")          
          print("ATTEMPT MSG FOLLOWS for attempt " + str(attempt_num))
          print("")      
          print(json.dumps(jData, indent=4, sort_keys=True))
          print("")
          print("%%%%%%%")
          print("")
          fileout.write("%%%%%%%\n")
          fileout.write("ATTEMPT MSG FOLLOWS:\n")                
          fileout.write("\n")      
          fileout.write(json.dumps(jData, indent=4, sort_keys=True))
          fileout.write("\n")
          fileout.write("\n")      
          fileout.write("%%%%%%%\n")
          fileout.write("\n")
          #
          # If the query has not finished, we get something like this back:
          # ATTEMPT MSG FOLLOWS:
          # {
          #     "data": {
          #         "quantDataMatrix": [
          #            [
          #                "Data Matrix: "
          #            ],
          #            [
          #                "Type: "
          #            ],
          #            [
          #                "log2_ratio"
          #            ],
          #            [
          #                "Study: "
          #            ],
          #            [
          #                "CPTAC GBM Discovery Study - Phosphoproteome"
          #            ],
          #            [
          #                "Status: "
          #            ],
          #            [
          #                "Not ready "
          #            ],
          #            [
          #                "Increment attempt and try again!"
          #            ]
          #        ]
          #    }
          # }

          
          # NOTE: apparently, at least one or more studies can come back, as a finished query, with an empty quantDataMatrix, like this:
          #  ATTEMPT MSG FOLLOWS:
          #  {
          #    "data": {
          #        "quantDataMatrix": [
          #            [
          #                "Gene/Aliquot"
          #            ]
          #        ]
          #     }
          # }
          #
          # So we must also check for something like that as a complete (and small) response, in addition to simply checking
          # on the response string size.
          #
          string_to_check_for = "Gene"
          #
          data_dict = jData["data"]
          quantDataMatrix_dict_list = data_dict["quantDataMatrix"]          

          print("")                           
          print("quantDataMatrix_dict_list= '" + str(quantDataMatrix_dict_list) + "'")
          print("")
          #
          # sample output at this point:
          # quantDataMatrix_dict_list= '[['Gene/Aliquot']]'
          

          if string_to_check_for in str(quantDataMatrix_dict_list):
             # The query has returned a complete response - albeit a small and presumbably empty matrix.
             print("")
             print("STUDY #" + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")                       
             print("The size of returned string is small. However, the returned string contains 'quantDataMatrix',")
             print("which shows that the requested data matrix has been returned, albeit as a small empty matrix.")
             print("We quit the retry loop now, with (fake) success, storing the (empty) matrix in the outfile.")     
             print("")
             fileout.write("\n")
             fileout.write("%%%%%%%\n")
             fileout.write("EMPTY (OR VERY SMALL) DATA MATRIX FOLLOWS:\n")
             fileout.write("\n")                   
             fileout.write("The size of returned string is small. However, the returned string contains 'quantDataMatrix'," + "\n")  
             fileout.write("which shows that the requested data matrix has been returned, albeit as a small (presumably empty) matrix." + "\n")  
             fileout.write("\n")      
             fileout.write(json.dumps(jData, indent=4, sort_keys=True))
             fileout.write("\n")
             fileout.write("\n")      
             fileout.write("%%%%%%%\n")
             fileout.write("\n")
             data_matrix_was_returned = True
          else:
            print("STUDY #" + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")                                   
            print("Size of returned string shows that the requested data matrix has not been returned yet.")
            print("We sleep for " + str(sleep_duration_in_sec) + " sec and try again.")                              
            print("")
            print("Sleeping now ...")
            fileout.write("Size of returned string shows that the requested data matrix has not been returned yet."  + "\n")           
            fileout.write("We sleep for " + str(sleep_duration_in_sec) + " sec and try again."  + "\n")           
            fileout.write(""  + "\n")           
            fileout.write("Sleeping now ..." + "\n")           
          
            time.sleep(sleep_duration_in_sec)
            print("")
            fileout.write("\n")            
            attempt_num += 1
        else:
          print("")
          print("STUDY # " + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")          
          print("The size of returned string shows that the requested data matrix has been returned. Success!")
          print("We quit the retry loop now, with success, storing the PDC matrix in the outfile.")     
          print("")
          fileout.write("\n")
          fileout.write("%%%%%%%\n")
          fileout.write("DATA MATRIX FOLLOWS:\n")
          fileout.write("STUDY #" + str(study_number_in_loop) +  " study_name = '" + study_name + "'")                    
          fileout.write("\n")      
          fileout.write(json.dumps(jData, indent=4, sort_keys=True))
          fileout.write("\n")
          fileout.write("\n")      
          fileout.write("%%%%%%%\n")
          fileout.write("\n")
          data_matrix_was_returned = True
     else:
        # If response code is not ok (200), print the resulting http error code with description
        print ("Answer returned: ERROR - status shown below.\n")
        print ("FAILURE for this study_submitter_id: " + study_submitter_id + "\n")                                  
        response.raise_for_status()
        break
    
  fileout.close()
  print ("")
  print ("query_to_get_quantDataMatrix_on_one_study_and_store_in_outfile(outfile, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec) ENDED\n")
  print ("")  
  

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# STUDY #4, study_name = 'CPTAC CCRCC Discovery Study - Proteome'

# The biospecimenData() query used is:
# { biospecimenPerStudy(study_id: "dbe94609-1fb3-11e9-b7f8-0a80fada099c") { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon} }
#
# response is OK
# len_of_jData string returned = 113885
#
# JSON OUTPUT OF THE QUERY ABOVE:
#
# {
#    "data": {
#        "biospecimenPerStudy": [
#            {
#                "aliquot_id": "bd34fbb3-2053-11e9-b7f8-0a80fada099c",
#                "aliquot_status": "Qualified",
#                 "aliquot_submitter_id": "CPT0026410003",
#                 "case_id": "dae8930e-1fb8-11e9-b7f8-0a80fada099c",
#                "case_status": "Qualified",
#                "case_submitter_id": "C3L-00791",
#                "disease_type": "Clear Cell Renal Cell Carcinoma",
#                "pool": "No",
#                "primary_site": "Kidney",
#                "project_name": "CPTAC3-Discovery",
#                "sample_id": "b72322c6-204c-11e9-b7f8-0a80fada099c",
#                "sample_status": "Qualified",
#                "sample_submitter_id": "C3L-00791-01",
#                "sample_type": "Primary Tumor",
#                "taxon": "Homo sapiens"
#            },
#
#             <CONTINUES ...>
#
#            {
#                "aliquot_id": "fd90e5e8-2053-11e9-b7f8-0a80fada099c",
#                "aliquot_status": "Qualified",
#                "aliquot_submitter_id": "CPT0086820003",
#                "case_id": "14074e8f-1fb9-11e9-b7f8-0a80fada099c",
#                "case_status": "Qualified",
#                "case_submitter_id": "C3L-01313",
#                "disease_type": "Clear Cell Renal Cell Carcinoma",
#                "pool": "No",
#                "primary_site": "Kidney",
#                "project_name": "CPTAC3-Discovery",
#                "sample_id": "fbfa1f47-204c-11e9-b7f8-0a80fada099c",
#                "sample_status": "Qualified",
#                "sample_submitter_id": "C3L-01313-03",
#                "sample_type": "Primary Tumor",
#                "taxon": "Homo sapiens"
#            }
#        ]
#    }
# }

           

               
def query_to_get_biospecimenData_on_all_studies_and_store_in_dicts():

   print ("query_to_get_biospecimenData_on_all_studies_and_store_in_dicts() STARTED\n")

   study_counter = 1    

   # GLOBAL_STUDY_ID_LIST is read in from a dictionaries file as pdc.GLOBAL_STUDY_ID_LIST
   #
   
   for study_id in pdc.GLOBAL_STUDY_ID_LIST:
       study_submitter_name = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id]
       study_submitter_id   = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]
       study_name           = pdc.GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]       
       print("")
       print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
       print(str(study_counter) + ") study_id = " + study_id)
       print("  study_name           = " + study_name)                            
       print("  study_submitter_name = " + study_submitter_name)
       print("  study_submitter_id   = " + study_submitter_id)

#       outfile.write("\n")
#       outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
#       outfile.write("# " + str(study_counter) + ") study_id = " + study_id + "\n")
#       outfile.write("#  study_name           = " + study_name + "\n")              
#       outfile.write("#  study_submitter_name = " + study_submitter_name + "\n")
#       outfile.write("#  study_submitter_id   = " + study_submitter_id + "\n")


       jData_dict = query_to_get_biospecimenData_on_one_study(study_id, study_counter, study_name)
       #
       if jData_dict == "QUERY_FAILURE":
          print("")
          print("  ----------------------")
          print("  QUERY_FAILURE on this study!")
          print("  Hence, no entries are made for this study into the output Python dictionary file.")
          print("  ----------------------")
          print("")
       else:
          # We have one dict entry, with a key of "data", and another dict "biospecimenPerStudy" as its data, at the top level.

          top_level_dict_size = len(jData_dict)
          biospecimenPerStudy_entry_dict = jData_dict["data"]
          list_of_aliquot_dictionaries = biospecimenPerStudy_entry_dict["biospecimenPerStudy"]
          len_of_aliquot_dictionary_list = len(list_of_aliquot_dictionaries)

          print("top_level_dict_size = " + str(top_level_dict_size) )
          print("len_of_aliquot_dictionary_list    = " + str(len_of_aliquot_dictionary_list) )      

# A sample dictionary in the list_of_aliquot_dictionaries:
#           {             
#                "aliquot_id": "fd90e5e8-2053-11e9-b7f8-0a80fada099c",
#                "aliquot_status": "Qualified",
#                "aliquot_submitter_id": "CPT0086820003",
#                "case_id": "14074e8f-1fb9-11e9-b7f8-0a80fada099c",
#                "case_status": "Qualified",
#                "case_submitter_id": "C3L-01313",
#                "disease_type": "Clear Cell Renal Cell Carcinoma",
#                "pool": "No",
#                "primary_site": "Kidney",
#                "project_name": "CPTAC3-Discovery",
#                "sample_id": "fbfa1f47-204c-11e9-b7f8-0a80fada099c",
#                "sample_status": "Qualified",
#                "sample_submitter_id": "C3L-01313-03",
#                "sample_type": "Primary Tumor",
#                "taxon": "Homo sapiens"
#            }

          num = 0
          print("")               
          for aliquot_dict in list_of_aliquot_dictionaries:
            num += 1
            # GLOBAL_PROGRAM_ID_TO_PROGRAM_NAME_DICT[aliquot_id] = taxon
            # print("    List item " + str(num) + ":")
            # print("    " + str(aliquot_dict) )
            #
            # a sample aliquot_dict we have to parse, from the JSON output:
            # 
            #     {'aliquot_id': '104dc216-2139-11ea-aee1-0e1aae319e49', 'sample_id': '104d2a27-2139-11ea-aee1-0e1aae319e49', 'case_id': '104c7b7e-2139-11ea-aee1-0e1aae319e49', 'aliquot_submitter_id': 'CPT0208980003', 'sample_submitter_id': 'C3N-03070-02', 'case_submitter_id': 'C3N-03070', 'aliquot_status': 'Qualified', 'case_status': 'Qualified', 'sample_status': 'Qualified', 'project_name': 'CPTAC3-Discovery', 'sample_type': 'Primary Tumor', 'disease_type': 'Glioblastoma', 'primary_site': 'Brain', 'pool': 'No', 'taxon': 'Homo sapiens'}
            #
            aliquot_id               = aliquot_dict.get("aliquot_id", "MISSING_value")
            sample_id                = aliquot_dict.get("sample_id", "MISSING_value")
            case_id                  = aliquot_dict.get("case_id", "MISSING_value")
            aliquot_submitter_id     = aliquot_dict.get("aliquot_submitter_id", "MISSING_value")
            sample_submitter_id      = aliquot_dict.get("sample_submitter_id", "MISSING_value")
            case_submitter_id        = aliquot_dict.get("case_submitter_id", "MISSING_value")
            aliquot_status           = aliquot_dict.get("aliquot_status", "MISSING_value")
            case_status              = aliquot_dict.get("case_status", "MISSING_value")
            sample_status            = aliquot_dict.get("sample_status", "MISSING_value")
            project_name             = aliquot_dict.get("project_name", "MISSING_value")
            sample_type              = aliquot_dict.get("sample_type", "MISSING_value")
            disease_type             = aliquot_dict.get("disease_type", "MISSING_value")
            primary_site             = aliquot_dict.get("primary_site", "MISSING_value")                                          
            pool                     = aliquot_dict.get("pool", "MISSING_value")
            taxon                    = aliquot_dict.get("taxon", "MISSING_value")                  
            
#            sample_id                = aliquot_dict["sample_id"]
#            case_id                  = aliquot_dict["case_id"]
#            aliquot_submitter_id     = aliquot_dict["aliquot_submitter_id"]
#            sample_submitter_id      = aliquot_dict["sample_submitter_id"]
#            case_submitter_id        = aliquot_dict["case_submitter_id"]
#            aliquot_status           = aliquot_dict["aliquot_status"]
#            case_status              = aliquot_dict["case_status"]
#            sample_status            = aliquot_dict["sample_status"]
#            project_name             = aliquot_dict["project_name"]
#            sample_type              = aliquot_dict["sample_type"]
#            disease_type             = aliquot_dict["disease_type"]
#            primary_site             = aliquot_dict["primary_site"]                                          
#            pool                     = aliquot_dict["pool"]
#            taxon                    = aliquot_dict["taxon"]                  
            
#            print("    aliquot_id              = " + aliquot_id)
#            print("    sample_id               = " + sample_id)
#            print("    case_id                 = " + case_id)
#            print("    aliquot_submitter_id    = " + aliquot_submitter_id)
#            print("    sample_submitter_id     = " + sample_submitter_id)
#            print("    case_submitter_id       = " + case_submitter_id)
#            print("    aliquot_status          = " + aliquot_status)
#            print("    case_status             = " + case_status)
#            print("    sample_status           = " + sample_status)
#            print("    project_name            = " + project_name)
#            print("    sample_type             = " + sample_type)
#            print("    disease_type            = " + disease_type)
#            print("    primary_site            = " + primary_site)
#            print("    pool                    = " + pool)            
#            print("    taxon                   = " + taxon)


            key = study_name + "::" + aliquot_id
   

            old = GLOBAL_DICT_STUDY_NAME_TO_ALIQUOT_ID_LIST.get(study_name, "EMPTY")
            if old == "EMPTY":
              new = aliquot_id
            else:
              new = old + "::" + aliquot_id
            GLOBAL_DICT_STUDY_NAME_TO_ALIQUOT_ID_LIST[study_name] = new
   
            old = GLOBAL_DICT_STUDY_NAME_TO_CASE_ID_LIST.get(study_name, "EMPTY")
            if old == "EMPTY":
              new = case_id
            else:
              new = old + "::" + case_id
            GLOBAL_DICT_STUDY_NAME_TO_CASE_ID_LIST[study_name] = new


            old = GLOBAL_DICT_STUDY_ID_TO_ALIQUOT_ID_LIST.get(study_id, "EMPTY")
            if old == "EMPTY":
              new = aliquot_id
            else:
              new = old + "::" + aliquot_id
            GLOBAL_DICT_STUDY_ID_TO_ALIQUOT_ID_LIST[study_id] = new
   
            old = GLOBAL_DICT_STUDY_ID_TO_CASE_ID_LIST.get(study_id, "EMPTY")
            if old == "EMPTY":
              new = case_id
            else:
              new = old + "::" + case_id
            GLOBAL_DICT_STUDY_ID_TO_CASE_ID_LIST[study_id] = new

   
   
            old = GLOBAL_DICT_CASE_ID_TO_STUDY_ID_LIST.get(case_id, "EMPTY")
            if old == "EMPTY":
              new = study_id
            else:
              new = old + "::" + study_id
            GLOBAL_DICT_CASE_ID_TO_STUDY_ID_LIST[case_id] = new

            old = GLOBAL_DICT_CASE_ID_TO_STUDY_NAME_LIST.get(case_id, "EMPTY")
            if old == "EMPTY":
              new = study_name
            else:
              new = old + "::" + study_name
            GLOBAL_DICT_CASE_ID_TO_STUDY_NAME_LIST[case_id] = new

   
            old = GLOBAL_DICT_SAMPLE_ID_TO_STUDY_ID_LIST.get(sample_id, "EMPTY")
            if old == "EMPTY":
              new = study_id
            else:
              new = old + "::" + study_id
            GLOBAL_DICT_SAMPLE_ID_TO_STUDY_ID_LIST[sample_id] = new

            old = GLOBAL_DICT_SAMPLE_ID_TO_STUDY_NAME_LIST.get(sample_id, "EMPTY")
            if old == "EMPTY":
              new = study_name
            else:
              new = old + "::" + study_name
            GLOBAL_DICT_SAMPLE_ID_TO_STUDY_NAME_LIST[sample_id] = new

   
            old = GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_ID_LIST.get(aliquot_id, "EMPTY")
            if old == "EMPTY":
              new = study_id
            else:
              new = old + "::" + study_id
            GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_ID_LIST[aliquot_id] = new

            old = GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_NAME_LIST.get(aliquot_id, "EMPTY")
            if old == "EMPTY":
              new = study_name
            else:
              new = old + "::" + study_name
            GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_NAME_LIST[aliquot_id] = new

            old = GLOBAL_DICT_CASE_ID_TO_ALIQUOT_ID_LIST.get(case_id, "EMPTY")
            if old == "EMPTY":
              new = aliquot_id
            else:
              new = old + "::" + aliquot_id
            GLOBAL_DICT_CASE_ID_TO_ALIQUOT_ID_LIST[case_id] = new

            old = GLOBAL_DICT_CASE_ID_TO_SAMPLE_ID_LIST.get(case_id, "EMPTY")
            if old == "EMPTY":
              new = sample_id
            else:
             new = old + "::" + sample_id
            GLOBAL_DICT_CASE_ID_TO_SAMPLE_ID_LIST[case_id] = new


            GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID[aliquot_id]   = case_id
            GLOBAL_DICT_ALIQUOT_ID_TO_SAMPLE_ID[aliquot_id] = sample_id

            GLOBAL_DICT_SAMPLE_ID_TO_CASE_ID[sample_id] = case_id

            # added Sunday 2/16/20
            old = GLOBAL_DICT_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST.get(aliquot_submitter_id, "EMPTY")
            if old == "EMPTY":
              new = aliquot_id
            else:
             new = old + "::" + aliquot_id
            GLOBAL_DICT_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST[aliquot_submitter_id] = new

            # added Sunday 2/16/20
            study_aliquot_submitter_key = study_name + "::" + aliquot_submitter_id
            old = GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST.get(study_aliquot_submitter_key, "EMPTY")
            if old == "EMPTY":
             new = aliquot_id
            else:
             new = old + "::" + aliquot_id
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST[study_aliquot_submitter_key] = new

            
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_TYPE[key] = sample_type
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PRIMARY_SITE[key] = primary_site
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_STATUS[key] = aliquot_status
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_SUBMITTER_ID[key] = aliquot_submitter_id
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_CASE_STATUS[key] = case_status
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_DISEASE_TYPE[key] = disease_type
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_POOL[key] = pool
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PROJECT_NAME[key] = project_name
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_STATUS[key] = sample_status
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID[key] = sample_submitter_id
            GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_TAXON[key] = taxon

       study_counter += 1

   print("")  
   print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
   print("")
   print("DONE!")    
   print("")
   print ("query_to_get_biospecimenData_on_all_studies_and_store_in_dicts() ENDED\n")    
           

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


def store_biospecimen_data_into_python_dict_file(output_dict_file):
   print("")    
   print ("store_biospecimen_data_into_python_dict_file(outfile) STARTED\n")    

   outfile = open(output_dict_file,'w')    

   outfile.write("\n" )   
   outfile.write("dict_study_name_to_aliquot_id_list= {}" + "\n" )
   outfile.write("dict_study_name_to_case_id_list= {}" + "\n" )

   outfile.write("dict_study_id_to_aliquot_id_list= {}" + "\n" )
   outfile.write("dict_study_id_to_case_id_list= {}" + "\n" )

   
   outfile.write("dict_case_id_to_study_id_list= {}" + "\n" )
   outfile.write("dict_case_id_to_study_name_list= {}" + "\n" )   

   outfile.write("dict_aliquot_id_to_study_id_list= {}" + "\n" )        
   outfile.write("dict_aliquot_id_to_study_name_list= {}" + "\n" )        

   outfile.write("dict_sample_id_to_study_id_list = {}" + "\n" )
   outfile.write("dict_sample_id_to_study_name_list = {}" + "\n" )

   outfile.write("dict_case_id_to_aliquot_id_list= {}" + "\n" )
   outfile.write("dict_case_id_to_sample_id_list= {}" + "\n" )   
   
   outfile.write("dict_aliquot_id_to_case_id = {}" + "\n" )
   outfile.write("dict_aliquot_id_to_sample_id = {}" + "\n" )

   outfile.write("dict_sample_id_to_case_id = {}" + "\n" )         

   outfile.write("dict_study_name_and_aliquot_id_to_sample_type= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_primary_site= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_aliquot_status= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_aliquot_submitter_id= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_case_status= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_disease_type= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_pool= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_project_name= {}" + "\n" )          
   outfile.write("dict_study_name_and_aliquot_id_to_sample_status= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_sample_submitter_id= {}" + "\n" )
   outfile.write("dict_study_name_and_aliquot_id_to_taxon = {}" + "\n" )

   # Sunday 2/16/20
   outfile.write("dict_aliquot_submitter_id_to_aliquot_id_list = {}" + "\n" )
   # Sunday 2/16/20   
   outfile.write("dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list = {}" + "\n" )   

   outfile.write("\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_STUDY_NAME_TO_ALIQUOT_ID_LIST)
   outfile.write("# number of entries in dict_study_name_to_aliquot_id_list = " + str(num) + "\n")
   outfile.write("\n")      
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_TO_ALIQUOT_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")      
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")      
      outfile.write("dict_study_name_to_aliquot_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_STUDY_ID_TO_ALIQUOT_ID_LIST)
   outfile.write("# number of entries in dict_study_id_to_aliquot_id_list = " + str(num) + "\n")
   outfile.write("\n")      
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_ID_TO_ALIQUOT_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_id_to_aliquot_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   num = len(GLOBAL_DICT_STUDY_NAME_TO_CASE_ID_LIST)
   outfile.write("# number of entries in dict_study_name_to_case_id_list = " + str(num) + "\n")
   outfile.write("\n")      
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_TO_CASE_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")      
      outfile.write("dict_study_name_to_case_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   num = len(GLOBAL_DICT_STUDY_ID_TO_CASE_ID_LIST)
   outfile.write("# number of entries in dict_study_id_to_case_id_list = " + str(num) + "\n")
   outfile.write("\n")      
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_ID_TO_CASE_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_id_to_case_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_CASE_ID_TO_STUDY_ID_LIST)
   outfile.write("# number of entries in dict_case_id_to_study_id_list = " + str(num) + "\n")
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_CASE_ID_TO_STUDY_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")      
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_case_id_to_study_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_CASE_ID_TO_STUDY_NAME_LIST)
   outfile.write("# number of entries in dict_case_id_to_study_name_list = " + str(num) + "\n")
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_CASE_ID_TO_STUDY_NAME_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_case_id_to_study_name_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_ID_LIST)
   outfile.write("# number of entries in dict_aliquot_id_to_study_id_list = " + str(num) + "\n")
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_aliquot_id_to_study_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_NAME_LIST)
   outfile.write("# number of entries in dict_aliquot_id_to_study_name_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_ALIQUOT_ID_TO_STUDY_NAME_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")      
      outfile.write("dict_aliquot_id_to_study_name_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_SAMPLE_ID_TO_STUDY_ID_LIST)
   outfile.write("# number of entries in dict_sample_id_to_study_id_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_SAMPLE_ID_TO_STUDY_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")      
      outfile.write("dict_sample_id_to_study_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_SAMPLE_ID_TO_STUDY_NAME_LIST)
   outfile.write("# number of entries in dict_sample_id_to_study_name_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_SAMPLE_ID_TO_STUDY_NAME_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_sample_id_to_study_name_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_CASE_ID_TO_ALIQUOT_ID_LIST)
   outfile.write("# number of entries in dict_case_id_to_aliqout_id_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_CASE_ID_TO_ALIQUOT_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_case_id_to_aliquot_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_CASE_ID_TO_SAMPLE_ID_LIST)
   outfile.write("# number of entries in dict_case_id_to_sample_id_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_CASE_ID_TO_SAMPLE_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_case_id_to_sample_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID)
   outfile.write("# number of entries in dict_aliquot_id_to_case_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_ALIQUOT_ID_TO_CASE_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_aliquot_id_to_case_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_ALIQUOT_ID_TO_SAMPLE_ID)
   outfile.write("# number of entries in dict_aliquot_id_to_sample_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_ALIQUOT_ID_TO_SAMPLE_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_aliquot_id_to_sample_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_SAMPLE_ID_TO_CASE_ID)
   outfile.write("# number of entries in dict_sample_id_to_case_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_SAMPLE_ID_TO_CASE_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_sample_id_to_case_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_TYPE)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_sample_type = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_TYPE.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_sample_type['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PRIMARY_SITE)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_primary_site = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PRIMARY_SITE.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      dlist = v.split("::")
      list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_primary_site['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_STATUS)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_aliquot_status = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_STATUS.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_aliquot_status['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_SUBMITTER_ID)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_aliquot_submitter_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_ALIQUOT_SUBMITTER_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_aliquot_submitter_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_CASE_STATUS)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_case_status = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_CASE_STATUS.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_case_status['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_DISEASE_TYPE)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_disease_type = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_DISEASE_TYPE.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_disease_type['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_POOL)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_pool = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_POOL.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_pool['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PROJECT_NAME)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_project_name = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_PROJECT_NAME.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_project_name['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")
   

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_STATUS)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_sample_status = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_STATUS.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_sample_status['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")

   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_sample_submitter_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_sample_submitter_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")
   

   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID)
   outfile.write("# number of entries in dict_study_name_and_aliquot_id_to_sample_submitter_id = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_ID_TO_SAMPLE_SUBMITTER_ID.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_id_to_sample_submitter_id['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")


   # added Sunday 2/16/20   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")
   num = len(GLOBAL_DICT_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST)
   outfile.write("# number of entries in dict_aliquot_submitter_id_to_aliquot_id_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_aliquot_submitter_id_to_aliquot_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")



   # added Sunday 2/16/20   
   outfile.write("# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   outfile.write("# START OF NEW DICTIONARY SECTION\n")   
   outfile.write("\n")

   # study_aliquot_submitter_key = study_name + "::" + aliquot_submitter_id
   num = len(GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST)
   outfile.write("# number of entries in dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list = " + str(num) + "\n")   
   outfile.write("\n")
   counter = 0 
   for k, v in GLOBAL_DICT_STUDY_NAME_AND_ALIQUOT_SUBMITTER_ID_TO_ALIQUOT_ID_LIST.items():
      # fixed_value = fix_possible_string_problems(v)
      counter += 1
      outfile.write("\n")            
      outfile.write("# entry " + str(counter) + "\n")
      if v is None:
         list_count = 0
         v = "None"
      else:
        dlist = v.split("::")
        list_count = len(dlist)
      outfile.write("# number of values = " + str(list_count) + "\n")            
      outfile.write("dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list['" + k + "'] = '" + v + "'" + "\n")
   outfile.write("\n")
   


#   outfile.write("dict_study_name_and_aliquot_id_to_taxon= {}" + "\n" )      
    
   outfile.close()
   print("")
   print("DONE!")    
   print("")
   print("Output was stored into:\n    " + output_dict_file)
   print("")    
   print ("store_biospecimen_data_into_python_dict_file(outfile) ENDED\n")    
   print("")    
   
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
               
               
def query_to_get_biospecimenData_on_all_studies():

    print ("query_to_get_biospecimenData_on_all_studies() STARTED\n")

    study_counter = 1    

    # GLOBAL_STUDY_ID_LIST is read in from a dictionaries file as pdc.GLOBAL_STUDY_ID_LIST
    #
    study_output_record_file = "run_record_of_study_biospecimen_file_download" + ".txt"
    outfile = open(study_output_record_file,'w')    

    for study_id in pdc.GLOBAL_STUDY_ID_LIST:
       study_submitter_name = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_NAME_DICT[study_id]
       study_submitter_id   = pdc.GLOBAL_STUDY_ID_TO_STUDY_SUBMITTER_ID_DICT[study_id]
       study_name           = pdc.GLOBAL_STUDY_ID_TO_STUDY_NAME_DICT[study_id]       
       print("")
       print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
       print(str(study_counter) + ") study_id = " + study_id)
       print("  study_name           = " + study_name)                            
       print("  study_submitter_name = " + study_submitter_name)
       print("  study_submitter_id   = " + study_submitter_id)

       outfile.write("\n")
       outfile.write("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
       outfile.write(str(study_counter) + ") study_id = " + study_id + "\n")
       outfile.write("  study_name           = " + study_name + "\n")              
       outfile.write("  study_submitter_name = " + study_submitter_name + "\n")
       outfile.write("  study_submitter_id   = " + study_submitter_id + "\n")


       study_output_filename = "study" + str(study_counter) + "_biospecimenData_in_JSON_format.txt"
       #
       query_to_get_biospecimenData_on_one_study_and_store_in_outfile(study_output_filename, study_id, study_counter, study_name)
       study_counter += 1
       
    outfile.close()       
    print("")  
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("")

    print ("query_to_get_biospecimenData_on_all_studies() ENDED\n")    

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# get ?query={ biospecimenPerStudy (study_id: "{study_id}"){ aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon} }


def query_to_get_biospecimenData_on_one_study_and_store_in_outfile(outfile, study_id, study_number_in_loop, study_name):
  
  print ("")
  print ("query_to_biospecimenData_on_one_study_and_store_in_outfile() STARTED\n")
  
  fileout = open(outfile,'w')
    
  query = '{ biospecimenPerStudy(study_id: "' + study_id + '"' + ') { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon} }'

  print("%%%%%%%%%%%%%%")  
  print("")
  print("STUDY #" + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")                                             
  print("The biospecimenData() query built is:")                    
  print(query)
  print("")
  
  fileout.write("\n")                
  fileout.write("%%%%%%%%%\n")
  fileout.write("\n")
  fileout.write("STUDY #" + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")
  fileout.write("\n")
  fileout.write("\n")  
  fileout.write("The biospecimenData() query used is:\n")                    
  fileout.write(query)
  fileout.write("\n")
  
  response = requests.post(url, json={'query': query})

  if(response.ok):
        #If the response was OK then print the returned JSON
        jData = json.loads(response.content)

        len_of_jData = len(str(jData))
        print("")
        print("response is OK")        
        print("len_of_jData string returned = " + str(len_of_jData) )                    
        print("")
        fileout.write("\n")
        fileout.write("response is OK\n")              
        fileout.write("len_of_jData string returned = " + str(len_of_jData) + "\n")
        fileout.write("\n")
      
        # print(json.dumps(jData, indent=4, sort_keys=True))
        print("")

        fileout.write("\n")      
        fileout.write(json.dumps(jData, indent=4, sort_keys=True))
        fileout.write("\n")
        fileout.write("\n")      

  else:
        # If response code is not ok (200), print the resulting http error code with description
        print ("Answer returned: ERROR - status shown below.\n")
        print ("FAILURE for this study_id: " + study_id + "\n")                                  
        response.raise_for_status()

  print("%%%%%%%%%%%%%%")
  print("")                    
  fileout.write("%%%%%%%%%\n")
  fileout.write("\n")                    
        
    
  fileout.close()
  print ("")
  print ("query_to_biospecimenData_on_one_study_and_store_in_outfile() ENDED\n")  
  print ("")  
  
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def query_to_get_biospecimenData_on_one_study(study_id, study_number_in_loop, study_name):
  
  print ("")
  print ("query_to_biospecimenData_on_one_study() STARTED\n")
  
  query = '{ biospecimenPerStudy(study_id: "' + study_id + '"' + ') { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon} }'

  print("%%%%%%%%%%%%%%")  
  print("")
  print("STUDY #" + str(study_number_in_loop) +  ", study_name = '" + study_name + "'")                                             
  print("The biospecimenData() query built is:")                    
  print(query)
  print("")
  
  response = requests.post(url, json={'query': query})

  returned_JSON_structure = "EMPTY"
  
  if(response.ok):
        #If the response was OK then print the returned JSON
        jData = json.loads(response.content)

        len_of_jData = len(str(jData))
        print("")
        print("response is OK")        
        print("len_of_jData string returned = " + str(len_of_jData) )                    
        print("")
        returned_JSON_structure = jData
  else:
        # If response code is not ok (200), print the resulting http error code with description
        print ("Answer returned: ERROR - status shown below.\n")
        print ("FAILURE for this study_id: " + study_id + "\n")                                  
        response.raise_for_status()

        returned_JSON_structure = "QUERY_FAILURE"        

  print("%%%%%%%%%%%%%%")
  print ("")
  print ("query_to_biospecimenData_on_one_study() ENDED\n")
  return returned_JSON_structure
  

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# CODE FROM PAUL
def query_pdc(**kwargs):
    url = 'https://pdc-dev.esacinc.com/graphql'

    # Send the POST graphql query
    print('Sending query.')
    if 'query' in kwargs:
        q = '{' + kwargs['api'] + '(' + kwargs['query_field'] + ':"' + kwargs['query'] + '") {' +\
            '\n'.join(kwargs['fields']) + '} }'
    else:
        q = '{' + kwargs['api'] + '{' + '\n\t'.join(kwargs['fields']) + '} }'

    print("")                
    print("%%%%%%%%%%%%%%")
    print("The query built in query_pdc() is:")                    
    print(q)
    print("")
    print("%%%%%%%%%%%%%%")
    print("")                    

#    pdc_response = requests.post(url, json={'query': q})
#    # Set up a data structure for the query result
#    decoded = dict()
#    # Check the results
#    if pdc_response.ok:
#        # Decode the response
#        decoded = pdc_response.json()
#    else:
#        # Response not OK, see error
#        pdc_response.raise_for_status()
#    return decoded['data'][kwargs['api']]

# %%%%%%%%%%%%%%%%%%%%%%%%%%

def main():

#    file_out = "Ver2_record_of_all_cases_in_the_PDC.txt"
#    query_to_get_all_cases(file_out)


# %%%%%%%%%%%
   
    if import_pdc_dictionaries == "NO":
    
      query_to_get_programs()

      # I am NOT currently using:
      # query_to_fill_in_global_lists_and_dictionaries_using_allPrograms()

      # Note:
      #     query_to_fill_in_global_lists_and_dictionaries_using_programsProjectsStudies()
      # returns basically the same info into my dictionaries that I build here as does
      #     query_to_fill_in_global_lists_and_dictionaries()
      #
      # Either call could be used.

      query_to_fill_in_global_lists_and_dictionaries_using_programsProjectsStudies()  

      query_to_fill_in_study_name_disease_type_primary_site_cases_count_aliquots_count_in_global_lists_and_dictionaries_using_study()

      # Note: the output file below outputs dicts built in both function calls above.
      
      # dict_outfile_name = "pdc_LATEST_dictionaries_for_pdc_proteomics_data.py"            
      # dict_outfile_name = "pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
      # 

      # dict_outfile_name = "pdc_AA_3_9_20_LATEST_dictionaries_for_pdc_proteomics_data.py"

      dict_outfile_name = "pdc_AA_3_17_20_LATEST_dictionaries_for_pdc_proteomics_data.py"

      store_global_dicts_and_lists_to_python_file(dict_outfile_name)
      
# %%%%%%%%%%%

    if import_pdc_biospecimen_aliquot_dictionaries == "NO":    
      query_to_get_biospecimenData_on_all_studies_and_store_in_dicts()
      #
      output_dict_file = "pdc_biospecimen_data_dicts" + ".py"
      store_biospecimen_data_into_python_dict_file(output_dict_file)

# %%%%%%%%%%%       
    
    # query_to_get_info_on_all_studies()
    
    # print_global_dicts_and_lists()
        
    # query_to_get_info_on_one_case()
    # query_to_find_all_files_available_for_a_case()
    # query_to_find_the_studies_for_a_case()
    # query_to_get_file_metadata()

    # study_submitter_name = 'TCGA_Breast_Cancer_Proteome'
    # study_submitter_id   = 'S015-1'
    # study_id             = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'
    # query_to_get_info_on_one_study(study_id)

             
    # query_to_get_summary_PDC_stats_across_all_programs()

    # getting one quantDataMatrix, as a test case
    # 1) study_id = cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe
    #  study_name           = CPTAC GBM Discovery Study - Proteome
    #  study_submitter_name = CPTAC GBM Discovery Study - Proteome
    #  study_submitter_id   = CPTAC GBM Discovery Study - Proteome
  
    outfile = "study1_quantDataMatrix_in_JSON_format.txt"
    study_submitter_id = "CPTAC GBM Discovery Study - Proteome"
    data_type = "log2_ratio"
    numOfAliquots = "NOT_USED"
    numOfGenes = "NOT_USED"
    sleep_duration_in_sec = 180
    study_number_in_loop = 1    
    study_name = "CPTAC GBM Discovery Study - Proteome"
    # query_to_get_quantDataMatrix_on_one_study_and_store_in_outfile(outfile, study_submitter_id, data_type, numOfAliquots, numOfGenes, sleep_duration_in_sec, study_number_in_loop, study_name)

    out_directory = "/GDITwork/PDC_queries/March_17_2020_Download_via_API_PDC_quanDataMatrix_GCT_files"
    query_to_get_quantDataMatrix_on_all_studies(out_directory)

    # query_to_get_biospecimenData_on_all_studies()

    # %%%%%%%%%%%%%%%%%%%%%%%%%%

if __name__ == "__main__":
    main()

    
