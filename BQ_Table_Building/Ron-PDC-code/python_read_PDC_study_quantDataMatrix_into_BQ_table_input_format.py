
# invocation:
#  python python_read_PDC_study_quantDataMatrix_into_BQ_table_input_format.py


# The input file contains a spreadsheet quantDataMatrix data for one Proteomics Data Commons (PDC) study.

# (base) bash-3.2$ pwd
# /GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files

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


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%


# Note: the quantDataMatrix files that are named starting with "S0*"
# have errors in many lines. That is, from presumed DOS/Windows entry
# of the lines, there is an embedded carriage return that shows up as
# "^M". These CRs are not recognized as line delimiters on my Mac (and
# on Linux machines, I believe). So the control-M must be converted to
# a true line delimiter before the quant data file is used as input into this program.
#
# I can fix this by changing the carriage returns in emacs.
#
# See the instructions at
#   https://its.ucsc.edu/unix-timeshare/tutorials/clean-ctrl-m.html
#
# and see
#   http://ergoemacs.org/emacs/emacs_line_ending_char.html
#
# Description
# How to remove CTRL-M characters from a file in UNIX.
# 
# You may need to do this when you import a text file from MS-DOS (or
# MS-Windows), and forget to transfer it in ASCII or text mode. Here are several ways to do it; pick the one you are most comfortable with.
#
#    The easiest way is probably to use the stream editor sed to remove the ^M characters. Type this command: % sed -e "s/^M//" filename > newfilename
#    To enter ^M, type CTRL-V, then CTRL-M. That is, hold down the CTRL key then press V and M in succession.

#    You can also do it inside Emacs. To do so, follow these steps:
#        Go to the beginning of the document
#        Type: M-x (esc-x) replace-string RET C-q C-m RET <enter string to replace the control-M> RET
#        where "RET" means <press the return key> and C-q and C-m mean <hold the CTRL key and press the m (or q) key>.

# Note: the change adds a pair of double quotes automatically around the replacement text. I then did another emacs replace-string, replacing
#  "controlM" with controlM


# %%%%%%%%%%%%%%%%%%%

# info on a proteome input file:
#
# (base) bash-3.2$ head "CPTAC GBM Discovery Study - Proteome-log2_ratio.gct"
# #1.3
# 10977	110	3	6
# id	authority	ncbi_gene_id	gene_id	b6358f6b-2154-11ea-8856-0aee84e6d0d3	b6359054-2154-11ea-8856-0aee84e6d0d3	<CONTINUED>
# aliquot_submitter_id	-666	-666	-666	CPT0206330003	CPT0186100003	<CONTINUED>
# aliquot_alias	-666	-666	-666	CPT0206330003	CPT0186100003	<CONTINUED>
# morphology	-666	-666	-666	9440/3	9440/3	<CONTINUED>
# primary_diagnosis	-666	-666	-666	Glioblastoma	Glioblastoma	<CONTINUED>
# tumor_grade	-666	-666	-666	Not Reported	Not Reported	<CONTINUED>
# tumor_stage	-666	-666	-666	Not Reported	Not Reported	<CONTINUED>
# A1BG	HGNC:5	1	f6ba4bc5-b814-11e8-907f-0a2705229b82	-0.3108	0.2480	<CONTINUED>
# ...
# ABCB9	HGNC:50	23457	61346134-3233-6231-2d32-3163352d3131	0.3511	0.0554	NaN	0.2465	-0.1716	NaN	NaN	1.3047	-0.3437	0.0056	1.0128	NaN	0.4671	NaN	-0.6115	0.5033	0.3108	0.3679	0.3503	-0.1054	0.3632	-0.4589	-0.3250	-0.3136	-0.3957	-0.2652	0.9394	0.0162	0.1102	-0.7752	-0.5820	0.2283	-0.2682	NaN	-0.6905	-0.4579	0.7410	0.5331	-0.3658	-0.0564	-0.3417	0.2770	-1.0922	-0.1750	NaN	-0.3193	-0.3038	NaN	NaN	NaN	0.4674	-0.0330	-0.3233	NaN	0.6245	0.2974	-0.0944	NaN	0.8465	-0.1502	0.2584	-0.4101	-0.0170	-0.0004	0.1166	-0.1840	0.5461	0.0273	1.0624	NaN	-0.1591	-0.1368	0.0581	-0.9081	0.3052	-0.9113	1.2411	-0.4304	NaN	-0.6142	1.6071	-0.1275	-0.6223	NaN	0.6518	-0.8167	-0.4490	-0.2551	0.1987	0.9904	-0.1726	1.3734	-0.3743	0.3380	0.8644	0.2382	NaN	0.1090	0.0816	0.0108	-0.2784	NaN	0.0041	-0.8720	NaN	-0.5397	-0.0039	NaN	-0.1680	NaN
# ABCC1	HGNC:51	4363	f6be1512-b814-11e8-907f-0a2705229b82	-0.2742	0.1602	0.1277	-0.0373	0.2490	-0.6385	-0.1790	-0.3690	-0.6399	-0.0462	-0.2104	0.1648	0.5400	-0.1983	-0.4791	-0.3109	-0.2372	0.0145	-0.3211	0.5043	0.3965	-0.7547	1.2606	-0.1885	0.0455	0.1072	-0.1301	-0.4418	-0.1257	-0.0382	0.4543	0.2230	-0.5865	0.0010	0.5119	0.2951	0.1561	-0.4707	0.0101	0.2701	-0.2214	-0.3174	-0.2136	-0.4650	-0.1821	-0.5725	-0.2343	-0.8237	-0.3120	-0.0754	-0.2332	-0.3847	0.1369	-0.4592	1.3026	-0.4188	-0.4308	-0.0350	0.3326	0.0334	-0.2999	0.0096	0.0980	-0.1452	0.0172	0.1893	0.3533	1.2461	-0.2956	0.6860	0.6115	-0.5928	0.1305	1.0776	-0.7764	-0.1483	-0.3130	0.8426	0.2490	0.4506	-0.3031	0.4547	-0.4177	0.3468	-0.1421	-0.5143	0.2338	0.1930	-0.2030	0.4448	0.0886	-0.1056	0.0305	0.0942	-0.2712	-0.1650	0.2674	0.4652	0.1172	-0.1653	0.4774	0.3560	0.5416	-0.2223	-0.2362	-0.5927	-0.3730	1.3294	0.3703	0.5736
#
# (base) bash-3.2$ tail -2 "CPTAC GBM Discovery Study - Proteome-log2_ratio.gct"
#
# ZZEF1	HGNC:29027	23140	fb691289-b814-11e8-907f-0a2705229b82	0.1574	0.0721	-0.1324	-0.1089	0.1772	0.0161	0.0531	0.2982	0.3122	0.2907	0.4103	0.2867	-0.0870	0.3277	-0.3601	-0.0082	0.2242	0.1324	0.2716	0.0417	0.0963	0.2220	0.2238	0.2486	0.4424	0.0894	0.1604	-0.1150	0.1276	0.1754	-0.0661	-0.0970	-0.2163	-0.0596	-0.0753	0.1531	0.1919	0.0258	0.2202	0.2097	0.0614	-0.1261	-0.0489	0.0470	-0.0069	-0.0869	0.1496	0.1787	0.1511	0.0597	-0.0516	0.1192	0.1849	-0.1241	0.0294	-0.5837	0.1783	0.1140	0.3145	0.1303	0.0230	0.2417	0.0793	0.0389	0.0546	-0.0329	0.3197	-0.1030	0.2459	-0.3311	0.4283	0.4615	0.1970	0.4346	-0.0850	0.2957	0.3615	0.0863	0.1158	0.2255	0.5011	-0.1080	-0.4529	0.2618	0.3250	-0.1155	0.3091	0.0598	-0.0454	0.1128	-0.1678	0.2723	-0.0325	-0.0147	0.4480	0.2210	0.0770	0.2323	-0.0095	0.2669	0.2570	-0.3392	0.1137	0.0507	-0.0480	0.0820	0.0140	0.1074	0.2789	0.0213
# ZZZ3	HGNC:24523	26009	fb6913d3-b814-11e8-907f-0a2705229b82	NaN	0.3949	0.4366	0.2184	NaN	0.1696	0.2216	-0.1087	0.4239	0.2769	NaN	-0.1966	0.1390	-0.0538	0.5814	0.2422	-0.4665	NaN	0.4841	-0.5591	0.0296	-0.1707	0.0894	-0.0642	-0.2639	0.3810	-0.4159	0.0323	-0.3977	0.1914	0.1272	0.6415	0.1551	-0.2976	0.3873	0.5172	-0.0139	0.2477	0.0651	0.7934	0.2613	-0.1364	0.1199	0.2703	0.2713	0.0531	-0.0329	0.2945	-0.0110	0.0824	0.4850	0.1148	0.3391	0.0955	-0.5708	0.3110	0.2060	-0.1585	-0.0779	NaN	0.1949	NaN	0.4773	0.0087	-0.7543	0.4510	0.0425	0.4357	-0.1921	0.0216	-0.2409	-0.3627	-0.1400	NaN	0.2733	NaN	-0.2860	0.1439	0.0473	0.1344	-0.8849	-0.4985	0.3900	0.1319	-0.6692	-0.0086	0.0270	0.2234	-0.5211	-0.4017	0.4016	0.1597	-0.0475	-0.0573	0.0427	NaN	-0.1704	0.2650	0.1701	0.3248	-0.8480	0.1255	-0.4938	NaN	0.1725	0.5333	0.7510	0.1954	0.4318	0.0031
# (base) bash-3.2$ 


# info on a phosphoproteome input file:
#
# (base) bash-3.2$ head "CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct"
#1.3
# 41580	110	1	6
# id	peptide	54d81293-2167-11ea-8856-0aee84e6d0d3	54d81380-2167-11ea-8856-0aee84e6d0d3	<CONTINUED>
# aliquot_submitter_id	-666	CPT0206330003	CPT0186100003	<CONTINUED>
# aliquot_alias	-666	CPT0206330003	CPT0186100003	<CONTINUED>
# morphology	-666	9440/3	9440/3	<CONTINUED>
# primary_diagnosis	-666	<CONTINUED>
# tumor_grade	-666	Not Reported	Not Reported	<CONTINUED>
# tumor_stage	-666	Not Reported	Not Reported	<CONTINUED>
# NP_000012.1:s324t327	YNAESTEREsQDtVAENDDGGFSEEWEAQR	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.6066	NaN	NaN	NaN	NaN	NaN	NaN	1.7149	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.5069	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.1194	0.1610	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.2193	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.2768	-0.0778	NaN	NaN	1.5466	NaN	NaN	NaN	NaN	2.1754	NaN	2.5376	NaN	1.8529	NaN	NaN	1.6938	0.1540	NaN	NaN	NaN	NaN	1.8243	NaN	NaN	0.2476	NaN	NaN	1.1146	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.5140	-0.4985	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
#
# 
# (base) bash-3.2$ tail "CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct"
#
# XP_024303909.1:s707	sFDEQNVPK	-0.4395	-0.1238	NaN	-0.5602	0.4329	NaN	NaN	NaN	0.4101	NaN	-2.7062	NaN	NaN	NaN	0.4154	NaN	NaN	-0.5204	NaN	NaN	NaN	-0.0208	NaN	NaN	NaN	-1.5962	-3.1841	NaN	NaN	0.1600	NaN	NaN	NaN	NaN	2.8733	-1.7161	NaN	NaN	NaN	0.3565	NaN	0.2159	2.2548	NaN	NaN	-0.0896	0.2780	NaN	NaN	NaN	NaN	-0.2114	-0.4930	NaN	-0.3656	NaN	-0.2779	NaN	NaN	-1.3709	NaN	-2.6813	NaN	0.1994	NaN	-1.0024	NaN	1.8834	NaN	NaN	NaN	NaN	1.7259	-1.2739	-0.2663	1.4815	-1.2092	NaN	NaN	-0.2006	NaN	3.0052	NaN	NaN	NaN	-0.2157	-0.1526	-1.2504	0.6275	0.2068	-0.0258	-1.0609	NaN	NaN	-1.4144	-1.1048	NaN	NaN	NaN	-1.4503	2.0799	NaN	NaN	1.0703	NaN	NaN	NaN	NaN	NaN	NaN
# XP_024303909.1:s873	TKPPLDHNAsATDYK	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-1.1190	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.3608	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.8803	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.0090	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.2606	NaN	NaN	NaN	0.4477	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.7948	NaN	NaN	NaN	0.1721	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.7056	0.1713	NaN	NaN	NaN
# XP_024303909.1:s886	FSSSIENsDSPVR	NaN	-0.5167	NaN	0.2915	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.1591	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.8157	-1.4754	NaN	NaN	NaN	0.1242	NaN	NaN	1.8084	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.0202	NaN	NaN	NaN	NaN	NaN	NaN	0.4704	1.1340	-0.1250	NaN	NaN	NaN	NaN	NaN	0.5354	0.8219	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.3216	0.7161	NaN	NaN	NaN	-0.0636	NaN	NaN	0.7991	NaN	0.1221	NaN	NaN	NaN	0.0391	NaN	NaN	NaN	NaN	NaN	1.3794	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN
# XP_024303909.1:s888	FSSSIENSDsPVR	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.1967	NaN	NaN	0.5223	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.3899	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.4117	0.7932	NaN	NaN	NaN	NaN	NaN	0.4063	NaN	NaN	NaN	NaN	0.8173	NaN	NaN	NaN	NaN	NaN	NaN	0.0858	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.5159	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	-0.0779	NaN
# XP_024303909.1:t1237	GLASPtAITPVASPICGK	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.6122	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	2.3238	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	0.5386	0.2290	NaN	NaN	NaN	NaN	NaN	NaN	-1.1077	-0.0019	NaN	NaN	NaN	NaN	NaN	NaN	NaN	1.1287	NaN	NaN	NaN	NaN	0.6286	NaN	NaN	1.4322	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	2.3342	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN	NaN


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

# The URL for PDC API calls
# url = 'https://pdc.esacinc.com/graphql'

# query to get file metadata


print('Importing regular expression module re  ...')
import re



# 3/12/20
# For debugging runs, we set this to small number.
# LIMIT_ALLOWED_ON_NUM_OF_DATA_ROWS_ONE_ROW_PER_ALIQUOT_PER_GENE = 10
#
# production setting
# LIMIT_ALLOWED_ON_NUM_OF_DATA_ROWS_ONE_ROW_PER_ALIQUOT_PER_GENE = 1,000,000,000
LIMIT_ALLOWED_ON_NUM_OF_DATA_ROWS_ONE_ROW_PER_ALIQUOT_PER_GENE = 10000000000



NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE = 0
NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH = 0
NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0

# It is easier to upload a comma-delimited text file into
# a BigQuery table than a a tab-delimited file.
#
# (base) bash-3.2$ grep comma pdc_gene_info_dictionaries.py > COMMA_LINES_ONLY_in_pdc_gene_info_dictionaries.txt
# (base) bash-3.2$ wc COMMA_LINES_ONLY_in_pdc_gene_info_dictionaries.txt
#     1,633   15722  173676 COMMA_LINES_ONLY_in_pdc_gene_info_dictionaries.txt
# (base) bash-3.2$ 
# (base) bash-3.2$ wc pdc_gene_info_dictionaries.py 
#   411,817 1269572 24926480 pdc_gene_info_dictionaries.py
#
# FIELD_DELIMITER = "\t"
FIELD_DELIMITER = ","


# 3/17/20
# The header value below can change. Usually it is "protein_abundance_log2ratio"
# but for these four quantDataMatrix files it needs to be different:
#
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   6153770 Jan 28 15:43 S016-1.all_peptides_ion_intensity_rounded.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'    
  # -rw-rw-rw-@  1 ronaldtaylor  staff   2330788 Jan 28 15:44 S016-1.all_peptides_log10_ion_intensity.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-1'] = 'TCGA_Ovarian_JHU_Glycoproteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Glycoproteome'] = 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   1225683 Jan 28 15:36 S020-1.all_peptides_log2ratio_rounded.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-1'] = 'Prospective_Colon_VU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_VU_Proteome'] = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff  28496284 Jan 28 15:43 S037-1.all_peptides_log10_ion_intensity.tsv
#
PROTEIN_ABUNDANCE_FIELD_HEADER = "protein_abundance_log2ratio"
gene_to_row_dict={}
gene_to_row_data_values_dict={}

protein_to_row_dict={}
protein_to_row_data_values_dict={}

header_row = ""
aliquot_submitter_id_row = ""
aliquot_alias_row        = ""
morphology_row           = ""
primary_diagnosis_row    = ""
tumor_grade_row          = ""
tumor_stage_row          = ""                  

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# import_pdc_dictionaries = "NO"
import_pdc_dictionaries = "YES"

# import_pdc_aliquot_dictionaries = "NO"
import_pdc_aliquot_dictionaries = "YES"

# import_pdc_gene_info_dictionaries = "NO"
import_pdc_gene_info_dictionaries = "YES"

# %%%%%%%%%%%%%%%


# created by the program
#  PDC_GraphQL_data_retrieval.py
#
# import_pdc_file_name                = "pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
# import_pdc_file_name_without_suffix = "pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data"

import_pdc_file_name                = "pdc_AA_Tues_3_9_20_LATEST_dictionaries_for_pdc_proteomics_data.py"
import_pdc_file_name_without_suffix = "pdc_AA_Tues_3_9_20_LATEST_dictionaries_for_pdc_proteomics_data"


# created by the program
#  PDC_GraphQL_data_retrieval.py
#
import_pdc_aliquot_file_name                = "pdc_biospecimen_data_dicts.py"
import_pdc_aliquot_file_name_without_suffix = "pdc_biospecimen_data_dicts"


# created by the program
#  read_PDC_gene_info_into_dictionaries.py
#
import_pdc_gene_info_file_name                = "pdc_gene_info_dictionaries.py"
import_pdc_gene_info_file_name_without_suffix = "pdc_gene_info_dictionaries"

if import_pdc_gene_info_dictionaries == "YES":
   print("")
   print("Importing dictionaries from " + import_pdc_gene_info_file_name + " as pdc_gene ...")
   import pdc_gene_info_dictionaries as pdc_gene
   

if import_pdc_aliquot_dictionaries == "YES":
   print("")
   print("Importing dictionaries from " + import_pdc_aliquot_file_name + " as pdc_aliquot ...")
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
   # import pdc_AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data as pdc
   import pdc_AA_3_9_20_LATEST_dictionaries_for_pdc_proteomics_data as pdc   


   # %%%%%%%%%%%%%%%%%%%%%%%%

# 2/15/20
# The below function is unfinished.
# At present, only one dictionary is populated, each entry with an entire row:
#  gene_to_row_dict[geneID]= lineWithoutNewline

def read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_dictionaries(inputfile, inputfile_name, outfile):

   global NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE

   global header_row   
   global aliquot_submitter_id_row
   global aliquot_alias_row
   global morphology_row
   global primary_diagnosis_row
   global tumor_grade_row
   global tumor_stage_row
   
   Outfile1 = open(outfile, 'w')   

   print('')
   print('')

   print("-----")
   print("read_PDC_study_proteome_quanDataMatrix_spreadsheet_into_dictionaries() STARTED")


   Outfile1.write("# ------\n")
   Outfile1.write("# This file stores the data from a PDC study quantDataMatrix tab-delimited file into a set of line vars and a Python dictionary with the gene ID as key.\n")

   Outfile1.write("# input file = " + inputfile  + "\n")

   Outfile1.write("# program that created this outfile = python_read_PDC_study_quantDataMatrix_into_BQ_table_input_format.py" + "\n")
   Outfile1.write("# name of this file = " + outfile + "\n")
   Outfile1.write("# ------\n")

   dataLineNum = 0;
   
   with open(inputfile, 'r') as f:

    # We discard the first two lines in the data file.
      
    ignoreline1 = f.readline()
    lineWithoutNewline = ignoreline1.strip()
    print("ignoreline1 = '" + lineWithoutNewline + "'")    

    ignoreline2 = f.readline()
    lineWithoutNewline = ignoreline2.strip()
    print("ignoreline2 = '" + lineWithoutNewline + "'")    

    # There is a header line here, followed by several other data lines,
    # before we get the lines with protein abundance data for each geneID
    
    headerline = f.readline()
    headerlineWithoutNewline = headerline.strip()
    # print("headerline = '" + headerline + "'")    
    # print("headerline stripped = '" + headerlineWithoutNewline + "'")
    header_row = headerlineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_submitter_id_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_submitter_id_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_alias_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_alias_row = lineWithoutNewline
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("morphology_row stripped = '" + lineWithoutNewLine + "'")    
    morphology_row = lineWithoutNewline
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("primary_diagnosis_row stripped = '" + lineWithoutNewLine + "'")    
    primary_diagnosis_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_grade_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_grade_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_stage_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_stage_row = lineWithoutNewline
    
    dataLineNum = 0
    while True:
      line = f.readline()
      if not line: break      
      dataLineNum += 1
      # 
      lineWithoutNewline = line.strip()

      # print('data line ' + str(dataLineNum) + ' = ' + lineWithoutNewline)

      columns = lineWithoutNewline.split('\t')
      # print ('columns=', columns)
      geneID = columns[0]
      print ('geneID=' + geneID)
      gene_to_row_dict[geneID]= lineWithoutNewline
   
   f.close()

   print("number of data lines = " + str(dataLineNum) )
   NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE = dataLineNum
   
   Outfile1.write("# \n")
   Outfile1.write("# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read from input file = " + str(NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE) + "\n")

   Outfile1.write("# -----\n")
   Outfile1.write("# \n")   
   Outfile1.write("# input file = " + inputfile + "\n")   
   Outfile1.write("# \n")      
   Outfile1.write("# -----\n")
   Outfile1.write("# \n")
   
   Outfile1.write("gene_to_row_dict={}" + "\n")
      
   Outfile1.write("# \n")   
   Outfile1.write("# -----\n")
   Outfile1.write("# \n")
   Outfile1.write("# \n")
   
   gene_num = 0   
   for key in sorted ( gene_to_row_dict.keys() ):
     geneID = key
     data_row = gene_to_row_dict[geneID]
     # print(key, value)
     gene_num += 1
     Outfile1.write("#\n")
     Outfile1.write("# gene " + str(gene_num) + geneID + "\n")          
     Outfile1.write("gene_to_row_dict['" + geneID  + "']='" + data_row + "'\n")     
     Outfile1.write("#\n")
     #     

   Outfile1.write("\n")
   Outfile1.write("\n")
   Outfile1.write("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("header_row = '" + header_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("aliquot_submitter_id_row = '" + aliquot_submitter_id_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("aliquot_alias_row = '" + aliquot_alias_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("morphology_row = '" + morphology_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("primary_diagnosis_row = '" + primary_diagnosis_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("tumor_grade_row = '" + tumor_grade_row + "'\n")
   Outfile1.write("\n")
   Outfile1.write("\n")      
   Outfile1.write("tumor_stage_row = '" + tumor_stage_row + "'\n")


   Outfile1.write("#\n")
   Outfile1.write("#   ----  ENDFILE ---- \n")     
   Outfile1.close()

   print("read_PDC_study_proteome_quanDataMatrix_spreadsheet_into_dictionaries() ENDED")   
   print('')
   
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
   
def read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format(inputfile, inputfile_name,
                                                                                                           study_name, study_id, outfile,
                                                                                                           outfile_with_all_rows, outfile_with_all_rows_with_added_gene_info,
                                                                                                           number_of_starting_columns_to_ignore):

   global NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE

   global header_row   
   global aliquot_submitter_id_row
   global aliquot_alias_row
   global morphology_row
   global primary_diagnosis_row
   global tumor_grade_row
   global tumor_stage_row
   
   Outfile1 = open(outfile, 'w')
   OutfileAllRows = open(outfile_with_all_rows, 'w')
   OutfileAllRowsPlusGeneInfo = open(outfile_with_all_rows_with_added_gene_info, 'w')         

   print('')
   print('')

   print("-----")
   print("read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format(outfile) STARTED")


   print("\n")
   print("# The outfile stores the data from a PDC study quantDataMatrix tab-delimited file into tab-delimited spreadsheet with modified column headers, for input into a BQ table for this study.\n")
   print("# The leftmost column in each data row is a gene_name (gene symbol).\n")      

   print("# input file = " + inputfile  + "\n")

   print("# program that creates this outfile = python_read_PDC_study_quantDataMatrix_into_BQ_table_input_format.py" + "\n")
   print("# name of the outfile = " + outfile + "\n")
   print("\n")

   dataLineNum = 0;
   
   with open(inputfile, 'r') as f:

    # We discard the first two lines in the data file.
      
    ignoreline1 = f.readline()
    lineWithoutNewline = ignoreline1.strip()
    print("ignoreline1 = '" + lineWithoutNewline + "'")    

    ignoreline2 = f.readline()
    lineWithoutNewline = ignoreline2.strip()
    print("ignoreline2 = '" + lineWithoutNewline + "'")    

    # There is a header line here, followed by several other data lines,
    # before we get the lines with gene abundance data for each gene_name
    
    headerline = f.readline()
    headerlineWithoutNewline = headerline.strip()
    # print("headerline = '" + headerline + "'")    
    # print("headerline stripped = '" + headerlineWithoutNewline + "'")
    header_row = headerlineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_submitter_id_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_submitter_id_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_alias_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_alias_row = lineWithoutNewline
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("morphology_row stripped = '" + lineWithoutNewLine + "'")    
    morphology_row = lineWithoutNewline
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("primary_diagnosis_row stripped = '" + lineWithoutNewLine + "'")    
    primary_diagnosis_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_grade_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_grade_row = lineWithoutNewline

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_stage_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_stage_row = lineWithoutNewline

    
    dataLineNum = 0
    while True:
      line = f.readline()
      if not line: break      
      dataLineNum += 1
      # 
      lineWithoutNewline = line.strip()

      # print('data line ' + str(dataLineNum) + ' = ' + lineWithoutNewline)

      columns = lineWithoutNewline.split('\t')
      # print ('columns=', columns)
      geneID = columns[0]
      # print ('proteinID=' + geneID)
      gene_to_row_dict[geneID]= lineWithoutNewline
   
   f.close()

   print("number of data lines = " + str(dataLineNum) )
   NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE = dataLineNum
   
   print("# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read from input file = " + str(NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE) )

#   print("")      
#   print("header row = " + header_row)   
#   print("")

   # Sunday 2/16/20
   # I found out from Paul Rudnick at the PDC that this will not work. The values in the header
   # line are NOT the aliquot ids. 
   # case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_id_in_header_line(header_row)

   # Instead, we can map using the aliquot_submitter_id values on the aliquot_submitter_id header line
   case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row, study_name, number_of_starting_columns_to_ignore)


   Outfile1.write(header_row + "\n")
   Outfile1.write(case_gdc_row + "\n")   
   gene_num = 0   
   for key in sorted ( gene_to_row_dict.keys() ):
     geneID = key
     data_row = gene_to_row_dict[geneID]
     # print(key, value)
     gene_num += 1
     # Outfile1.write("# gene " + str(gene_num) + geneID + "\n")          
     Outfile1.write(data_row + "\n")     


   #  %%%%%%%%%%%%%%%%%%%%%%%%
   # writing out the 2nd output file, with all rows
   
   OutfileAllRows.write(header_row + "\n")
   OutfileAllRows.write(case_gdc_row + "\n")

   # The first 4 columns in the proteome (NOT phosphoproteome, which has 2 such desc cols) type of PDC source file used here are: id, authority, ncbi_gene_id, gene_id
   #
   # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4 
   #
   # The starting rows in the source file given below have "-666" in cols 2,3, and 4
   # as markers. I change that to the None string here.


   new_row = aliquot_submitter_id_row.replace("-666", "None")
   OutfileAllRows.write(new_row + "\n")

   new_row = aliquot_alias_id_row.replace("-666", "None")   
   OutfileAllRows.write(new_row + "\n")
   
   new_row = morphology_row.replace("-666", "None")
   OutfileAllRows.write(new_row + "\n")
   
   new_row = primary_diagnosis_row.replace("-666", "None")
   OutfileAllRows.write(new_row + "\n")   

   new_row = tumor_grade_row.replace("-666", "None")
   OutfileAllRows.write(new_row + "\n")      

   new_row = tumor_stage_row.replace("-666", "None")
   OutfileAllRows.write(new_row + "\n")         
   
   gene_num = 0   
   for key in sorted ( gene_to_row_dict.keys() ):
     geneID = key
     data_row = gene_to_row_dict[geneID]
     # print(key, value)
     gene_num += 1
     OutfileAllRows.write(data_row + "\n")     


   #  %%%%%%%%%%%%%%%%%%%%%%%%
   # writing out the 3rd output file, with all rows, and with added gene info from the dictionaries I
   # have created using
   #   python_read_PDC_gene_info_into_dictionaries.py

   # The first 4 fields in the header line are: id	authority	ncbi_gene_id	gene_id

   header_row_without_first_4_fields_list = header_row.split("\t")
   header_row_without_first_4_fields_list.pop(0)
   header_row_without_first_4_fields_list.pop(0)
   header_row_without_first_4_fields_list.pop(0)
   header_row_without_first_4_fields_list.pop(0)

   header_row_without_first_4_fields = FIELD_DELIMITER.join(header_row_without_first_4_fields_list)

   # We add 9 fields (the gene_name (id),  authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix downloaded file
   #
   new_header_row = "id" + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"
   new_header_row = new_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
   new_header_row = new_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
   new_header_row = new_header_row + FIELD_DELIMITER + "updated_date"
   new_header_row = new_header_row + FIELD_DELIMITER + header_row_without_first_4_fields

   print("")
   print("header_row = " + header_row)
   print("")   
   print("header_row_without_first_4_fields = " + header_row_without_first_4_fields)   
   print("")
   print("new_header_row = " + new_header_row)   
   print("")
   
   OutfileAllRowsPlusGeneInfo.write(new_header_row + "\n")


   new_row_list = case_gdc_row.split("\t")
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")


   # The first 4 columns in the proteome (NOT phosphoproteome, which has 2 such desc cols) type of PDC source file used here are: id, authority, ncbi_gene_id, gene_id
   #
   # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
   #
   # The starting rows in the source file given below have "-666" in cols 2,3, and 4
   # as markers. I change that to the None string here.


   new_row = aliquot_submitter_id_row.replace("-666", "None")
   new_row_list = new_row.split("\t")
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")


   new_row = aliquot_alias_id_row.replace("-666", "None")
   new_row_list = new_row.split("\t")
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")

    

   new_row = morphology_row.replace("-666", "None")
   new_row_list = new_row.split("\t")   
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")


   new_row = primary_diagnosis_row.replace("-666", "None")
   new_row_list = new_row.split("\t")   
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")


   new_row = tumor_grade_row.replace("-666", "None")
   new_row_list = new_row.split("\t")   
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")

   new_row = tumor_stage_row.replace("-666", "None")
   new_row_list = new_row.split("\t")   
   # We add 9 empty fields, after the first 4 - that is, at index 4, since we start at position 0.
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_list.insert(4,"None")
   new_row_with_9_empty_fields_added = FIELD_DELIMITER.join(new_row_list)   
   OutfileAllRowsPlusGeneInfo.write(new_row_with_9_empty_fields_added + "\n")
   
   gene_num = 0

   gene_to_row_list = list(gene_to_row_dict.items())
   #
   # sort in accending order
   gene_to_row_list.sort()
   gene_to_row_sorted_dict = dict(gene_to_row_list)
   for gene_name in gene_to_row_sorted_dict.keys():
     gene_num += 1      
     data_row = gene_to_row_dict[gene_name]
     print(str(gene_num) + ") " + gene_name)

     new_row_list = data_row.split("\t")
     # We add 9 gene info fields (in reverse order, when we add, after the first 4 - that is, at index 4, since we start at position 0.

   # SAMPLE GENE DATA - showing the types of data we add
   # gene 58 = ABCA12
   #
   # The gene_name (as id - first field), authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix
   # 
   # pdc_gene_name_to_ncbi_gene_id_dict['ABCA12'] = '26154'
   # pdc_gene_name_to_authority_dict['ABCA12']    = 'HGNC:14637'
   # pdc_gene_name_to_description_dict['ABCA12']  = 'phosphoglycerate mutase 1'
   # pdc_gene_name_to_organism_dict['ABCA12']     = 'Homo sapiens'
   # pdc_gene_name_to_chromosome_dict['ABCA12']   = '2'
   # pdc_gene_name_to_locus_dict['ABCA12']        = '2q35'
   # pdc_gene_name_to_proteins_dict['ABCA12']     = 'NP_056472.2;NP_775099.2;Q86UK0;Q86UK0-2;XP_011509253.1'
   # pdc_gene_name_to_assays_dict['ABCA12']       = 'controlM'
   # pdc_gene_name_to_access_dict['ABCA12']       = 'NULL'
   # pdc_gene_name_to_cud_label_dict['ABCA12']    = 'NULL'
   # pdc_gene_name_to_updated_date_dict['ABCA12'] = '6/4/2019 16:47'
   # pdc_gene_name_to_gene_uuid_dict['ABCA12']    = 'f6bdfefa-b814-11e8-907f-0a2705229b82'

     description = pdc_gene.pdc_gene_name_to_description_dict.get(gene_name, "not_found")
     org         = pdc_gene.pdc_gene_name_to_organism_dict.get(gene_name, "not_found")
     chromo      = pdc_gene.pdc_gene_name_to_chromosome_dict.get(gene_name, "not_found")
     locus       = pdc_gene.pdc_gene_name_to_locus_dict.get(gene_name, "not_found")
     proteins    = pdc_gene.pdc_gene_name_to_proteins_dict.get(gene_name, "not_found")
     assays      = pdc_gene.pdc_gene_name_to_assays_dict.get(gene_name, "not_found")
     access      = pdc_gene.pdc_gene_name_to_access_dict.get(gene_name, "not_found")
     cud_label   = pdc_gene.pdc_gene_name_to_cud_label_dict.get(gene_name, "not_found")
     updated_date = pdc_gene.pdc_gene_name_to_updated_date_dict.get(gene_name, "not_found")
     #
     # already present
     # gene_uuid    = pdc_gene.pdc_gene_name_to_gene_uuid_dict.get(gene_name, "not_found")        

     # We use index 4, which means the 5th entry in the list
     # new_row_list.insert(4,gene_uuid)
     new_row_list.insert(4,updated_date)
     new_row_list.insert(4,cud_label)
     new_row_list.insert(4,access)
     new_row_list.insert(4,assays)
     new_row_list.insert(4,proteins)
     new_row_list.insert(4,locus)
     new_row_list.insert(4,chromo)
     new_row_list.insert(4,org)
     new_row_list.insert(4,description)
     new_row_with_gene_info_fields_added = FIELD_DELIMITER.join(new_row_list)   
     OutfileAllRowsPlusGeneInfo.write(new_row_with_gene_info_fields_added + "\n")     

   Outfile1.close()
   OutfileAllRows.close()
   OutfileAllRowsPlusGeneInfo.close()

   print("# input file = " + inputfile + "\n")
   print("# output file = " + outfile + "\n")
   print("# output file with all rows = " + outfile_with_all_rows + "\n")
   print("# output file with all rows with added gene info = " + outfile_with_all_rows_with_added_gene_info + "\n")            
   
   print("read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format(outfile) ENDED")
   print("")
   
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
   
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
      
# Sunday 2/16/20
# I found out from Paul Rudnick at the PDC that the call below will not work. The values in the header
# line are NOT the aliquot ids. 
# case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_id_in_header_line(header_row)
#
# (Though this will change: Paul wrote on Mon Feb 17th 2020:
# > Also, we now have a version of the quantDataMatrix that reads from
# > files.  This columns are "aliquot_id:aliquot_alias" which may be
# > more useful.  I'll keep you posted with a release date for that. )
#
# Instead, we can map using the aliquot_submitter_id values on the aliquot_submitter_id header line
# case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row)

# Called like so:
# create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row, study_name)


def create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row, study_name, number_of_starting_columns_to_ignore):

   print("create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row) STARTED")

   aliquot_submitter_id_row_list = aliquot_submitter_id_row.split("\t")
   num_of_columns_in_aliquot_submitter_id_row = len(aliquot_submitter_id_row_list)
   print('')

   new_row_list = []
   new_row_list.append("gdc_case_id")
   num_of_empty_cols_to_add = number_of_starting_columns_to_ignore - 1
   for x in range(0, num_of_empty_cols_to_add):
       new_row_list.append("None")
   for x in range(0, number_of_starting_columns_to_ignore):
       del aliquot_submitter_id_row_list[0]
   shortened_aliquot_submitter_id_row_list = aliquot_submitter_id_row_list
   print("")
   num_of_columns_in_shortened_aliquot_submitter_id_row = len(shortened_aliquot_submitter_id_row_list)
   print("num_of_columns_in_original_aliquot_submitter_id_row = " + str(num_of_columns_in_aliquot_submitter_id_row) )
   print("num_of_columns_in_shortened_aliquot_submitter_id_row = " + str(num_of_columns_in_shortened_aliquot_submitter_id_row) )
   print("")

   # sample entries from the dictionary file we are using for lookup:
   # START OF NEW DICTIONARY SECTION
   # number of entries in dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list = 4350
   #
   # entry 1
   # number of values = 1
   # dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list['CPTAC GBM Discovery Study - Proteome::CPT0206330003'] = '104dc141-2139-11ea-aee1-0e1aae319e49'
   #
   # entry 2
   # number of values = 1
   # dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list['CPTAC GBM Discovery Study - Proteome::CPT0186100003'] = '104da8e7-2139-11ea-aee1-0e1aae319e49'
   

   num_of_entries = 0   
   num_of_errors = 0
   num_of_entries_mapped_to_gdc_case_ids = 0


   for aliquot_submitter_id in shortened_aliquot_submitter_id_row_list:
        num_of_entries += 1     
        key = study_name + "::" + aliquot_submitter_id
        #
        aliquot_id =  pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list.get(key, "NOT_FOUND")
        if aliquot_id == "NOT_FOUND":
            print("")
            print("in create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line():")
            print("MAPPING ERROR " + str(num_of_entries) + ") " + " study_name::aliquot_submitter_id of " + key + " had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder")           
            num_of_errors += 1
            new_row_list.append("not_mapped")
        else:
           # NOTE: The aliquot_id list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.           
           gdc_case_id = pdc_aliquot.dict_aliquot_id_to_case_id.get(aliquot_id)
           # sample_id = pdc_aliquot.dict_aliquot_id_to_sample_id.get(aliquot_id)
           new_row_list.append(gdc_case_id)
           num_of_entries_mapped_to_gdc_case_ids += 1

   new_row = FIELD_DELIMITER.join(new_row_list)
   print("" )
   print("NUM OF DATA COLUMNS                             = " + str(num_of_entries) )
   print("NUM OF COLUMNS CORRECTLY MAPPED TO GDC CASE IDS = " + str(num_of_entries_mapped_to_gdc_case_ids) )   
   print("NUM OF COLUMN MAPPING ERRORS                    = " + str(num_of_errors) )
   print("" )
   print("create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row) ENDED")   
   return new_row



# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%   

# The first 4 columns in the  PDC proteome source files currently used are: id, authority, ncbi_gene_id, gene_id
# NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4

# The first 2 columns in the  PDC phosphoproteome source files currently used are: id, peptide
# NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 2

# range (start, stop[, step])
#    range() takes three arguments.
#    Out of the three 2 arguments are optional. I.e., Start and Step are the optional arguments.
#
#    A start argument is a starting number of the sequence. i.e., lower limit. By default, it starts with 0 if not specified.
#    A stop argument is an upper limit. i.e.generate numbers up to this number, The range()  function doesn’t include this number in the result.
#    The step is a difference between each number in the result. The default value of the step is 1 if not specified.
#
# Example: use
#  for x in range(0,6):
#       print(x + ",")
# to print
# 0,1,2,3,4,5,


# Sunday 2/16/20
# I found out from Paul Rudnick at the PDC that using the ids in the header line will not work, in terms of
# mapping to gdc_case_ids. The values in the header line are currently NOT the aliquot ids.
#
# (Though this will change: Paul wrote on Mon Feb 17th 2020:
# > Also, we now have a version of the quantDataMatrix that reads from
# > files.  This columns are "aliquot_id:aliquot_alias" which may be
# > more useful.  I'll keep you posted with a release date for that. )

# Instead, we can map using the aliquot_submitter_id values on the aliquot_submitter_id header line
# case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_submitter_id_header_line(aliquot_submitter_id_row)

# Note that
#   aliquot_submitter_id_row_list
# that is passed in to
#      
#   create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list(aliquot_submitter_id_row_list, study_name)
#      
# has already been shortened by its first 4 fields for a proteome file, by its first 2 fields for a phosphoproteome file
# These omitted fields are not aliquot_submitter_id values. In the list used here, we have  only the aliquot_submitter_id values.
#
      
def create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list(aliquot_submitter_id_row_list, study_name):

   print("create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list() STARTED")

   num_of_columns_in_header_row = len(aliquot_submitter_id_row_list)
   print('')

   new_row_list = []

   # sample entries from the dictionary file we are using for lookup:
   # START OF NEW DICTIONARY SECTION
   # number of entries in dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list = 4350
   #
   # entry 1
   # number of values = 1
   # dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list['CPTAC GBM Discovery Study - Proteome::CPT0206330003'] = '104dc141-2139-11ea-aee1-0e1aae319e49'
   #
   # entry 2
   # number of values = 1
   # dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list['CPTAC GBM Discovery Study - Proteome::CPT0186100003'] = '104da8e7-2139-11ea-aee1-0e1aae319e49'
   

   num_of_entries = 0   
   num_of_errors = 0
   num_of_entries_mapped_to_gdc_case_ids = 0

   for aliquot_submitter_id in aliquot_submitter_id_row_list:
        num_of_entries += 1
        print("aliquot_submitter_id # =  " + str(num_of_entries), ", aliquot_submitter_id = '" + aliquot_submitter_id + "', study_name = '" + study_name + "'")
        
        key = study_name + "::" + aliquot_submitter_id
        #
        if aliquot_submitter_id == "":
               print("")
               print("in create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list():")
               print("on aliquot_submitter_id entry # =  " + str(num_of_entries))            
               print("aliquot_submitter_id = '" + aliquot_submitter_id + "'")
               print("study_name           = '" + study_name + "'")
               print("aliquot_submitter_id is EMPTY")                                             
               print("MAPPING ERROR for " + " study_name::aliquot_submitter_id key of '" + key + "' had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder")
               print("")            
               num_of_errors += 1
               new_row_list.append("not_mapped")
               continue
        #
        # NOTE: The aliquot_id_list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.        
        aliquot_id =  pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list.get(key, "NOT_FOUND")
        #    
        if aliquot_id == "NOT_FOUND":
            # 3/16/20
            # We may have an entry like so, with multiple aliquot_submitter_ids:
            #
            #   on aliquot_submitter_id entry # =  118
            #   aliquot_submitter_id = 'CPT0182890003, CPT0182910002, CPT0182910003, CPT0182910004, CPT0182910005'
            #   study_name           = 'CPTAC UCEC Discovery Study - Proteome'
            #
            # If this is the case, then we check each aliquot_submitter_id in the list of such individually and, if and only if all the aliquot_submitter_id values point to the same
            # aliquot_id, then we can still succeed and return that one aliquot_id. If not, then we report a mapping error (to multiple aliquot_ids).
            #
            #
            list_of_one_or_more_aliquot_submitter_ids = aliquot_submitter_id.split(",")
            if len(list_of_one_or_more_aliquot_submitter_ids) > 1:
               # We possibly have multiple alquot_submitter_ids.
               # We see if all the aliquot_submitter_ids point to the same aliquot_id. If so, then success. If not, report failure.
               aliquot_id_mapped_to_by_all_alqiuot_submitter_ids = find_the_common_aliquot_id_mapped_to_by_multiple_aliquot_submitter_ids_if_it_exists(list_of_one_or_more_aliquot_submitter_ids, study_name)
               if aliquot_id_mapped_to_by_all_alqiuot_submitter_ids == "NO_UNIQUE_MAPPING":
                 print("")
                 print("in create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list():")
                 print("on aliquot_submitter_id entry # =  " + str(num_of_entries))            
                 print("aliquot_submitter_id (multiple ids) = '" + aliquot_submitter_id + "'")
                 print("study_name                          = '" + study_name + "'")
                 print("The keys formed do not point to an COMMON entry in pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list")
                 print("MAPPING ERROR for " + " study_name::aliquot_submitter_id keys " + " had no COMMON mapping to a single parent aliquot_id; 'not_mapped' is used as a placeholder")
                 print("")            
                 num_of_errors += 1
                 new_row_list.append("not_mapped")
               else:
                 # We have a success - the mutiple keys formed by study:aliquot_submitter_id for the multipel aliquot_submitter_id values point to a single
                 # shared aliquot_id_mapped_to_by_all_alqiuot_submitter_ids held in COMMON.
                 #
                 # NOTE: The aliquot_id list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.
                 #
                 gdc_case_id = pdc_aliquot.dict_aliquot_id_to_case_id.get(aliquot_id_mapped_to_by_all_alqiuot_submitter_ids)
                 # sample_id = pdc_aliquot.dict_aliquot_id_to_sample_id.get(aliquot_id)
                 new_row_list.append(gdc_case_id)
                 num_of_entries_mapped_to_gdc_case_ids += 1
            else:
               # Only one aliquot_submitter_id is possibly present, and at this point it has already failed as part of a dict key - so it does not map to any aliquot_id.
               print("")
               print("in create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list():")
               print("on aliquot_submitter_id entry # =  " + str(num_of_entries))            
               print("aliquot_submitter_id = '" + aliquot_submitter_id + "'")
               print("study_name           = '" + study_name + "'")
               print("key                  = '" + key + "'")
               print("The key formed does not point to an entry in pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list")
               print("MAPPING ERROR for " + " study_name::aliquot_submitter_id key of '" + key + "' had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder")
               print("")            
               num_of_errors += 1
               new_row_list.append("not_mapped")
        else:
           # We have a success - the key formed by study:aliqout_submitter_id points to an aliquot_id (aliquot_id_list) we can use.
           #
           # NOTE: The aliquot_id_list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.
           #
           gdc_case_id = pdc_aliquot.dict_aliquot_id_to_case_id.get(aliquot_id)
           # sample_id = pdc_aliquot.dict_aliquot_id_to_sample_id.get(aliquot_id)
           new_row_list.append(gdc_case_id)
           num_of_entries_mapped_to_gdc_case_ids += 1

   print("" )
   print("NUM OF DATA COLUMNS                             = " + str(num_of_entries) )
   print("NUM OF COLUMNS CORRECTLY MAPPED TO GDC CASE IDS = " + str(num_of_entries_mapped_to_gdc_case_ids) )   
   print("NUM OF COLUMN MAPPING ERRORS                    = " + str(num_of_errors) )
   print("" )
   print("create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list() ENDED")   
   return new_row_list

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def find_the_common_aliquot_id_mapped_to_by_multiple_aliquot_submitter_ids_if_it_exists(aliquot_submitter_id_list, study_name):


   print("")      
   print("find_the_common_aliquot_id_mapped_to_by_multiple_aliquot_submitter_ids_if_it_exists(aliquot_submitter_id_list STARTED")
   current_aliquot_id = "not_set"
   aliquot_id_mapped_to_by_all_alqiuot_submitter_ids = "NO_UNIQUE_MAPPING"
   num_of_entries = 0
   print("aliquot_submitter_id_list = '" + str(aliquot_submitter_id_list) + "'")   

   for aliquot_submitter_id in aliquot_submitter_id_list:
        num_of_entries += 1
        key = study_name + "::" + aliquot_submitter_id
        aliquot_id =  pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list.get(key, "NOT_FOUND")
        print(str(num_of_entries) + ") aliquot_submitter_id = '" + aliquot_submitter_id + "', aliquot_id found = '" + aliquot_id + "'")
        #
        if current_aliquot_id == "not_set":
          current_aliquot_id = aliquot_id
        else:
          if current_aliquot_id == aliquot_id and not(aliquot_id == "NOT_FOUND"):
            # We have matches so far; we continue
            continue
          else:
            # At least aliquot_id found does not match the aliquot_id pointed to by the aliquot_submitter_ids used earlier in our input list
            current_aliquot_id = "NO_UNIQUE_MAPPING"
            break
        
   aliquot_id_mapped_to_by_all_alqiuot_submitter_ids = current_aliquot_id
   print("aliquot_id_mapped_to_by_all_alqiuot_submitter_ids = '" + aliquot_id_mapped_to_by_all_alqiuot_submitter_ids + "'")
   print("find_the_common_aliquot_id_mapped_to_by_multiple_aliquot_submitter_ids_if_it_exists(aliquot_submitter_id_list ENDED")   
   print("")
              
   return aliquot_id_mapped_to_by_all_alqiuot_submitter_ids

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Mon 2/17/20
# Kawther, David P, and I reviewed a BQ table created using a spreadsheet that I created using the
#  def read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format()
# above.
#
# It was decided, to match the gene expression data already in BQ
# tables, to create an additional transposed table.  For example,
# suppose that we have 10,000 genes in a PDC study, each with 110
# protein abundance log2 values, with each value coming from a given
# aliquot_id, its parent sample_id, and its parent gdc_case_id. The
# values for a given gene are spread over a number of columns in the
# studyDataMatrix file that I get from PDC.
#
# Then we have
# 110 * 10,000 = 1,100,000 rows in our transposed output file, with
# each row having one data value for one gene id, for one aliquot_id,
# sample_id, gdc_case_id combination.
#
# The function below builds such a BQ input file.

   # The gene_name (as id - first field), authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix
   #
   # However, authority, ncbi_gene_id, and gene_uuid fields are also already present in the gene info dictionaries that
   # I created using
   #   python_read_PDC_gene_info_into_dictionaries.py
   #
   # and we can add values for such on each gene row we output, using the dictionary entries.
   #
   # SAMPLE GENE DATA - showing the types of data we add
   # gene 58 = ABCA12
   #
   # pdc_gene_name_to_ncbi_gene_id_dict['ABCA12'] = '26154'
   # pdc_gene_name_to_authority_dict['ABCA12']    = 'HGNC:14637'
   # pdc_gene_name_to_description_dict['ABCA12']  = 'phosphoglycerate mutase 1'
   # pdc_gene_name_to_organism_dict['ABCA12']     = 'Homo sapiens'
   # pdc_gene_name_to_chromosome_dict['ABCA12']   = '2'
   # pdc_gene_name_to_locus_dict['ABCA12']        = '2q35'
   # pdc_gene_name_to_proteins_dict['ABCA12']     = 'NP_056472.2;NP_775099.2;Q86UK0;Q86UK0-2;XP_011509253.1'
   # pdc_gene_name_to_assays_dict['ABCA12']       = 'controlM'
   # pdc_gene_name_to_access_dict['ABCA12']       = 'NULL'
   # pdc_gene_name_to_cud_label_dict['ABCA12']    = 'NULL'
   # pdc_gene_name_to_updated_date_dict['ABCA12'] = '6/4/2019 16:47'
   # pdc_gene_name_to_gene_uuid_dict['ABCA12']    = 'f6bdfefa-b814-11e8-907f-0a2705229b82'

def read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile):

   global NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE

   global header_row   
   global aliquot_submitter_id_row
   global aliquot_alias_row
   global morphology_row
   global primary_diagnosis_row
   global tumor_grade_row
   global tumor_stage_row
   
   Outfile1 = open(transposed_outfile, 'w')

   print('')
   print('')

   print("-----")
   print("read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name,study_name, study_id, transposed_outfile) STARTED")


   print("\n")
   print("# The outfile stores the data from a PDC study quantDataMatrix tab-delimited file into transposd tab-delimited spreadsheet, for input into a BQ table for this study.\n")

   print("# input file = " + inputfile  + "\n")

   print("# program that creates this outfile = python_read_PDC_study_quantDataMatrix_into_BQ_table_input_format.py" + "\n")
   print("# name of the outfile = " + transposed_outfile + "\n")
   print("\n")

   dataLineNum = 0;
   
   with open(inputfile, 'r') as f:

    # We discard the first two lines in the data file.
      
    ignoreline1 = f.readline()
    lineWithoutNewline = ignoreline1.strip()
    print("ignoreline1 = '" + lineWithoutNewline + "'")    

    ignoreline2 = f.readline()
    lineWithoutNewline = ignoreline2.strip()
    print("ignoreline2 = '" + lineWithoutNewline + "'")    

    # There is a header line here, followed by several other data lines,
    # before we get the lines with protein abundance data for each geneID
    
    headerline = f.readline()
    headerlineWithoutNewline = headerline.strip()
    # print("headerline = '" + headerline + "'")    
    # print("headerline stripped = '" + headerlineWithoutNewline + "'")
    header_row = headerlineWithoutNewline

    header_row_list = header_row.split('\t')
    print("type(header_row_list) = '" + str( type(header_row_list) ) + "'")    
    len1 = len(header_row_list)
    # print("header_row_list = '" + str(header_row_list) + "'")
    # print("")    
    #
    # We remove the FIRST FOUR fields: id, authority, ncbi_gene_id, and gene_uuid, leaving the column headers for the gene data values
    f1 = header_row_list.pop(0)
    f2 = header_row_list.pop(0)
    f3 = header_row_list.pop(0)
    f4 = header_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of header row = " + str(len1))            
    print("Field1 popped from header row = '" + f1 + "'")
    print("Field2 popped from header row = '" + f2 + "'")
    print("Field3 popped from header row = '" + f3 + "'")
    print("Field4 popped from header row = '" + f4 + "'")     
    print("")
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_submitter_id_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_submitter_id_row = lineWithoutNewline
    aliquot_submitter_id_row_list = aliquot_submitter_id_row.split('\t')
    len1 = len(aliquot_submitter_id_row_list)        
    f1 = aliquot_submitter_id_row_list.pop(0)
    f2 = aliquot_submitter_id_row_list.pop(0)
    f3 = aliquot_submitter_id_row_list.pop(0)
    f4 = aliquot_submitter_id_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of aliquot_submitter_id row = " + str(len1))            
    print("Field1 popped from aliquot_submitter_id row = '" + f1 + "'")
    print("Field2 popped from aliquot_submitter_id row = '" + f2 + "'")
    print("Field3 popped from aliquot_submitter_id row = '" + f3 + "'")
    print("Field4 popped from aliquot_submitter_id row = '" + f4 + "'")     
    print("")
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_alias_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_alias_row = lineWithoutNewline
    aliquot_alias_row_list = aliquot_alias_row.split('\t')
    len1 = len(aliquot_alias_row_list)            
    f1 = aliquot_alias_row_list.pop(0)
    f2 = aliquot_alias_row_list.pop(0)
    f3 = aliquot_alias_row_list.pop(0)
    f4 = aliquot_alias_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of aliquot_alias row = " + str(len1))            
    print("Field1 popped from aliquot_alias row = '" + f1 + "'")
    print("Field2 popped from aliquot_alias row = '" + f2 + "'")
    print("Field3 popped from aliquot_alias row = '" + f3 + "'")
    print("Field4 popped from aliquot_alias row = '" + f4 + "'")     
    print("")
    
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("morphology_row stripped = '" + lineWithoutNewLine + "'")    
    morphology_row = lineWithoutNewline
    morphology_row_list = morphology_row.split('\t')
    len1 = len(morphology_row_list)                
    f1 = morphology_row_list.pop(0)
    f2 = morphology_row_list.pop(0)
    f3 = morphology_row_list.pop(0)
    f4 = morphology_row_list.pop(0)    

    print("")
    print("DEBUG CHECK:")
    print("len of morphology row = " + str(len1))            
    print("Field1 popped from morphology row = '" + f1 + "'")
    print("Field1 popped from morphology row = '" + f2 + "'")
    print("Field1 popped from morphology row = '" + f3 + "'")
    print("Field1 popped from morphology row = '" + f4 + "'")    
    print("")

    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("primary_diagnosis_row stripped = '" + lineWithoutNewLine + "'")
    primary_diagnosis_row = lineWithoutNewline    
    primary_diagnosis_row_list = primary_diagnosis_row.split('\t')
    len1 = len(primary_diagnosis_row_list)            
    f1 = primary_diagnosis_row_list.pop(0)
    f2 = primary_diagnosis_row_list.pop(0)
    f3 = primary_diagnosis_row_list.pop(0)
    f4 = primary_diagnosis_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of primary_diagnosis row = " + str(len1))            
    print("Field1 popped from primary_diagnosis row = '" + f1 + "'")
    print("Field1 popped from primary_diagnosis row = '" + f2 + "'")
    print("Field1 popped from primary_diagnosis row = '" + f3 + "'")
    print("Field1 popped from primary_diagnosis row = '" + f4 + "'")    
    print("")

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_grade_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_grade_row = lineWithoutNewline
    tumor_grade_row_list = tumor_grade_row.split('\t')
    len1 = len(tumor_grade_row_list)                
    f1 = tumor_grade_row_list.pop(0)
    f2 = tumor_grade_row_list.pop(0)
    f3 = tumor_grade_row_list.pop(0)
    f4 = tumor_grade_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of tumor_grade row = " + str(len1))            
    print("Field1 popped from tumor_grade row = '" + f1 + "'")
    print("Field1 popped from tumor_grade row = '" + f2 + "'")
    print("Field1 popped from tumor_grade row = '" + f3 + "'")
    print("Field1 popped from tumor_grade row = '" + f4 + "'")    
    print("")
    

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_stage_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_stage_row = lineWithoutNewline
    tumor_stage_row_list = tumor_stage_row.split('\t')
    len1 = len(tumor_stage_row_list)                    
    f1 = tumor_stage_row_list.pop(0)
    f2 = tumor_stage_row_list.pop(0)
    f3 = tumor_stage_row_list.pop(0)
    f4 = tumor_stage_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of tumor_stage row = " + str(len1))            
    print("Field1 popped from tumor_stage row = '" + f1 + "'")
    print("Field1 popped from tumor_stage row = '" + f2 + "'")
    print("Field1 popped from tumor_stage row = '" + f3 + "'")
    print("Field1 popped from tumor_stage row = '" + f4 + "'")    
    print("")
    
    
    dataLineNum = 0
    #
    # 3/12/20 The LIMIT val is used in debugging runs, to create
    # small output files that can be brought into, for example, Excel,
    # for easy viewing.
    # LIMIT_ALLOWED_ON_NUM_OF_DATA_ROWS_ONE_ROW_PER_ALIQUOT_PER_GENE = 10
    #
    while dataLineNum < LIMIT_ALLOWED_ON_NUM_OF_DATA_ROWS_ONE_ROW_PER_ALIQUOT_PER_GENE:    
      gene_line = f.readline()
      if not gene_line: break      
      dataLineNum += 1
      # 
      lineWithoutNewline = gene_line.strip()

      # print('data line ' + str(dataLineNum) + ' = ' + lineWithoutNewline)

      gene_line_list = lineWithoutNewline.split('\t')
      # print ('columns=', columns)
      gene_name = gene_line_list[0]
      # print ('geneID=' + gene_name)
      gene_line_list.pop(0)
      gene_line_list.pop(0)
      gene_line_list.pop(0)
      gene_line_list.pop(0)      
      gene_line_data_values_only_list = gene_line_list
      #
      # We store the protein abundance data values list as a value
      gene_to_row_data_values_dict[gene_name]= gene_line_data_values_only_list
   
   f.close()

   print("number of data lines = " + str(dataLineNum) )
   NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE = dataLineNum
   
   print("# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read from input file = " + str(NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE) )

   # Sunday 2/16/20
   # I found out from Paul Rudnick at the PDC that this will not work. The values in the header
   # line are NOT the aliquot ids. 
   # case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_id_in_header_line(header_row)

   # Instead, we can map using the aliquot_submitter_id values on the aliquot_submitter_id header line
   # Note that
   #   aliquot_submitter_id_row_list
   # has already been shortened by its first 4 fields for a proteome file, by its first 2 fields for a phosphoproteome file
   # These omitted fields are not aliquot_submitter_id values. In the list used here, we have  only the aliquot_submitter_id values.
   #
   case_gdc_row_list = create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list(aliquot_submitter_id_row_list, study_name)
   

   #  %%%%%%%%%%%%%%%%%%%%%%%%
   # writing out the transposed output file, with added gene info from the dictionaries I
   # have created using
   #   python_read_PDC_gene_info_into_dictionaries.py

   # transposed_header_row = "gene_name" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "study_name"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "aliquot_submitter_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "aliquot_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "sample_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gdc_case_id"             
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"

   # Tues 2/18/20
   # After a discussion with Kawther, I switched to the below field ordering, to
   # better match the field order for the RNAseq data in our BQ tables.
   # note: gene_symbol below = gene_name used above
   #
   transposed_header_row = "study_name" + FIELD_DELIMITER + "gdc_case_id" + FIELD_DELIMITER + "sample_id" + FIELD_DELIMITER + "aliquot_id" + FIELD_DELIMITER + "aliquot_submitter_id"   
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gene_symbol" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"

   print("")
   print("tranposed_header_row = " + transposed_header_row)
   print("")   
   
   Outfile1.write(transposed_header_row + "\n")

   gene_num = 0
   num_of_errors = 0

   gene_to_row_data_values_list = list(gene_to_row_data_values_dict.items())
   #
   # sort in ascending order
   gene_to_row_data_values_list.sort()
   gene_to_row_data_values_sorted_dict = dict(gene_to_row_data_values_list)
   for gene_name in gene_to_row_data_values_sorted_dict.keys():
     data_values_row_list = gene_to_row_data_values_dict[gene_name]
     gene_num += 1
     #
     # We use the modulus operator to time printouts
     print_time = gene_num % 500
     if (print_time == 0 ):
        print(str(gene_num) + ") " + gene_name)
     #
     aliquot_submitter_id_column_num = 0
     # data_values_row_list preserves the original data order in the source PDC spreadsheet,
     # so the aliquot_submitter_id values that we use below should still be in the right order to match the data values.
     #
     for protein_abundance_value in data_values_row_list:
        
       aliquot_submitter_id = aliquot_submitter_id_row_list[aliquot_submitter_id_column_num]
       gdc_case_id          = case_gdc_row_list[aliquot_submitter_id_column_num]

       aliquot_submitter_id_column_num += 1
       
       # We add 9 gene info fields (in reverse order, when we add, after the first 4 - that is, at index 4, since we start at position 0.

       # SAMPLE GENE DATA - showing the types of data we add
       # gene 58 = ABCA12
       #
       # The gene_name (as id - first field), authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix
       # 
       # pdc_gene_name_to_ncbi_gene_id_dict['ABCA12'] = '26154'
       # pdc_gene_name_to_authority_dict['ABCA12']    = 'HGNC:14637'
       # pdc_gene_name_to_description_dict['ABCA12']  = 'phosphoglycerate mutase 1'
       # pdc_gene_name_to_organism_dict['ABCA12']     = 'Homo sapiens'
       # pdc_gene_name_to_chromosome_dict['ABCA12']   = '2'
       # pdc_gene_name_to_locus_dict['ABCA12']        = '2q35'
       # pdc_gene_name_to_proteins_dict['ABCA12']     = 'NP_056472.2;NP_775099.2;Q86UK0;Q86UK0-2;XP_011509253.1'
       # pdc_gene_name_to_assays_dict['ABCA12']       = 'controlM'
       # pdc_gene_name_to_access_dict['ABCA12']       = 'NULL'
       # pdc_gene_name_to_cud_label_dict['ABCA12']    = 'NULL'
       # pdc_gene_name_to_updated_date_dict['ABCA12'] = '6/4/2019 16:47'
       # pdc_gene_name_to_gene_uuid_dict['ABCA12']    = 'f6bdfefa-b814-11e8-907f-0a2705229b82'

       gene_uuid    = pdc_gene.pdc_gene_name_to_gene_uuid_dict.get(gene_name, "not_found")                
       authority    = pdc_gene.pdc_gene_name_to_authority_dict.get(gene_name, "not_found")                
       ncbi_gene_id = pdc_gene.pdc_gene_name_to_ncbi_gene_id_dict.get(gene_name, "not_found")        
       description = pdc_gene.pdc_gene_name_to_description_dict.get(gene_name, "not_found")
       org         = pdc_gene.pdc_gene_name_to_organism_dict.get(gene_name, "not_found")
       chromo      = pdc_gene.pdc_gene_name_to_chromosome_dict.get(gene_name, "not_found")
       locus       = pdc_gene.pdc_gene_name_to_locus_dict.get(gene_name, "not_found")
       proteins    = pdc_gene.pdc_gene_name_to_proteins_dict.get(gene_name, "not_found")
       assays      = pdc_gene.pdc_gene_name_to_assays_dict.get(gene_name, "not_found")
       access      = pdc_gene.pdc_gene_name_to_access_dict.get(gene_name, "not_found")
       cud_label   = pdc_gene.pdc_gene_name_to_cud_label_dict.get(gene_name, "not_found")
       updated_date = pdc_gene.pdc_gene_name_to_updated_date_dict.get(gene_name, "not_found")


       aliquot_id = "ALIQUOT_ID_PLACEHOLDER"
       sample_id  = "SAMPLE_ID_PLACEHOLDER"

       key = study_name + "::" + aliquot_submitter_id
       #
       # jjjjj
       #
       aliquot_id =  pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list.get(key, "NOT_FOUND")
       if aliquot_id == "NOT_FOUND":
            # print("")
            # print("DEBUG:")
            # print(str(gene_num) + ") gene_name = " + gene_name)              
            # print("aliquot_submitter_id = " + aliquot_submitter_id)
            # print("gdc_case_id          = " + gdc_case_id)       
            # print("MAPPING ERROR #" + str(num_of_errors) + ": study_name::aliquot_submitter_id of '" + key + "' had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder")          
            num_of_errors += 1
            aliquot_id = "not_mapped"
            sample_id = "not_mapped"            
       else:
           # NOTE: The aliquot_id list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.
           # gdc_case_id = pdc_aliquot.dict_aliquot_id_to_case_id.get(aliquot_id)
           sample_id = pdc_aliquot.dict_aliquot_id_to_sample_id.get(aliquot_id)

       new_row_list = []

       
       # new_row_list.append(gene_name)
       # new_row_list.append(protein_abundance_value)
       # new_row_list.append(study_name)
       # new_row_list.append(aliquot_submitter_id)
       # new_row_list.append(aliquot_id)
       # new_row_list.append(sample_id)              
       # new_row_list.append(gdc_case_id)              
       # new_row_list.append(authority)
       # new_row_list.append(ncbi_gene_id)
       # new_row_list.append(gene_uuid)
       # new_row_list.append(description)
       # new_row_list.append(org)
       # new_row_list.append(chromo)
       # new_row_list.append(locus)
       # new_row_list.append(proteins)
       # new_row_list.append(assays)
       # new_row_list.append(access)
       # new_row_list.append(cud_label)
       # new_row_list.append(updated_date)

       # Tues 2/18/20
       # After a discussion with Kawther, I switched to the field ordering below, to
       # better match the field order for the RNAseq data in our BQ tables.
       # note: gene_symbol below = gene_name used above
       #
       # transposed_header_row = "study_name" + FIELD_DELIMITER + "gdc_case_id" + FIELD_DELIMITER + "sample_id" + FIELD_DELIMITER + "aliquot_id" + FIELD_DELIMITER + "aliquot_submitter_id"   
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gene_symbol" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"

       new_row_list.append(study_name)
       new_row_list.append(gdc_case_id)       
       new_row_list.append(sample_id)
       new_row_list.append(aliquot_id)
       new_row_list.append(aliquot_submitter_id)
       new_row_list.append(gene_name)
       new_row_list.append(protein_abundance_value)
       new_row_list.append(authority)
       new_row_list.append(ncbi_gene_id)
       new_row_list.append(gene_uuid)
       new_row_list.append(description)
       new_row_list.append(org)
       new_row_list.append(chromo)
       new_row_list.append(locus)
       new_row_list.append(proteins)
       new_row_list.append(assays)
       new_row_list.append(access)
       new_row_list.append(cud_label)
       new_row_list.append(updated_date)

       new_row_as_string = FIELD_DELIMITER.join(new_row_list)   
       Outfile1.write(new_row_as_string + "\n")     

     
   Outfile1.close()

   print("" )
   print("NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = " + str(num_of_errors) )             
   print("" )   
   print("input file = " + inputfile )
   print("output file = " + transposed_outfile)
   print("" )   
   print("read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name,study_name, study_id, transposed_outfile) ENDED")   
   print("")
   

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Mon 2/17/20
# Kawther, David P, and I reviewed a BQ table created using a spreadsheet that I created using the
#  def read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format()
# above.
#
# It was decided, to match the gene expression data already in BQ
# tables, to create an additional transposed table.  For example,
# suppose that we have 10,000 genes in a PDC study, each with 110
# protein abundance log2 values, with each value coming from a given
# aliquot_id, its parent sample_id, and its parent gdc_case_id. The
# values for a given gene are spread over a number of columns in the
# studyDataMatrix file that I get from PDC.
#
# Then we have
# 110 * 10,000 = 1,100,000 rows in our transposed output file, with
# each row having one data value for one gene id, for one aliquot_id,
# sample_id, gdc_case_id combination.
#
# The function below builds such a BQ input file.

   # The gene_name (as id - first field), authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix
   #
   # However, authority, ncbi_gene_id, and gene_uuid fields are also already present in the gene info dictionaries that
   # I created using
   #   python_read_PDC_gene_info_into_dictionaries.py
   #
   # and we can add values for such on each gene row we output, using the dictionary entries.
   #
   # SAMPLE GENE DATA - showing the types of data we add
   # gene 58 = ABCA12
   #
   # pdc_gene_name_to_ncbi_gene_id_dict['ABCA12'] = '26154'
   # pdc_gene_name_to_authority_dict['ABCA12']    = 'HGNC:14637'
   # pdc_gene_name_to_description_dict['ABCA12']  = 'phosphoglycerate mutase 1'
   # pdc_gene_name_to_organism_dict['ABCA12']     = 'Homo sapiens'
   # pdc_gene_name_to_chromosome_dict['ABCA12']   = '2'
   # pdc_gene_name_to_locus_dict['ABCA12']        = '2q35'
   # pdc_gene_name_to_proteins_dict['ABCA12']     = 'NP_056472.2;NP_775099.2;Q86UK0;Q86UK0-2;XP_011509253.1'
   # pdc_gene_name_to_assays_dict['ABCA12']       = 'controlM'
   # pdc_gene_name_to_access_dict['ABCA12']       = 'NULL'
   # pdc_gene_name_to_cud_label_dict['ABCA12']    = 'NULL'
   # pdc_gene_name_to_updated_date_dict['ABCA12'] = '6/4/2019 16:47'
   # pdc_gene_name_to_gene_uuid_dict['ABCA12']    = 'f6bdfefa-b814-11e8-907f-0a2705229b82'

def read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile):

   global NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE
   global NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH
   global NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER
   global header_row   
   global aliquot_submitter_id_row
   global aliquot_alias_row
   global morphology_row
   global primary_diagnosis_row
   global tumor_grade_row
   global tumor_stage_row

   row_num_to_row_data_values_dict = {}
   row_num_to_protein_name_dict = {}
   row_num_to_protein_name_plus_mutation_site_dict = {}      
   row_num_to_peptide_dict = {}   
   
   Outfile1 = open(transposed_outfile, 'w')

   print('')
   print('')

   print("-----")
   print("PHOSPHO run - read_PDC_study_phophoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name,study_name, study_id, transposed_outfile) STARTED")


   print("\n")
   print("# The outfile stores the data from a PDC study quantDataMatrix tab-delimited file into transposd tab-delimited spreadsheet, for input into a BQ table for this study.\n")

   print("# input file = " + inputfile  + "\n")

   print("# program that creates this outfile = python_read_PDC_study_quantDataMatrix_into_BQ_table_input_format.py" + "\n")
   print("# name of the outfile = " + transposed_outfile + "\n")
   print("\n")

   dataLineNum = 0;
   
   with open(inputfile, 'r') as f:

    # We discard the first two lines in the data file.
      
    ignoreline1 = f.readline()
    lineWithoutNewline = ignoreline1.strip()
    print("ignoreline1 = '" + lineWithoutNewline + "'")    

    ignoreline2 = f.readline()
    lineWithoutNewline = ignoreline2.strip()
    print("ignoreline2 = '" + lineWithoutNewline + "'")    

    # There is a header line here, followed by several other data lines,
    # before we get the lines with protein abundance data for each geneID
    
    headerline = f.readline()
    headerlineWithoutNewline = headerline.strip()
    # print("headerline = '" + headerline + "'")    
    # print("headerline stripped = '" + headerlineWithoutNewline + "'")
    header_row = headerlineWithoutNewline

    header_row_list = header_row.split('\t')
    len1 = len(header_row_list)    
    # print("header_row_list = '" + str(header_row_list) + "'")
    # print("")    
    #
    # We remove the FIRST TWO fields: id and peptide, leaving the column headers for the gene data values
    f1 = header_row_list.pop(0)
    f2 = header_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of header row = " + str(len1))            
    print("Field1 popped from header row = '" + f1 + "'")
    print("Field2 popped from header row = '" + f2 + "'")
    print("")
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_submitter_id_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_submitter_id_row = lineWithoutNewline
    aliquot_submitter_id_row_list = aliquot_submitter_id_row.split('\t')
    len1 = len(aliquot_submitter_id_row_list)        
    f1 = aliquot_submitter_id_row_list.pop(0)
    f2 = aliquot_submitter_id_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of aliquot_submitter_id row = " + str(len1))            
    print("Field1 popped from aliquot_submitter_id row = '" + f1 + "'")
    print("Field2 popped from aliquot_submitter_id row = '" + f2 + "'")
    print("")
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("aliquot_alias_row stripped = '" + lineWithoutNewLine + "'")    
    aliquot_alias_row = lineWithoutNewline
    aliquot_alias_row_list = aliquot_alias_row.split('\t')
    len1 = len(aliquot_alias_row_list)            
    f1 = aliquot_alias_row_list.pop(0)
    f2 = aliquot_alias_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of aliquot_alias row = " + str(len1))            
    print("Field1 popped from aliquot_alias row = '" + f1 + "'")
    print("Field2 popped from aliquot_alias row = '" + f2 + "'")
    print("")
    
    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("morphology_row stripped = '" + lineWithoutNewLine + "'")    
    morphology_row = lineWithoutNewline
    morphology_row_list = morphology_row.split('\t')
    len1 = len(morphology_row_list)                
    f1 = morphology_row_list.pop(0)
    f2 = morphology_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of morphology row = " + str(len1))            
    print("Field1 popped from morphology row = '" + f1 + "'")
    print("Field1 popped from morphology row = '" + f2 + "'")
    print("")

    
    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("primary_diagnosis_row stripped = '" + lineWithoutNewLine + "'")
    primary_diagnosis_row = lineWithoutNewline    
    primary_diagnosis_row_list = primary_diagnosis_row.split('\t')
    len1 = len(primary_diagnosis_row_list)            
    f1 = primary_diagnosis_row_list.pop(0)
    f2 = primary_diagnosis_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of primary_diagnosis row = " + str(len1))            
    print("Field1 popped from primary_diagnosis row = '" + f1 + "'")
    print("Field1 popped from primary_diagnosis row = '" + f2 + "'")
    print("")

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_grade_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_grade_row = lineWithoutNewline
    tumor_grade_row_list = tumor_grade_row.split('\t')
    len1 = len(tumor_grade_row_list)                
    f1 = tumor_grade_row_list.pop(0)
    f2 = tumor_grade_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of tumor_grade row = " + str(len1))            
    print("Field1 popped from tumor_grade row = '" + f1 + "'")
    print("Field1 popped from tumor_grade row = '" + f2 + "'")
    print("")
    

    line = f.readline()
    lineWithoutNewline = line.strip()
    # print("tumor_stage_row stripped = '" + lineWithoutNewLine + "'")    
    tumor_stage_row = lineWithoutNewline
    tumor_stage_row_list = tumor_stage_row.split('\t')
    len1 = len(tumor_stage_row_list)                    
    f1 = tumor_stage_row_list.pop(0)
    f2 = tumor_stage_row_list.pop(0)

    print("")
    print("DEBUG CHECK:")
    print("len of tumor_stage row = " + str(len1))            
    print("Field1 popped from tumor_stage row = '" + f1 + "'")
    print("Field1 popped from tumor_stage row = '" + f2 + "'")
    print("")

    
    dataLineNum = 0
    while True:
      protein_line = f.readline()
      if not protein_line: break            
      #
      # strip the line of both leading and trailing chars          
      data_line    = protein_line.strip()      
      #
      dataLineNum += 1            
      #
      test_line = data_line
      # We try removing all white spaces from the test_line and see if that creates an empty line
      test_line.replace(" ", "")
      test_line.replace("\t", "")      
      
      if test_line == "":
         # skip empty lines
         print("")
         print("Skipping an empty line on line # = " + str(dataLineNum))
         print("")         
         continue
      # 
      # print('data line ' + str(dataLineNum) + " = '" + data_line + "'")
      # print('data line ' + str(dataLineNum) )      

      line_list = data_line.split('\t')
      # print ('columns=', columns)
      #
      # example: NP_000012.1:s324t327      
      protein_name_plus_mutation_site = line_list[0]
      #
      protein_name_plus_mutation_site_list = protein_name_plus_mutation_site.split(":")
      protein_name_plus_mutation_site_list_size = len(protein_name_plus_mutation_site_list)      

      if protein_name_plus_mutation_site_list_size > 1:
         protein_name = protein_name_plus_mutation_site_list[0] 
         mut_site     = protein_name_plus_mutation_site_list[1]
      else:         
         protein_name = protein_name_plus_mutation_site
         mut_site     = "MUT_SITE_IS_NOT_SPECIFIED"         
         print("")
         print("NOTE: MUTATION_SITE IS NOT GIVEN in:")
         print("  protein_name = '" + protein_name + "'")
         print("  data_line = '" + data_line + "'")
         print("")         

      # print ('proteinID=' + protein_name_plus_mutation_site)
      #
      peptide = line_list[1]
      # We remove the first two phosphoproteome file fields for protein_id and peptide
      line_list.pop(0)
      line_list.pop(0)
      line_data_values_only_list = line_list
      #
      # We store the protein abundance data values list as a value
      row_num_to_row_data_values_dict[dataLineNum] = line_data_values_only_list
      row_num_to_protein_name_dict[dataLineNum]    = protein_name
      row_num_to_protein_name_plus_mutation_site_dict[dataLineNum]    = protein_name_plus_mutation_site
      row_num_to_peptide_dict[dataLineNum]         = peptide
   
   f.close()

   print("number of data lines = " + str(dataLineNum) )
   NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE = dataLineNum
   
   print("# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read from input file = " + str(NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE) )

   # Sunday 2/16/20
   # I found out from Paul Rudnick at the PDC that this will not work. The values in the header
   # line are NOT the aliquot ids. 
   # case_gdc_row = create_row_showing_case_gdc_id_mapped_using_aliquot_id_in_header_line(header_row)

   # Instead, we can map using the aliquot_submitter_id values on the aliquot_submitter_id header line
   # Note that
   #   aliquot_submitter_id_row_list
   # has already been shortened by its first 4 fields for a proteome file, by its first 2 fields for a phosphoproteome file
   # These omitted fields are not aliquot_submitter_id values. In the list used here, we have  only the aliquot_submitter_id values.
   #
   case_gdc_row_list = create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list(aliquot_submitter_id_row_list, study_name)
   

   #  %%%%%%%%%%%%%%%%%%%%%%%%
   
   # writing out the transposed output file, with added gene info from the dictionaries I
   # have created using
   #   python_read_PDC_gene_info_into_dictionaries.py

   # transposed_header_row = "protein_name" + FIELD_DELIMITER + "peptide" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "study_name"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "aliquot_submitter_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "aliquot_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "sample_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gdc_case_id"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
   # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"


   # Tues 2/18/20
   # After a discussion with Kawther, I switched to the below field ordering, to
   # better match the field order for the RNAseq data in our BQ tables.
   # note: gene_symbol below = gene_name used above
   #
   transposed_header_row = "study_name" + FIELD_DELIMITER + "gdc_case_id" + FIELD_DELIMITER + "sample_id" + FIELD_DELIMITER + "aliquot_id" + FIELD_DELIMITER + "aliquot_submitter_id"   
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "protein_name" + FIELD_DELIMITER + "peptide"
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gene_symbol" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
   transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"

   print("")
   print("tranposed_header_row = " + transposed_header_row)
   print("")   
   
   Outfile1.write(transposed_header_row + "\n")


   row_num_to_row_data_values_list = list(row_num_to_row_data_values_dict.items())
   #
   # sort in ascending order
   #
   # Tuples are compared element by element starting from the first element which is very similar to how strings are compared.
   #
   row_num_to_row_data_values_list.sort()
   row_num_to_row_data_values_sorted_dict = dict(row_num_to_row_data_values_list)

   NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH = 0
   NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
   
   row_num = 1
   num_of_errors = 0
   
   for source_row_num in row_num_to_row_data_values_sorted_dict.keys():
     data_values_row_list = row_num_to_row_data_values_dict[source_row_num]
     protein_name         = row_num_to_protein_name_dict[source_row_num]
     protein_name_plus_mutation_site = row_num_to_protein_name_plus_mutation_site_dict[source_row_num]
     peptide              = row_num_to_peptide_dict[source_row_num]


     # An example of a protein name that is not foudn in Paul Rudnick's PDC gene info file is "NP_001075955.2"
     # (base) bash-3.2$ grep NP_001075955.2 pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_instances_removed.csv
     #
     # (base) bash-3.2$ grep NP_001075955  pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_instances_removed.csv
     #  ACD,65057,HGNC:25070,"ACD, shelterin complex subunit and telomerase recruitment factor",Homo sapiens,16,16q22.1,A0A0C4DGT6;NP_001075955.1;NP_001075956.1;NP_075065.2;Q96AP0;Q96AP0-2;R4GMR6;R4GNJ5;XP_005256172.1,,NULL,NULL,6/4/2019 16:47,f6ce835b-b814-11e8-907f-0a2705229b82

     gene_symbol = pdc_gene.pdc_protein_name_to_gene_name_dict.get(protein_name, "no_gene_match")

     # We use the modulus operator to time printouts
     print_time = row_num % 500
     if ( print_time == 0 ):
        print("")
        print(str(row_num) + ") data row in source = " + str(source_row_num) + ", protein_name = " + protein_name + ", protein_name_plus_mutation_site = " + protein_name_plus_mutation_site)
        print("                peptide = " + peptide + ", gene_symbol = " + gene_symbol)
        first_protein_abundance_value = data_values_row_list[0]
        print("                first protein_abundance_log2ratio = " + str(first_protein_abundance_value) )

     if gene_symbol == "no_gene_match":
        protein_name_array = protein_name.split(".")
        array_size = len(protein_name_array)
        if array_size > 1:
            # We have failed to find a protein-to-gene name match using full protein name including its revision number.
            # Example:
            # (full) protein_name = XP_024309873.1
            # protein_root_name = XP_024309873
            # protein_revision_number = 1
            #
            # We now try to find a protein-to-gene name match using only the protein root name, without the revision extension.
            #
            protein_root_name = protein_name_array[0]
            protein_revision_number = protein_name_array[1]
            gene_symbol = pdc_gene.pdc_protein_name_to_gene_name_dict.get(protein_root_name, "no_gene_match")
            if gene_symbol == "no_gene_match":
                 do_nothing = "";
            else:
                 NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER += 1
        else:
            protein_root_name = protein_name
            protein_revision_number = "no_revision_number"            
        print("")
        print("Values occurring on no_gene_match:")
        print("protein array_size = " + str(array_size) )
        print("(full) protein_name = " + protein_name)
        print("protein_root_name = " + protein_root_name)
        print("protein_revision_number = " + str(protein_revision_number) )
        print("gene_symbol (gene_name) found using  protein_root_name = " + gene_symbol)
        print("")        
                                                                          
     if gene_symbol == "no_gene_match":
        NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH += 1

     if gene_symbol == "no_gene_match":
        print("")
        print("NO NAME MATCH: NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH = " + str(NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH) )        
        print(str(row_num) + ") data row in source = " + str(source_row_num) + ", protein_name = " + protein_name + ", protein_name_plus_mutation_site = " + protein_name_plus_mutation_site)
        print("                peptide = " + peptide + ", gene_symbol = " + gene_symbol)
        first_protein_abundance_value = data_values_row_list[0]
        print("                first protein_abundance_log2ratio = " + str(first_protein_abundance_value) )
        print("")        

     # Here, in this code, gene_symbol means the same as gene_name
     gene_name = gene_symbol
     
     row_num += 1     

     aliquot_submitter_id_column_num = 0
     # data_values_row_list preserves the original data order in the source PDC spreadsheet,
     # so the aliquot_submitter_id values that we use below should still be in the right order to match the data values.
     #
     for protein_abundance_value in data_values_row_list:
        
       aliquot_submitter_id = aliquot_submitter_id_row_list[aliquot_submitter_id_column_num]
       gdc_case_id          = case_gdc_row_list[aliquot_submitter_id_column_num]
       
       aliquot_submitter_id_column_num += 1
       
       # We add 9 gene info fields (in reverse order, when we add, after the first 4 - that is, at index 4, since we start at position 0.

       # SAMPLE GENE DATA - showing the types of data we add
       # gene 58 = ABCA12
       #
       # The gene_name (as id - first field), authority, ncbi_gene_id, and gene_uuid fields are already present in the quantDataMatrix
       # 
       # pdc_gene_name_to_ncbi_gene_id_dict['ABCA12'] = '26154'
       # pdc_gene_name_to_authority_dict['ABCA12']    = 'HGNC:14637'
       # pdc_gene_name_to_description_dict['ABCA12']  = 'phosphoglycerate mutase 1'
       # pdc_gene_name_to_organism_dict['ABCA12']     = 'Homo sapiens'
       # pdc_gene_name_to_chromosome_dict['ABCA12']   = '2'
       # pdc_gene_name_to_locus_dict['ABCA12']        = '2q35'
       # pdc_gene_name_to_proteins_dict['ABCA12']     = 'NP_056472.2;NP_775099.2;Q86UK0;Q86UK0-2;XP_011509253.1'
       # pdc_gene_name_to_assays_dict['ABCA12']       = 'controlM'
       # pdc_gene_name_to_access_dict['ABCA12']       = 'NULL'
       # pdc_gene_name_to_cud_label_dict['ABCA12']    = 'NULL'
       # pdc_gene_name_to_updated_date_dict['ABCA12'] = '6/4/2019 16:47'
       # pdc_gene_name_to_gene_uuid_dict['ABCA12']    = 'f6bdfefa-b814-11e8-907f-0a2705229b82'

       gene_uuid    = pdc_gene.pdc_gene_name_to_gene_uuid_dict.get(gene_name, "not_found")                
       authority    = pdc_gene.pdc_gene_name_to_authority_dict.get(gene_name, "not_found")                
       ncbi_gene_id = pdc_gene.pdc_gene_name_to_ncbi_gene_id_dict.get(gene_name, "not_found")        
       description = pdc_gene.pdc_gene_name_to_description_dict.get(gene_name, "not_found")
       org         = pdc_gene.pdc_gene_name_to_organism_dict.get(gene_name, "not_found")
       chromo      = pdc_gene.pdc_gene_name_to_chromosome_dict.get(gene_name, "not_found")
       locus       = pdc_gene.pdc_gene_name_to_locus_dict.get(gene_name, "not_found")
       proteins    = pdc_gene.pdc_gene_name_to_proteins_dict.get(gene_name, "not_found")
       assays      = pdc_gene.pdc_gene_name_to_assays_dict.get(gene_name, "not_found")
       access      = pdc_gene.pdc_gene_name_to_access_dict.get(gene_name, "not_found")
       cud_label   = pdc_gene.pdc_gene_name_to_cud_label_dict.get(gene_name, "not_found")
       updated_date = pdc_gene.pdc_gene_name_to_updated_date_dict.get(gene_name, "not_found")


       aliquot_id = "ALIQUOT_ID_PLACEHOLDER"
       sample_id  = "SAMPLE_ID_PLACEHOLDER"

       key = study_name + "::" + aliquot_submitter_id
       #
       aliquot_id =  pdc_aliquot.dict_study_name_and_aliquot_submitter_id_to_aliquot_id_list.get(key, "NOT_FOUND")
       if aliquot_id == "NOT_FOUND":
            # print("MAPPING ERROR " + str(num_of_errors) + ": study_name::aliquot_submitter_id of " + key + " had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder")
            num_of_errors += 1
            aliquot_id = "not_mapped"
            sample_id = "not_mapped"            
       else:
           # NOTE: The aliquot_id list always appears to contain just one value for a given study_id and aliquot_submitter_id, so we don't have to worry about multiple values here.
           # gdc_case_id = pdc_aliquot.dict_aliquot_id_to_case_id.get(aliquot_id)
           sample_id = pdc_aliquot.dict_aliquot_id_to_sample_id.get(aliquot_id)

       new_row_list = []

       
       # new_row_list.append(gene_name)
       # new_row_list.append(protein_abundance_value)
       # new_row_list.append(study_name)
       # new_row_list.append(aliquot_submitter_id)
       # new_row_list.append(aliquot_id)
       # new_row_list.append(sample_id)              
       # new_row_list.append(gdc_case_id)              
       # new_row_list.append(authority)
       # new_row_list.append(ncbi_gene_id)
       # new_row_list.append(gene_uuid)
       # new_row_list.append(description)
       # new_row_list.append(org)
       # new_row_list.append(chromo)
       # new_row_list.append(locus)
       # new_row_list.append(proteins)
       # new_row_list.append(assays)
       # new_row_list.append(access)
       # new_row_list.append(cud_label)
       # new_row_list.append(updated_date)

       # Tues 2/18/20
       # After a discussion with Kawther, I switched to the field ordering below, to
       # better match the field order for the RNAseq data in our BQ tables.
       # note: gene_symbol below = gene_name used above
       #
       # transposed_header_row = "study_name" + FIELD_DELIMITER + "gdc_case_id" + FIELD_DELIMITER + "sample_id" + FIELD_DELIMITER + "aliquot_id" + FIELD_DELIMITER + "aliquot_submitter_id"   
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "gene_symbol" + FIELD_DELIMITER + PROTEIN_ABUNDANCE_FIELD_HEADER
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "authority" + FIELD_DELIMITER + "ncbi_gene_id" + FIELD_DELIMITER + "gene_uuid"   
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "description" + FIELD_DELIMITER + "organism" + FIELD_DELIMITER + "chromosome" + FIELD_DELIMITER + "locus"
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "proteins" + FIELD_DELIMITER + "assays" + FIELD_DELIMITER + "access" + FIELD_DELIMITER + "cud_label"
       # transposed_header_row = transposed_header_row + FIELD_DELIMITER + "updated_date"

       new_row_list.append(study_name)
       new_row_list.append(gdc_case_id)       
       new_row_list.append(sample_id)
       new_row_list.append(aliquot_id)
       new_row_list.append(aliquot_submitter_id)
       new_row_list.append(protein_name_plus_mutation_site)
       new_row_list.append(peptide)
       #
       # note: gene_id = gene_symbol
       new_row_list.append(gene_name)
       #
       new_row_list.append(protein_abundance_value)
       new_row_list.append(authority)
       new_row_list.append(ncbi_gene_id)
       new_row_list.append(gene_uuid)
       new_row_list.append(description)
       new_row_list.append(org)
       new_row_list.append(chromo)
       new_row_list.append(locus)
       new_row_list.append(proteins)
       new_row_list.append(assays)
       new_row_list.append(access)
       new_row_list.append(cud_label)
       new_row_list.append(updated_date)

       new_row_as_string = FIELD_DELIMITER.join(new_row_list)   
       Outfile1.write(new_row_as_string + "\n")     

     
   Outfile1.close()

   print("")   
   print("input file  = " + inputfile)
   print("output file = " + transposed_outfile)
   print("")
   print("total number of data rows in output file = " + str(row_num) )
   print("")   
   print("NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = " + str(NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER) )
   print("NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = " + str(NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH))
   print("")
   print("PHOSPHO run - read_PDC_study_phophoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name,study_name, study_id, transposed_outfile) ENDED")   
   print("")


# 4.5 million rows in a 2.4 GB output file:
# wc quantDataMatrix_transposed_CPTAC_GBM_Discovery_PhosphoProteome_BQ_formatted_2_25_20.txt
#  4,573,801 142696861 2606131029 quantDataMatrix_transposed_CPTAC_GBM_Discovery_PhosphoProteome_BQ_formatted_2_25_20.txt
# (base) bash-3.2$    

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Row expansion in the transposed files:

# input file            = /GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/CPTAC GBM Discovery Study - Proteome-log2_ratio.gct
# transposed_outfile    = /GDITwork/PDC_queries/quantDataMatrix_transposed_CPTAC_GBM_Discovery_Proteome_BQ_formatted_2_26_20.txt
# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 10977
#
# 10,977 data rows expanded to 1,207,470 data rows

# input file            = /GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct
# transposed_outfile    = /GDITwork/PDC_queries/quantDataMatrix_transposed_CPTAC_GBM_Discovery_PhosphoProteome_BQ_formatted_2_26_20.txt
# NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 41580
#
# 41,580 data rows expanded to 5,781,272 data rows

# wc quant*
#  4,573,801 142696861 2606131029 quantDataMatrix_transposed_CPTAC_GBM_Discovery_PhosphoProteome_BQ_formatted_2_26_20.txt
#  1,207,471 34628679 540491173 quantDataMatrix_transposed_CPTAC_GBM_Discovery_Proteome_BQ_formatted_2_26_20.txt
#  5781272 177325540 3146622202 total
# (base) bash-3.2$


# nbash-3.2$ pwd
# /GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files
# bash-3.2$ 
# -rw-rw-rw-@  1 ronaldtaylor  staff  25834920 Jan 28 11:44 CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff   9564939 Jan 28 11:59 CPTAC GBM Discovery Study - Proteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff   9517564 Jan 28 12:02 CPTAC GBM Discovery Study - Proteome-unshared_log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  84188566 Jan 28 12:38 HBV-Related Hepatocellular Carcinoma - Phosphoproteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  25125438 Jan 28 13:39 HBV-Related Hepatocellular Carcinoma - Proteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  24855358 Jan 28 13:41 HBV-Related Hepatocellular Carcinoma - Proteome-unshared_log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff   7011168 Jan 28 13:43 Pediatric Brain Cancer Pilot Study - Phosphoproteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  11811466 Jan 28 13:44 Pediatric Brain Cancer Pilot Study - Proteome-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  11672766 Jan 28 13:45 Pediatric Brain Cancer Pilot Study - Proteome-unshared_log2_ratio.gct

   
def main():

  print("Start")

  # -rw-rw-rw-@  1 ronaldtaylor  staff   9564939 Jan 28 11:59 CPTAC GBM Discovery Study - Proteome-log2_ratio.gct
  #
  # inputfile = '/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/CPTAC GBM Discovery Study - Proteome-log2_ratio.gct'
  # inputfile_name = "CPTAC GBM Discovery Study - Proteome-log2_ratio.gct"
  #  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - Proteome'] = 'cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe'
  # study_name     = "CPTAC GBM Discovery Study - Proteome"
  # study_id       = 'cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe'
  #
  #    outfile        = '/GDITwork/PDC_queries/quantDataMatrix_CPTAC_GBM_Discovery_Proteome_BQ_formatted.txt'
  #    outfileAllRows = '/GDITwork/PDC_queries/quantDataMatrix_CPTAC_GBM_Discovery_Proteome_BQ_formatted.txt''
  #    outfileAllRowsWithGeneInfo = '/GDITwork/PDC_queries/quantDataMatrix_CPTAC_GBM_Discovery_Proteome_BQ_formatted_all_rows_with_added_gene_info.txt'
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # BELOW CALL IS NOT CURRENTLY USED; We create a transposed version instead to upload into BiqQuery.
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_column_header_annotated_BQ_table_input_format(inputfile, inputfile_name,
  #                                                      study_name, study_id, outfile, outfileAllRows, outfileAllRowsWithGeneInfo,
  #                                                      NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS)
  #                                                                                                        )
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_CPTAC_GBM_Discovery_Proteome_BQ_formatted_2_26_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 10977
  # %%%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%%  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   9517564 Jan 28 12:02 CPTAC GBM Discovery Study - Proteome-unshared_log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/CPTAC GBM Discovery Study - Proteome-unshared_log2_ratio.gct"
  # inputfile_name = "CPTAC GBM Discovery Study - Proteome-unshared_log2_ratio.gct"
  #  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - Proteome'] = 'cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe'  
  # study_name  = "CPTAC GBM Discovery Study - Proteome"
  # study_id   = 'cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe'
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_CPTAC_GBM_Discovery_Proteome-unshared_BQ_formatted_2_28_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 10977
  #
  # %%%%%%%%%%%%%%%
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  25834920 Jan 28 11:44 CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct
  #
  # inputfile = '/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct'
  # inputfile_name = "CPTAC GBM Discovery Study - Phosphoproteome-log2_ratio.gct"
  #
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - Phosphoproteome'] = 'efc3143a-1797-11ea-9bfa-0a42f3c845fe'
  # study_name = "CPTAC GBM Discovery Study - Phosphoproteome"
  # study_id   = 'efc3143a-1797-11ea-9bfa-0a42f3c845fe'
  #
  # The first 2 columns in the phosphoprotein type of PDC source file used are: id (with a protein name), peptide
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 2
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_CPTAC_GBM_Discovery_Phosphoproteome_BQ_formatted_2_27_20.txt"  
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  #
  # NUM OF COLUMN MAPPING ERRORS                    = 0  
  # NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
  # NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = 911
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 41580
  #
  # %%%%%%%%%%%%%%








  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  25125438 Jan 28 13:39 HBV-Related Hepatocellular Carcinoma - Proteome-log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/HBV-Related Hepatocellular Carcinoma - Proteome-log2_ratio.gct"
  # inputfile_name = "HBV-Related Hepatocellular Carcinoma - Proteome-log2_ratio.gct"
  #  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['HBV-Related Hepatocellular Carcinoma - Proteome'] = 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe'
  #
  # study_name  = "HBV-Related Hepatocellular Carcinoma - Proteome"
  # study_id    = 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe'
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_HBV-Related_Hepatocellular_Carcinoma_Proteome_BQ_formatted_2_28_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF COLUMN MAPPING ERRORS                    = 0
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0  
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 10249
  
  # %%%%%%%%%%%%%%

  # -rw-rw-rw-@  1 ronaldtaylor  staff  24855358 Jan 28 13:41 HBV-Related Hepatocellular Carcinoma - Proteome-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/HBV-Related Hepatocellular Carcinoma - Proteome-unshared_log2_ratio.gct"
  # inputfile_name = "HBV-Related Hepatocellular Carcinoma - Proteome-unshared_log2_ratio.gct"
  #  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['HBV-Related Hepatocellular Carcinoma - Proteome'] = 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe'
  #
  # study_name  = "HBV-Related Hepatocellular Carcinoma - Proteome"
  # study_id    = 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe'
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_HBV-Related_Hepatocellular_Carcinoma_Proteome-unshared_BQ_formatted_2_28_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 10249
  # 
  # %%%%%%%%%%%%%%
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  84188566 Jan 28 12:38 HBV-Related Hepatocellular Carcinoma - Phosphoproteome-log2_ratio.gct  
  #
  inputfile = '/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/HBV-Related Hepatocellular Carcinoma - Phosphoproteome-log2_ratio.gct'
  inputfile_name = "HBV-Related Hepatocellular Carcinoma - Phosphoproteome-log2_ratio.gct"
  #
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['HBV-Related Hepatocellular Carcinoma - Phosphoproteome'] = '37dfda3f-1132-11ea-9bfa-0a42f3c845fe'  
  study_name = "HBV-Related Hepatocellular Carcinoma - Phosphoproteome"
  study_id   = '37dfda3f-1132-11ea-9bfa-0a42f3c845fe'  
  #
  # The first 2 columns in the phosphoprotein type of PDC source file used are: id (with a protein name), peptide
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 2
  #
  transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_HBV-Related_Hepatocellular_Carcinoma_Phosphoproteome_BQ_formatted_2_28_20.txt"  
  read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF COLUMN MAPPING ERRORS                    = 0  
  # 
  # NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
  # NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = 1088
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 49683
  # 
  # %%%%%%%%%%%%%%





  



  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  11811466 Jan 28 13:44 Pediatric Brain Cancer Pilot Study - Proteome-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/Pediatric Brain Cancer Pilot Study - Proteome-log2_ratio.gct"
  # inputfile_name = "Pediatric Brain Cancer Pilot Study - Proteome-log2_ratio.gct"
  #
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Pediatric Brain Cancer Pilot Study - Proteome'] = '58be6db8-f1f7-11e9-9a07-0a80fada099c'  
  #
  # study_name  = "Pediatric Brain Cancer Pilot Study - Proteome"
  # study_id    = '58be6db8-f1f7-11e9-9a07-0a80fada099c'  
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_Pediatric_Brain_Cancer_Pilot_Study_Proteome_BQ_formatted_2_28_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF COLUMN MAPPING ERRORS                    = 0
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 7154
  #
  # %%%%%%%%%%%%%%
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  11672766 Jan 28 13:45 Pediatric Brain Cancer Pilot Study - Proteome-unshared_log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/Pediatric Brain Cancer Pilot Study - Proteome-unshared_log2_ratio.gct"
  # inputfile_name = "Pediatric Brain Cancer Pilot Study - Proteome-unshared_log2_ratio.gct"
  #
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Pediatric Brain Cancer Pilot Study - Proteome'] = '58be6db8-f1f7-11e9-9a07-0a80fada099c'  
  #
  # study_name  = "Pediatric Brain Cancer Pilot Study - Proteome"
  # study_id    = '58be6db8-f1f7-11e9-9a07-0a80fada099c'  
  #
  # The first 4 columns in the protein type of PDC source file used are: id (with a gene name/symbol), authority, ncbi_gene_id, gene_id
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 4
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_Pediatric_Brain_Cancer_Pilot_Study_Proteome-unshared_BQ_formatted_2_28_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF COLUMN MAPPING ERRORS                    = 0
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 7154
  #  
  # %%%%%%%%%%%%%%  
  #
  # %%%%%%%%%%%%%%
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7011168 Jan 28 13:43 Pediatric Brain Cancer Pilot Study - Phosphoproteome-log2_ratio.gct
  #
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/Pediatric Brain Cancer Pilot Study - Phosphoproteome-log2_ratio.gct"
  # inputfile_name = "Pediatric Brain Cancer Pilot Study - Phosphoproteome-log2_ratio.gct"
  #
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Pediatric Brain Cancer Pilot Study - Phosphoproteome'] = '58be6cbb-f1f7-11e9-9a07-0a80fada099c'
  #
  # study_name  = "Pediatric Brain Cancer Pilot Study - Phosphoproteome"  
  # study_id   = '58be6cbb-f1f7-11e9-9a07-0a80fada099c'
  #
  # The first 2 columns in the phosphoprotein type of PDC source file used are: id (with a protein name), peptide
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 2
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_transposed_Pediatric_Brain_Cancer_Pilot_Study_Phosphoproteome_BQ_formatted_3_3_20.txt"    
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF COLUMN MAPPING ERRORS                    = 0
  # NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
  # NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = 184
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 6138
  # %%%%%%%%%%%%%%




  
# -rw-rw-rw-@  1 ronaldtaylor  staff   9095217 Jan 28 14:13 S015-1-log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff   8996841 Jan 28 14:14 S015-1-unshared_log2_ratio.gct
# -rw-rw-rw-@  1 ronaldtaylor  staff  51338297 Jan 28 15:34 S015-2-log2_ratio.gct
#
# 26
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-1'] = 'TCGA_Breast_Cancer_Proteome'
# 27
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-2'] = 'TCGA_Breast_Cancer_Phosphoproteome'
#
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Proteome'] = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'
# 26
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Phosphoproteome'] = 'b93bb1e9-57b8-11e8-b07a-00a098d917f8'
# 27

  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   9095217 Jan 28 14:13 S015-1-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S015-1-log2_ratio.gct"
  # inputfile_name = "S015-1-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-1'] = 'TCGA_Breast_Cancer_Proteome'  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Proteome'] = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'  
  #
  # study_name  = 'TCGA_Breast_Cancer_Proteome'  
  # study_id    = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_TCGA_Breast_Cancer_Proteome_3_9_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   8996841 Jan 28 14:14 S015-1-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S015-1-unshared_log2_ratio.gct"
  # inputfile_name = "S015-1-unshared_log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-1'] = 'TCGA_Breast_Cancer_Proteome'  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Proteome'] = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'  
  #
  # study_name  = 'TCGA_Breast_Cancer_Proteome'  
  # study_id    = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_TCGA_Breast_Cancer_Proteome_unshared_3_9_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  51338297 Jan 28 15:34 S015-2-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S015-2-log2_ratio.gct"
  # inputfile_name = "S015-2-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-2'] = 'TCGA_Breast_Cancer_Phosphoproteome'  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Phosphoproteome'] = 'b93bb1e9-57b8-11e8-b07a-00a098d917f8'
  #
  # study_name  = 'TCGA_Breast_Cancer_Phosphoproteome'  
  # study_id    = 'b93bb1e9-57b8-11e8-b07a-00a098d917f8'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/FIRST_BATCH/quantDataMatrix_TCGA_Breast_Cancer_Phosphoproteome_3_9_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%





  

  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-2'] = 'TCGA_Ovarian_JHU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7871618 Jan 28 15:39 S020-2-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7794952 Jan 28 15:40 S020-2-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7871618 Jan 28 15:39 S020-2-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S020-2-log2_ratio.gct"
  # inputfile_name = "S020-2-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-2'] = 'TCGA_Ovarian_JHU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'    
  #
  # study_name  = 'TCGA_Ovarian_JHU_Proteome'
  # study_id    = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'    
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_TCGA_Ovarian_JHU_Proteome_3_11_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7794952 Jan 28 15:40 S020-2-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S020-2-unshared_log2_ratio.gct"
  # inputfile_name = "S020-2-unshared_log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-2'] = 'TCGA_Ovarian_JHU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'    
  #
  # study_name  = 'TCGA_Ovarian_JHU_Proteome'
  # study_id    = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'    
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_TCGA_Ovarian_JHU_Proteome_unshared_3_11_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%

  




  
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-3'] = 'TCGA_Ovarian_PNNL_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_PNNL_Proteome'] = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'   
  # -rw-rw-rw-@  1 ronaldtaylor  staff   4767862 Jan 28 15:41 S020-3-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff   4722119 Jan 28 15:41 S020-3-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   4767862 Jan 28 15:41 S020-3-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S020-3-log2_ratio.gct"
  # inputfile_name = "S020-3-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-3'] = 'TCGA_Ovarian_PNNL_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'   
  #
  # study_name  = 'TCGA_Ovarian_PNNL_Proteome'
  # study_id    = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'   
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_TCGA_Ovarian_PNNL_Proteome_3_11_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   4722119 Jan 28 15:41 S020-3-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S020-3-unshared_log2_ratio.gct"
  # inputfile_name = "S020-3-unshared_log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-3'] = 'TCGA_Ovarian_PNNL_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'   
  #
  # study_name  = 'TCGA_Ovarian_PNNL_Proteome'
  # study_id    = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'   
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_TCGA_Ovarian_PNNL_Proteome_unshared_3_11_20.txt"  
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%






  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-4'] = 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'] = 'bb076b33-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   8137089 Jan 28 15:42 S020-4-log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   8137089 Jan 28 15:42 S020-4-log2_ratio.gct  
  #
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S020-4-log2_ratio.gct"
  # inputfile_name = "S020-4-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-4'] = 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'] = 'bb076b33-57b8-11e8-b07a-00a098d917f8'  
  #
  # study_name  = 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'
  # study_id   = 'bb076b33-57b8-11e8-b07a-00a098d917f8'  
  #
  # The first 2 columns in the phosphoprotein type of PDC source file used are: id (with a protein name), peptide
  # NUMBER_OF_STARTING_COLUMNS_TO_IGNORE_WHEN_PROCESSING_DATA_COLUMNS = 2
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive_3_11_20.txt"    
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%



  


  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-2'] = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Proteome_Qeplus'] = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  10975116 Jan 28 15:45 S037-2-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff  10876580 Jan 28 15:45 S037-2-unshared_log2_ratio.gct
  #
  # 3/12/20
  # NOTE: I had problems with these two input files
  #    S037-2-log2_ratio.gct
  #    S037-2-unshared_log2_ratio.gct  
  # for the study
  #   'Prospective_Colon_PNNL_Proteome_Qeplus'
  #
  # Apparently, one data column is blank / empty (but still remaining as a column) in its aliquot_submitter row.
  # Hence, we cannot map from the gene (protein) values to a aliquot_id nor a case_id for that particular blank value.
  #
  # I get this mapping error printed out:
  #
  # in create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list():
  # on aliquot_submitter_id entry # =  77
  # aliquot_submitter_id = ''
  # study_name           = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # MAPPING ERROR for  study_name::aliquot_submitter_id of key had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder
  #
  # NUM OF DATA COLUMNS                             = 198
  # NUM OF COLUMNS CORRECTLY MAPPED TO GDC CASE IDS = 197
  # NUM OF COLUMN MAPPING ERRORS                    = 1
  #
  #
  # So, when the values get transposed in the output file, that one column generates
  #  7,418 gene rows
  # for the one missing aliquot_submitter_id which cannot be mapped.
  #
  # See this file:
  # grep not_mapped quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt > not_mapped_quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt
  #
  # I manually added a header line to that "not_mapped" file, for viewing in Excel, so there are 7,419 rows:
  #
  # bash-3.2$ wc not_mapped_quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt
  #   7419   48554 2615168 not_mapped_quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt
  #
  # Other than than one missing (null) aliquote_submitter_id value, the output file generated from
  #  S037-2-log2_ratio.gct
  # is OK, I believe - we just cannot map back to a case_id for the data values falling under that one column.
  #
  # So the output file
  #   /GDITwork/PDC_queries/TO_BE_UPLOADED_INTO_BQ/quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt
  # is still usable.
  #
  # Likewise for
  #  S037-2-unshared_log2_ratio.gct
  # the output file is still usable - just with the gene values for that one blank aliquot_submitter_id are not mapped to a case id.
  # 
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  10975116 Jan 28 15:45 S037-2-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S037-2-log2_ratio.gct"
  # inputfile_name = "S037-2-log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-2'] = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Proteome_Qeplus'] = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'  
  #
  # study_name  = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # study_id    = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_3_11_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  10876580 Jan 28 15:45 S037-2-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S037-2-unshared_log2_ratio.gct"
  # inputfile_name = "S037-2-unshared_log2_ratio.gct"
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-2'] = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Proteome_Qeplus'] = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'  
  #
  # study_name  = 'Prospective_Colon_PNNL_Proteome_Qeplus'
  # study_id    = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Colon_PNNL_Proteome_Qeplus_unshared_3_12_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  # %%%%%%%%%%%%%%







  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-3'] = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Phosphoproteome_Lumos'] = 'bc23a4a1-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  48027935 Jan 28 15:46 S037-3-log2_ratio.gct
  #
  # NOTE: I had problems with this input file
  #    S037-3-log2_ratio.gct
  #
  # Apparently, one data column is blank / empty (but still remaining as a column) in its aliquot_submitter row.
  # Hence, we cannot map from the gene (protein) values to a aliquot_id nor a case_id for that particular blank value.
  #
  # I get this mapping error printed out:
  #
  # aliquot_submitter_id # =  87 , aliquot_submitter_id = '067db520-1b1f-4569-9414-e37a6e_D2', study_name = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
  # aliquot_submitter_id # =  88 , aliquot_submitter_id = '', study_name = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
  # in create_row_list_showing_case_gdc_id_mapped_using_aliquot_submitter_id_row_list():
  # on aliquot_submitter_id entry # =  88
  # aliquot_submitter_id = ''
  # study_name           = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
  # MAPPING ERROR for  study_name::aliquot_submitter_id of key had no mapping to an aliquot_id; 'not_mapped' is used as a placeholder
  #
  # Other than than one missing (null) aliquote_submitter_id value, the output file generated from
  #  S037-3-log2_ratio.gct
  # is OK, I believe - we just cannot map back to a case_id for the data values falling under that one column.
  #
  # Other info on processing this file:
  # NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
  # NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = 19
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 44949  
  # 
  # %%%%%%%%%%%%%%
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  48027935 Jan 28 15:46 S037-3-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S037-3-log2_ratio.gct"
  # inputfile_name = "S037-3-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
  # study_id    = 'bc23a4a1-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Colon_PNNL_Phosphoproteome_Lumos_3_12_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%




  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-1'] = 'Prospective_Ovarian_JHU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_JHU_Proteome'] = 'bc81da61-57b8-11e8-b07a-00a098d917f8'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7092218 Jan 28 15:47 S038-1-log2_ratio.gct    
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7053918 Jan 28 15:47 S038-1-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7092218 Jan 28 15:47 S038-1-log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S038-1-log2_ratio.gct"
  # inputfile_name = "S038-1-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Ovarian_JHU_Proteome'
  # study_id    = 'bc81da61-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Ovarian_JHU_Proteome_3_13_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7092218 Jan 28 15:47 S038-1-unshared_log2_ratio.gct  
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S038-1-unshared_log2_ratio.gct"
  # inputfile_name = "S038-1-unshared_log2_ratio.gct"
  #
  # study_name  = 'Prospective_Ovarian_JHU_Proteome'
  # study_id    = 'bc81da61-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Ovarian_JHU_Proteome_unshared_3_13_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%



  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-2'] = 'Prospective_Ovarian_PNNL_Proteome_Qeplus'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_PNNL_Proteome_Qeplus'] = 'bcdeeba0-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7166465 Jan 28 15:48 S038-2-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7120611 Jan 28 15:48 S038-2-unshared_log2_ratio.gct
  #  
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7166465 Jan 28 15:48 S038-2-log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S038-2-log2_ratio.gct"
  # inputfile_name = "S038-2-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Ovarian_PNNL_Proteome_Qeplus'
  # study_id    = 'bcdeeba0-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Ovarian_PNNL_Proteome_Qeplus_3_13_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # %%%%%%%%%%%%%%
  
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff   7120611 Jan 28 15:48 S038-2-unshared_log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S038-2-unshared_log2_ratio.gct"
  # inputfile_name = "S038-2-unshared_log2_ratio.gct"
  #
  # study_name  = 'Prospective_Ovarian_PNNL_Proteome_Qeplus'
  # study_id    = 'bcdeeba0-57b8-11e8-b07a-00a098d917f8'  
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/SECOND_BATCH/quantDataMatrix_Prospective_Ovarian_PNNL_Proteome_Qeplus_unshared_3_13_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # %%%%%%%%%%%%%%
  


  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-3'] = 'Prospective_Ovarian_PNNL_Phosphoproteome_Lumos'  
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_PNNL_Phosphoproteome_Lumos'] = 'bd70311c-57b8-11e8-b07a-00a098d917f8'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  26495197 Jan 28 15:48 S038-3-log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  # -rw-rw-rw-@  1 ronaldtaylor  staff  26495197 Jan 28 15:48 S038-3-log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S038-3-log2_ratio.gct"
  # inputfile_name = "S038-3-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Ovarian_PNNL_Phosphoproteome_Lumos'  
  # study_id    = 'bd70311c-57b8-11e8-b07a-00a098d917f8'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_Prospective_Ovarian_PNNL_Phosphoproteome_Lumos_3_13_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # %%%%%%%%%%%%%%






  
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S039-1'] = 'Prospective_Breast_BI_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_VU_Proteome'] = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  12245307 Jan 28 15:49 S039-1-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff  12135611 Jan 28 15:50 S039-1-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S039-1-log2_ratio.gct"
  # inputfile_name = "S039-1-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Breast_BI_Proteome'
  # study_id    = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_Prospective_Breast_BI_Proteome_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # %%%%%%%%%%%%%%
  #
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S039-1-unshared_log2_ratio.gct"
  # inputfile_name = "S039-1-unshared_log2_ratio.gct"
  #
  # study_name  = 'Prospective_Breast_BI_Proteome'
  # study_id    = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_Prospective_Breast_BI_Proteome_unshared_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%
  



  

  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S039-2'] = 'Prospective_Breast_BI_Phosphoproteome' 
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Breast_BI_Phosphoproteome'] = 'be2883cb-57b8-11e8-b07a-00a098d917f8'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  60435810 Jan 28 15:51 S039-2-log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S039-2-log2_ratio.gct"
  # inputfile_name = "S039-2-log2_ratio.gct"
  #
  # study_name  = 'Prospective_Breast_BI_Phosphoproteome' 
  # study_id    = 'be2883cb-57b8-11e8-b07a-00a098d917f8'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_Prospective_Breast_BI_Phosphoproteome_3_16_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)      
  # %%%%%%%%%%%%%%





  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-1'] = 'CPTAC UCEC Discovery Study - Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - Proteome'] = 'c935c587-0cd1-11e9-a064-0a9c39d33490'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  11547490 Jan 28 16:14 S043-1-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff  11472016 Jan 28 16:15 S043-1-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S043-1-log2_ratio.gct"
  # inputfile_name = "S043-1-log2_ratio.gct"
  #
  # study_name  = 'CPTAC UCEC Discovery Study - Proteome'
  # study_id    = 'c935c587-0cd1-11e9-a064-0a9c39d33490'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_UCEC_Discovery_Study_Proteome_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%
  #
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S043-1-unshared_log2_ratio.gct"
  # inputfile_name = "S043-1-unshared_log2_ratio.gct"
  #
  # study_name  = 'CPTAC UCEC Discovery Study - Proteome'
  # study_id    = 'c935c587-0cd1-11e9-a064-0a9c39d33490'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_UCEC_Discovery_Study_Proteome_unshared_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%




  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-2'] = 'CPTAC UCEC Discovery Study - Phosphoproteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - Phosphoproteome'] = 'cb7220f5-0cd1-11e9-a064-0a9c39d33490'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  34592169 Jan 28 16:19 S043-2-log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S043-2-log2_ratio.gct"
  # inputfile_name = "S043-2-log2_ratio.gct"
  #
  # study_name  = 'CPTAC UCEC Discovery Study - Phosphoproteome'
  # study_id    = 'cb7220f5-0cd1-11e9-a064-0a9c39d33490'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_UCEC_Discovery_Study_Phosphoproteome_3_16_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)      
  # %%%%%%%%%%%%%%




  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-1'] = 'CPTAC CCRCC Discovery Study - Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - Proteome'] = 'dbe94609-1fb3-11e9-b7f8-0a80fada099c'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  15027078 Jan 28 16:15 S044-1-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff  14906096 Jan 28 16:16 S044-1-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S044-1-log2_ratio.gct"
  # inputfile_name = "S044-1-log2_ratio.gct"
  #
  # study_name  = 'CPTAC CCRCC Discovery Study - Proteome'
  # study_id    = 'dbe94609-1fb3-11e9-b7f8-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_CCRCC_Discovery_Study_Proteome_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S044-1-unshared_log2_ratio.gct"
  # inputfile_name = "S044-1-unshared_log2_ratio.gct"
  #
  # study_name  = 'CPTAC CCRCC Discovery Study - Proteome'
  # study_id    = 'dbe94609-1fb3-11e9-b7f8-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_CCRCC_Discovery_Study_Proteome_unshared_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)  
  # %%%%%%%%%%%%%%




  

  # NOTE THE MISPELLING in "Phosphoproteme" - THAT COMES FROM THE PDC - we must keep that mispelling in order to map.  
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-2'] = 'CPTAC CCRCC Discovery Study - Phosphoproteme'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - Phosphoproteme'] = 'dd0a228f-1fb3-11e9-b7f8-0a80fada099c'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  47822683 Jan 28 16:16 S044-2-log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S044-2-log2_ratio.gct"
  # inputfile_name = "S044-2-log2_ratio.gct"
  #
  # NOTE THE MISPELLING in "Phosphoproteme" - THAT COMES FROM THE PDC - we must keep that mispelling in order to map.    
  # study_name  = 'CPTAC CCRCC Discovery Study - Phosphoproteme'
  # study_id    = 'dd0a228f-1fb3-11e9-b7f8-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_CCRCC_Discovery_Study_Phosphoproteome_3_16_20.txt"
  # read_PDC_study_phosphoproteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUMBER_OF_PROTEIN_NAMES_THAT_MATCH_ON_ROOT_BUT_NOT_REVISION_NUMBER = 0
  # NUMBER_OF_PROTEIN_NAMES_NOT_HAVING_A_GENE_ID_MATCH                 = 788
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 44312  
  #
  # %%%%%%%%%%%%%%

  




  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-1'] = 'CPTAC LUAD Discovery Study - Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - Proteome'] = 'f1c59a53-ab7c-11e9-9a07-0a80fada099c'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  18656940 Jan 28 16:17 S046-1-log2_ratio.gct
  # -rw-rw-rw-@  1 ronaldtaylor  staff  18481188 Jan 28 16:17 S046-1-unshared_log2_ratio.gct
  #
  # %%%%%%%%%%%%%%  
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S046-1-log2_ratio.gct"
  # inputfile_name = "S046-1-log2_ratio.gct"
  #
  # study_name  = 'CPTAC LUAD Discovery Study - Proteome'
  # study_id    = 'f1c59a53-ab7c-11e9-9a07-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_LUAD_Discovery_Study_Proteome_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)   # 
  # %%%%%%%%%%%%%%
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S046-1-unshared_log2_ratio.gct"
  # inputfile_name = "S046-1-unshared_log2_ratio.gct"
  #
  # study_name  = 'CPTAC LUAD Discovery Study - Proteome'
  # study_id    = 'f1c59a53-ab7c-11e9-9a07-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_LUAD_Discovery_Study_Proteome_unshared_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile) 
  # %%%%%%%%%%%%%%



  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-2'] = 'CPTAC LUAD Discovery Study - Phosphoproteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - Phosphoproteome'] = 'f1c59f58-ab7c-11e9-9a07-0a80fada099c'
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  55942048 Jan 28 16:19 S046-2-log2_ratio.gct
  #
  # inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S046-2-log2_ratio.gct"
  # inputfile_name = "S046-2-log2_ratio.gct"
  #
  # study_name  = 'CPTAC LUAD Discovery Study - Phosphoproteome'
  # study_id    = 'f1c59f58-ab7c-11e9-9a07-0a80fada099c'
  #
  # transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_CPTAC_LUAD_Discovery_Study_Phosphoproteome_3_16_20.txt"
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)
  #
  # from the run:
  # NUM OF MAPPING ERRORS (aliquot_submitter_id in header row empty or could not be mapped from) = 0
  # NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = 47956
  # %%%%%%%%%%%%%%
  



  
  


# 3/17/20
# The header value below can change. Usually it is "protein_abundance_log2ratio"
# but for these four quantDataMatrix files it needs to be different:
#
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   6153770 Jan 28 15:43 S016-1.all_peptides_ion_intensity_rounded.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'    
  # -rw-rw-rw-@  1 ronaldtaylor  staff   2330788 Jan 28 15:44 S016-1.all_peptides_log10_ion_intensity.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-1'] = 'TCGA_Ovarian_JHU_Glycoproteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Glycoproteome'] = 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff   1225683 Jan 28 15:36 S020-1.all_peptides_log2ratio_rounded.tsv
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-1'] = 'Prospective_Colon_VU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_VU_Proteome'] = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'  
  # -rw-rw-rw-@  1 ronaldtaylor  staff  28496284 Jan 28 15:43 S037-1.all_peptides_log10_ion_intensity.tsv
  # 
  # PROTEIN_ABUNDANCE_FIELD_HEADER = "protein_abundance_log2ratio"



  # 3/17/20
  # There are four non-standard data files from the PDC Github site which I have not yet parsed. Their info is shown below.
  #  They all lack aliquot_submitter_id lines that I could
  # use to map from the data file to aliquot ids and then to case ids, to link to our own data sets.
  # Before creating BQ tables for these quantDataMatrix files for these four studies, I am waiting to see if the new API will allow retrieval of data sets for
  # for these quant matrices that include aliquot_submitter_ids, as well as all the other header fields included with the other quant files, that is, for
  #  aliquot_alias
  #  morphology
  #  primary_diagnosis
  #  tumor_grade
  #  tumor_stage



  # NOT YET PARSED
  # The first two lines in the input data file:
  #
#  Gene	A6-3807-01A-22	A6-3808-01A-22	A6-3810-01A-22	AA-3518-01A-11	AA-3525-01A-12	AA-3526-01A-11	AA-3529-01A-12	AA-3531-01A-22	AA-3534-01A-22	AA-3552-01A-22	AA-3554-01A-22	AA-3558-01A-22	AA-3561-01A-22	AA-3664-01A-22	AA-3666-01A-31	AA-3672-01A-22	AA-3684-01A-31	AA-3695-01A-22	AA-3710-01A-22	AA-3715-01A-22	AA-3818-01A-22	AA-3848-01A-22	AA-3864-01A-22	AA-3986-01A-12	AA-3989-01A-22	AA-A004-01A-22	AA-A00A-01A-22	AA-A00A-01A-41	AA-A00E-01A-31	AA-A00F-01A-31	AA-A00J-01A-12	AA-A00K-01A-12	AA-A00K-01A-31	AA-A00N-01A-32	AA-A00N-01A-41	AA-A00O-01A-13	AA-A00R-01A-22	AA-A00R-01A-31	AA-A00U-01A-41	AA-A010-01A-31	AA-A017-01A-22	AA-A01C-01A-22	AA-A01D-01A-23	AA-A01F-01A-23	AA-A01I-01A-12	AA-A01K-01A-31	AA-A01P-01A-23	AA-A01R-01A-23	AA-A01S-01A-23	AA-A01T-01A-23	AA-A01V-01A-24	AA-A01X-01A-23	AA-A01Z-01A-13	AA-A022-01A-23	AA-A024-01A-32	AA-A029-01A-41	AA-A02E-01A-23	AA-A02H-01A-32	AA-A02J-01A-23	AA-A02O-01A-23	AA-A02R-01A-23	AA-A02Y-01A-31	AA-A03F-01A-41	AA-A03J-01A-23	AF-2691-01A-41	AF-2692-01A-41	AF-3400-01A-41	AF-3913-01A-12	AG-3574-01A-22	AG-3580-01A-22	AG-3584-01A-22	AG-3593-01A-22	AG-3594-01A-12	AG-4007-01A-22	AG-A002-01A-23	AG-A008-01A-23	AG-A00C-01A-23	AG-A00H-01A-22	AG-A00H-01A-31	AG-A00Y-01A-12	AG-A011-01A-32	AG-A014-01A-12	AG-A015-01A-51	AG-A016-01A-23	AG-A01J-01A-22	AG-A01L-01A-22	AG-A01N-01A-23	AG-A01W-01A-23	AG-A01Y-01A-43	AG-A020-01A-23	AG-A026-01A-71	AG-A02N-01A-31	AG-A02X-01A-32	AG-A032-01A-31	AG-A036-01A-22
# A1BG	7.56276854301652	10.0534626049255	7.9778607292647	9.59028440371816	7.68466586402586	7.7227983968709	8.16046853111904	8.19617618503997	8.42078061954856	7.9624640460579	7.95999483832842	8.49886168899288	7.94767873993694	7.55545721720465	7.56002624891289	8.1034616220947	8.17695898058691	7.8654001181793	7.17231096852195	7.00603795499732	8.25212455250564	8.32035403281767	8.3386556655787	8.08778141780954	8.08242630086077	8.31365634661803	8.04960561259497	7.99387691494121	8.16405529189345	8.44855173920158	7.65465775464952	8.25406445291434	7.95889819471077	8.94551782207784	8.43328968519502	8.49899936358015	8.27508089845686	8.47625179600703	8.40568778667278	9.18069920129603	8.4987239707479	7.96670477665787	7.99982624745441	7.1078880251828	7.62510657540347	7.93851972517649	8.30254737248749	8.16849748352303	7.90762630484327	8.45484486000851	8.47479881880063	7.20951501454263	6.99821573237096	8.04883008652835	8.11293997608408	8.0111473607758	9.5643109099606	8.18127177155946	7.74225366990659	8.20030318298158	8.5398285583779	7.94870630890485	7.76656155263753	7.42488163663107	5.25090769970086	7.99642410395509	10.1020905255118	8.10243370568134		8.44793286559218	7.07003786660775	8.00043407747932	7.77349389227097		8.40174508223706	8.08170727009735	6.92272545799326	8.00432137378264	7.78951020409025	8.06370855939142	7.65224634100332	8.17666993266815	7.81130684008134	6.27783833300205	9.01283722470517	7.76064861958136	7.63898815934368	7.86075712308154	7.78561452494682	8.44978684698577	7.85033985458348	7.63042787502502	8.0920184707528	7.21748394421391	7.43007505555194
  #
  PROTEIN_ABUNDANCE_FIELD_HEADER = "ion_intensity_rounded"  
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   6153770 Jan 28 15:43 S016-1.all_peptides_ion_intensity_rounded.tsv
  #
  # %%%%%%%%%%%%%%  
  inputfile = "/GDITwork/PDC_queries/PDC_quanDataMatrix_GCT_files/S016-1.all_peptides_ion_intensity_rounded.tsv
  inputfile_name = "S016-1.all_peptides_ion_intensity_rounded.tsv"
  #
  study_name  = 'TCGA_Colon_Cancer_Proteome'
  study_id    = 'b998098f-57b8-11e8-b07a-00a098d917f8'  
  #
  transposed_outfile = "/GDITwork/PDC_queries/OUT_TO_BE_UPLOADED_INTO_BQ/THIRD_BATCH/quantDataMatrix_TCGA_Colon_Cancer_Proteome_3_17_20.txt"
  #
  # The fn call below will FAIL, since is presumes there are muitiple header lines in the data file, including one for the aliquat_submitter_id that I 
  # use to map to aliqiuot ids and then case ids, which are completely missing.
  #
  # read_PDC_study_proteome_quantDataMatrix_spreadsheet_into_transposed_BQ_table_input_format(inputfile, inputfile_name, study_name, study_id, transposed_outfile)   #
  #
  # %%%%%%%%%%%%%%

  

  # NOT YET PARSED
  # The first two lines in the input data file:
  #
# Gene	A6-3807-01A-22	A6-3808-01A-22	A6-3810-01A-22	AA-3518-01A-11	AA-3525-01A-12	AA-3526-01A-11	AA-3529-01A-12	AA-3531-01A-22	AA-3534-01A-22	AA-3552-01A-22	AA-3554-01A-22	AA-3558-01A-22	AA-3561-01A-22	AA-3664-01A-22	AA-3666-01A-31	AA-3672-01A-22	AA-3684-01A-31	AA-3695-01A-22	AA-3710-01A-22	AA-3715-01A-22	AA-3818-01A-22	AA-3848-01A-22	AA-3864-01A-22	AA-3986-01A-12	AA-3989-01A-22	AA-A004-01A-22	AA-A00A-01A-22	AA-A00A-01A-41	AA-A00E-01A-31	AA-A00F-01A-31	AA-A00J-01A-12	AA-A00K-01A-12	AA-A00K-01A-31	AA-A00N-01A-32	AA-A00N-01A-41	AA-A00O-01A-13	AA-A00R-01A-22	AA-A00R-01A-31	AA-A00U-01A-41	AA-A010-01A-31	AA-A017-01A-22	AA-A01C-01A-22	AA-A01D-01A-23	AA-A01F-01A-23	AA-A01I-01A-12	AA-A01K-01A-31	AA-A01P-01A-23	AA-A01R-01A-23	AA-A01S-01A-23	AA-A01T-01A-23	AA-A01V-01A-24	AA-A01X-01A-23	AA-A01Z-01A-13	AA-A022-01A-23	AA-A024-01A-32	AA-A029-01A-41	AA-A02E-01A-23	AA-A02H-01A-32	AA-A02J-01A-23	AA-A02O-01A-23	AA-A02R-01A-23	AA-A02Y-01A-31	AA-A03F-01A-41	AA-A03J-01A-23	AF-2691-01A-41	AF-2692-01A-41	AF-3400-01A-41	AF-3913-01A-12	AG-3574-01A-22	AG-3580-01A-22	AG-3584-01A-22	AG-3593-01A-22	AG-3594-01A-12	AG-4007-01A-22	AG-A002-01A-23	AG-A008-01A-23	AG-A00C-01A-23	AG-A00H-01A-22	AG-A00H-01A-31	AG-A00Y-01A-12	AG-A011-01A-32	AG-A014-01A-12	AG-A015-01A-51	AG-A016-01A-23	AG-A01J-01A-22	AG-A01L-01A-22	AG-A01N-01A-23	AG-A01W-01A-23	AG-A01Y-01A-43	AG-A020-01A-23	AG-A026-01A-71	AG-A02N-01A-31	AG-A02X-01A-32	AG-A032-01A-31	AG-A036-01A-22
# A1BG	7.563	10.054	7.978	9.590	7.685	7.723	8.160	8.196	8.421	7.962	7.960	8.499	7.948	7.555	7.560	8.104	8.177	7.865	7.172	7.006	8.252	8.320	8.339	8.088	8.082	8.314	8.050	7.994	8.164	8.449	7.655	8.254	7.959	8.946	8.433	8.499	8.275	8.476	8.406	9.181	nnnnn8.499	7.967	8.000	7.108	7.625	7.939	8.303	8.168	7.908	8.455	8.475	7.210	6.998	8.049	8.113	8.011	9.564	8.181	7.742	8.200	8.540	7.949	7.767	7.425	5.251	7.996	10.102	8.102		8.448	7.070	8.001	7.773		8.402	8.082	6.923	8.004	7.790	8.064	7.652	8.177	7.811	6.278	9.013	7.761	7.639	7.861	7.786	8.450	7.850	7.630	8.092	7.218	7.430	
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'    
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   2330788 Jan 28 15:44 S016-1.all_peptides_log10_ion_intensity.tsv



  

  
  # NOT YET PARSED
  # The first two lines in the input data file:
  #
# Glycosite	09-1664-01A	09-2056-01B	13-1404-01A	13-1409-01A	13-1410-01A	13-1482-01A	13-1483-01A	13-1485-01A	13-1487-01A	13-1488-01A	13-1489-01A	13-1492-01A	13-1494-01A	13-1497-01A	13-1500-01A	13-1501-01A	13-1504-01A	13-1507-01A	13-1511-01A	13-2059-01A	13-2060-01A	13-2061-01A	13-2065-01A	13-2066-01A	23-1121-01A	23-1122-01A	24-1103-01A	24-1416-01A	24-1422-01A	24-1430-01A	24-1431-01A	24-1435-01A	24-1436-01A	24-1464-01A	24-1474-01A	24-1544-01A	24-1548-01A	24-1553-01A	24-1555-01A	24-1557-01A	24-1558-01A	24-1562-01A	24-1565-01A	24-1920-01A	24-1927-01A	24-2024-01A	24-2030-01A	24-2033-01A	24-2261-01A	24-2289-01A	24-2290-01A	24-2298-01A	25-1312-01A	25-1313-01A	25-1318-01A	25-1322-01A	25-1323-01A	25-1329-01A	25-1623-01A	25-1628-01A	25-1631-01A	25-2396-01A	25-2397-01A	25-2399-01A	25-2404-01A	25-2409-01A	29-1688-01A	29-1690-01A	29-1693-01A	29-1696-01A	29-1697-01A	29-1698-01A	29-1702-01A	29-1705-01A	29-1710-01A	29-1711-01A	29-1762-01A	29-1763-01A	29-1766-01A	29-1768-01A	29-1769-01A	29-1770-01A	29-1771-01A	29-1785-01A	29-2427-01A	29-2431-01A	30-1887-01A	30-1891-01A	36-1569-01A	36-1570-01A	36-1571-01A	36-1577-01A	36-1578-01A	36-1580-01A	36-2530-01A	36-2532-01A	36-2534-01A	36-2537-01A	36-2542-01A	36-2544-01A	36-2545-01A	36-2547-01A	36-2549-01A	59-2348-01A	59-2351-01A	59-2352-01A	59-2372-01A	61-1724-01A	61-1727-01A	61-1741-01A	61-1907-01A	61-1910-01A	61-1911-01A	61-1914-01A	61-1915-01A	61-1918-01A	61-1995-01A	61-2008-01A	61-2087-01A	61-2088-01A	61-2094-01A	61-2613-01A	OVARIAN-CONTROL.1	OVARIAN-CONTROL.10	OVARIAN-CONTROL.2	OVARIAN-CONTROL.3	OVARIAN-CONTROL.4	OVARIAN-CONTROL.5	OVARIAN-CONTROL.6	OVARIAN-CONTROL.7	OVARIAN-CONTROL.8	OVARIAN-CONTROL.9
# NP_000005.2:n55				-0.697			-0.175	-0.178	-0.794	-0.014		0.054	-0.064		0.424	-0.238		0.063			-0.195	0.398	0.042		1.162	0.412		-0.515	-0.102		-0.264	0.838		0.836	0.283			-0.381	-1.020		-0.221	0.058	0.857	0.361	-0.459	-0.384	0.250	-0.647	-0.022					0.193		-0.176				-0.258	1.060	0.594	-1.175			-0.350	0.185	0.386	-0.301		-1.010		0.503	-0.367	0.396	-0.137		0.376	0.379	1.559	0.501				0.556		0.750	-0.351	0.757	-0.095	0.245	0.154	-0.377		-0.175		-0.400	1.597	-0.422	-0.264	0.024	-0.423	0.129												0.540	-0.456		-0.146				0.597		0.040	-0.176	-0.426	-0.104	-0.169	-0.277	0.023		-0.882	
NP_000005.2:n396								-0.771	-0.965	0.763			0.092			-0.312		0.015	0.910			0.459				1.098	0.011				0.580		-0.014	1.570	-0.499			-0.536	0.339			-0.691		0.032		-1.164				0.440						-0.220				-1.586							0.690		-0.212														0.225					-0.792		0.894								2.583																				-0.020			
  #
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-1'] = 'TCGA_Ovarian_JHU_Glycoproteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Glycoproteome'] = 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff   1225683 Jan 28 15:36 S020-1.all_peptides_log2ratio_rounded.tsv




  # NOT YET PARSED
  # The first three lines in the input data file:
  #
# Gene	0065bfd9-e1b9-43a8-9379-9658ba_D2	01d1ffca-4235-4dfe-bf62-56f71d_D2	0630ecb0-b664-4e75-bb3c-fb62ee_D2	0f757044-9bc8-4ed4-90f3-e0a49a_D2	1312a0e0-5025-433e-9670-0595bb_D2	1425a1a1-c835-47a2-a9b9-4db5d0_D2	1563ef63-d2e6-406f-adc1-255949_D2	159c49fd-40ff-4616-b6b6-be21a6_D2	17cd2121-d347-431c-9cc7-fbab14_D2	1e451f20-718d-458e-abeb-93f1d8_D2	1ea1d7ee-e382-4a68-8e7c-74e833_D2	21c6f56a-8881-421b-81fa-c354cb_D2	21cab01c-e968-42cc-9651-1e53c0_D2	2d266d2d-4bb3-436c-a3e7-06acb2_D2	2f8b02e4-1848-4abc-a95e-dd9eb5_D2	3ca0ab23-70ae-4083-bd7d-9f71d8_D2	3d797911-d476-436f-9cce-33e20c_D2	416bb805-e4cd-4ca6-8fdd-1eec2d_D2	42216d99-e8ff-43c9-b6db-28c3a6_D2	43192517-085e-450e-9481-0a6713_D2	43e0432b-343d-4e12-817f-21281c_D2	47b9deba-dea0-4e0c-8b0a-4864c3_D2	4976cbf9-cad4-48ce-b234-622c70_D2	4b263734-430f-41dc-a296-8691f0_D2	4b68d71e-48b4-45ba-a041-9d6aa0_D2	4cdeecb5-19fb-493c-91f5-eb0efc_D2	4d86cf19-8d5b-46b5-ab4c-ace0b2_D2	52d61ab1-5781-4b47-8e88-37c2ea_D2	5557e478-3e6f-475b-8acc-1b4159_D2	573048dd-2502-40e0-8e8c-c41bb8_D2	58a92726-4d1f-4e44-8938-52b97d_D2	591fb460-703a-437d-8d9a-ff2a35_D2	59891744-2db3-4541-a86a-7f911f_D2	5a007558-764c-463b-a788-fdbccb_D2	5a85e6e7-35ab-4ca9-ab62-707e77_D2	5da00abd-5fa7-4deb-95f1-7982e0_D2	5f823547-ee16-45b7-a6a0-ef25d6_D2	64b606d7-d40f-41ae-b220-a66324_D2	66b829db-1cdd-460d-b2f3-0e9ca9_D2	695b0b00-a86b-43c0-a6c8-50a840_D2	6f5853a3-1d47-4a54-94dc-970c1c_D2	722378ea-d1a7-4cfc-aca5-ba45f9_D2	743c74e4-cd76-4f38-8cb4-3c9107_D2	74f7759a-279c-4098-b080-358900_D2	760f15d2-444c-4deb-b133-62f3ca_D2	7782134d-02db-44ee-adfa-fc1372_D2	77c493b1-34ba-4efc-8def-987116_D2	799893aa-d523-4a08-9ea7-611cca_D2	7a0266b4-b07c-41dc-8ddd-350e1b_D2	7cc7f69f-99f5-4b23-a9e0-ba0dc8_D2	7e3d5ae0-3dcc-4207-8dd2-59ab4b_D2	803736b3-9ca9-4f9f-bdfe-8783e3_D2	8495cc54-eb88-40ab-b03f-363edf_D2	85930155-5a70-4aab-97e3-e9c5a9_D2	87408d9e-4029-4e8f-b14c-8efc04_D2	8960a27e-69d8-4ec8-a795-c6cf8a_D2	8af80ae9-2088-4124-91fb-8d6c17_D2	8c73f3a5-fea8-4942-90c0-966f66_D2	92c1ebb5-ac1d-4bf5-a14f-516e1d_D2	9452572f-e3ec-4063-a82e-43ac90_D2	97423edb-6f19-4b41-a7cb-af1635_D2	98742d96-cc2f-43f0-9de2-bc7afe_D2	9899e1ad-62e1-4cc2-91fc-49b85c_D2	9fb0c084-16fd-4d54-ab60-d94303_D2	a2f03d85-5f1a-4b3e-9dbf-74e67e_D2	a467b905-fc0b-427b-8753-6d38dd_D2	a8955d38-7a72-4f9d-9c70-5c0f08_D2	aa0f5b40-3290-4255-8f4d-f45d4e_D2	b2615b47-5ac2-4775-9205-50990d_D2	b4dd4a04-5eda-43dd-8298-215b07_D2	b873f54a-5a86-4dca-90f9-715820_D2	b913f361-c5c2-4e49-91ca-719bd8_D2	bc574f73-89ba-44d7-992e-82622c_D2	c2c53076-b5db-4b7d-983c-a35740_D2	c2db7236-990c-42b6-9ffb-2ad150_D2	c6ae57a7-65a0-438e-bae2-97b1c8_D2	c8694e31-2c8f-4eb6-a7ef-a137d9_D2	c952715e-35ca-4e18-ad6f-dd4e39_D2	cf795cd2-317b-4897-a35e-390001_D2	d0511ea6-43c3-4c61-b08e-cf49b7_D2	d068de5f-8394-46bc-b03a-c6dc9c_D2	d3078fa8-0692-411f-8441-0c7c48_D2	d7608884-a0d6-4e9e-aac7-6cd813_D2	d8345fa1-f692-4025-bc82-bfaec6_D2	d91f20f5-f59f-421d-9164-e5fd19_D2	db3f5ad9-d14d-4126-a3e7-d80faa_D2	dffcac99-9eac-4717-aeb6-f4a2e7_D2	e0068ac5-f557-443a-bd4b-bdfcef_D2	e295a7d1-a0d0-4b0b-bdc2-729a0d_D2	e515eb5c-8005-416a-9b42-59de9d_D2	e8faa3a7-9fc7-428e-9334-a0afd0_D2	ec9afcc4-ccf7-4226-b0e8-d6897d_D2	ecf1ca92-c8b7-470f-baf4-81cf73_D2	f0bb6ff4-7cb8-427f-84bf-7f51d7_D2	f4bccfe4-98d0-466b-a6d7-44341b_D2	f69deaeb-6b6f-4c61-8900-fd0f26_D2	f6a7f764-2495-4a1d-89a2-e12f51_D2	f9d6fb1b-988e-4d28-9272-cc672f_D2	fd4edc52-09ef-4b01-be7c-f23f05_D2	fe4b3b1c-9f7f-4109-8a76-ca1d6f_D2
# A0A024QZ42	10.038	9.638	9.768	9.906	9.644	9.567	8.945	9.593	9.600	9.262	9.070	9.754	9.322	9.335	9.336	9.267	9.242	9.890	9.929	8.949	9.907	9.345	10.008	9.101	9.522	9.447	7.833	9.511	8.641	10.013	9.984	9.631	8.857	10.003	9.559	9.200	9.086	8.850	8.769	9.355	9.804	10.238	9.060	9.998	9.333	9.432	9.156	9.526	9.937	10.170	9.271	9.929	9.907	9.537	9.643	9.704	9.298	9.653	9.639	9.890	9.395	9.378	9.693	9.913	9.925	9.817	9.624	8.857	10.002	9.913	9.744	9.336	9.884	9.645	9.716	9.054	8.859	9.559	9.633	9.656	9.578	9.092	9.675	9.890	8.231	9.567	9.633	9.839	9.251	10.169	9.817	9.940	9.730	10.102	9.660	9.787	9.912	9.605	9.428	9.188	
# A0A024QZB8	8.028																								7.726					7.799				7.783																													8.131	8.646	7.751		7.681														7.717		8.178				7.658						8.303	6.729		8.753					
  #
  # GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-1'] = 'Prospective_Colon_VU_Proteome'
  # GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_VU_Proteome'] = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'  
  #
  # -rw-rw-rw-@  1 ronaldtaylor  staff  28496284 Jan 28 15:43 S037-1.all_peptides_log10_ion_intensity.tsv
  #
  # S037-1.all_peptides_log10_ion_intensity.tsv



  
  # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
  
  # print("")                        
  # print("output file               = " + outfile )
  # print("output file with all starting experimental desc rows                                   = " + outfileAllRows )
  # print("output file with all starting experimental desc rows and with 9 added gene info fields = " + outfileAllRowsWithGeneInfo )

  print("")
  print("input file            = " + inputfile )    
  print("transposed_outfile    = " + transposed_outfile )      
      
  print("----- ")
  print("")                      
  print("NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE read  = " + str(NUMBER_OF_DATA_ROWS_IN_SOURCE_FILE) )
  print("")                      

  # %%%%%%%%%
  
  print("Done.")

# %%%%%%%%%%%%%%%%%%%%%%%%%%

if __name__ == "__main__":
    main()


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# from
#  /GDITWork/PDC_queries/AA_Tues_2_11_20_LATEST_dictionaries_for_pdc_proteomics_data.py


# GLOBAL_PROGRAM_ID_LIST = ['10251935-5540-11e8-b664-00a098d917f8', '1a4a4346-f231-11e8-a44b-0a9c39d33490', 'c3408a52-f1e8-11e9-9a07-0a80fada099c', 'c3408b38-f1e8-11e9-9a07-0a80fada099c']

# GLOBAL_PROGRAM_NAME_LIST = ['Clinical Proteomic Tumor Analysis Consortium', 'Quantitative digital maps of tissue biopsies', 'International Cancer Proteogenome Consortium', 'Pediatric Brain Tumor Atlas - CBTTC']

# GLOBAL_PROJECT_ID_LIST = ['267d6671-0e78-11e9-a064-0a9c39d33490', '48653303-5546-11e8-b664-00a098d917f8', '48af5040-5546-11e8-b664-00a098d917f8', 'd282b2d7-f238-11e8-a44b-0a9c39d33490', '095cf1fe-0f93-11ea-9bfa-0a42f3c845fe', 'edb4ca56-f1e8-11e9-9a07-0a80fada099c']

# GLOBAL_PROJECT_NAME_LIST = ['CPTAC3-Discovery', 'CPTAC2 Confirmatory', 'CPTAC2 Retrospective', 'Quantitative digital maps of tissue biopsies', 'Integrated Proteogenomic Characterization of HBV-related Hepatocellular carcinoma', 'Proteogenomic Analysis of Pediatric Brain Cancer Tumors Pilot Study']

# GLOBAL_STUDY_ID_LIST = ['cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe', 'c935c587-0cd1-11e9-a064-0a9c39d33490', 'cb7220f5-0cd1-11e9-a064-0a9c39d33490', 'dbe94609-1fb3-11e9-b7f8-0a80fada099c', 'dd0a228f-1fb3-11e9-b7f8-0a80fada099c', 'de58670a-06ad-11ea-8c2e-0a7b46c3918d', 'de58a7ea-06ad-11ea-8c2e-0a7b46c3918d', 'de58ab8e-06ad-11ea-8c2e-0a7b46c3918d', 'de58af5a-06ad-11ea-8c2e-0a7b46c3918d', 'de58b336-06ad-11ea-8c2e-0a7b46c3918d', 'de58b64c-06ad-11ea-8c2e-0a7b46c3918d', 'efc3143a-1797-11ea-9bfa-0a42f3c845fe', 'f1c59a53-ab7c-11e9-9a07-0a80fada099c', 'f1c59f58-ab7c-11e9-9a07-0a80fada099c', 'a6d1361f-1797-11ea-9bfa-0a42f3c845fe', '0ea91a54-1798-11ea-9bfa-0a42f3c845fe', 'bb67ec40-57b8-11e8-b07a-00a098d917f8', 'bbc1441e-57b8-11e8-b07a-00a098d917f8', 'bc23a4a1-57b8-11e8-b07a-00a098d917f8', 'bc81da61-57b8-11e8-b07a-00a098d917f8', 'bcdeeba0-57b8-11e8-b07a-00a098d917f8', 'bd70311c-57b8-11e8-b07a-00a098d917f8', 'bdcd3802-57b8-11e8-b07a-00a098d917f8', 'be2883cb-57b8-11e8-b07a-00a098d917f8', 'b8da9eeb-57b8-11e8-b07a-00a098d917f8', 'b93bb1e9-57b8-11e8-b07a-00a098d917f8', 'b998098f-57b8-11e8-b07a-00a098d917f8', 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8', 'ba4e17a5-57b8-11e8-b07a-00a098d917f8', 'baa8ae46-57b8-11e8-b07a-00a098d917f8', 'bb076b33-57b8-11e8-b07a-00a098d917f8', 'ad18f195-f3c0-11e8-a44b-0a9c39d33490', 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe', '37dfda3f-1132-11ea-9bfa-0a42f3c845fe', '58be6db8-f1f7-11e9-9a07-0a80fada099c', '58be6cbb-f1f7-11e9-9a07-0a80fada099c']

# GLOBAL_STUDY_NAME_LIST = ['CPTAC GBM Discovery Study - Proteome', 'CPTAC UCEC Discovery Study - Proteome', 'CPTAC UCEC Discovery Study - Phosphoproteome', 'CPTAC CCRCC Discovery Study - Proteome', 'CPTAC CCRCC Discovery Study - Phosphoproteme', 'CPTAC UCEC Discovery Study - CompRef Proteome ', 'CPTAC UCEC Discovery Study - CompRef Phosphoproteome', 'CPTAC CCRCC Discovery Study - CompRef Proteome', 'CPTAC CCRCC Discovery Study - CompRef Phosphoproteme', 'CPTAC LUAD Discovery Study - CompRef Proteome', 'CPTAC LUAD Discovery Study - CompRef Phosphoproteome', 'CPTAC GBM Discovery Study - Phosphoproteome', 'CPTAC LUAD Discovery Study - Proteome', 'CPTAC LUAD Discovery Study - Phosphoproteome', 'CPTAC GBM Discovery Study - CompRef Proteome', 'CPTAC GBM Discovery Study - CompRef Phosphoproteome', 'Prospective_Colon_VU_Proteome', 'Prospective_Colon_PNNL_Proteome_Qeplus', 'Prospective_Colon_PNNL_Phosphoproteome_Lumos', 'Prospective_Ovarian_JHU_Proteome', 'Prospective_Ovarian_PNNL_Proteome_Qeplus', 'Prospective_Ovarian_PNNL_Phosphoproteome_Lumos', 'Prospective_Breast_BI_Proteome', 'Prospective_Breast_BI_Phosphoproteome', 'TCGA_Breast_Cancer_Proteome', 'TCGA_Breast_Cancer_Phosphoproteome', 'TCGA_Colon_Cancer_Proteome', 'TCGA_Ovarian_JHU_Glycoproteome', 'TCGA_Ovarian_JHU_Proteome', 'TCGA_Ovarian_PNNL_Proteome', 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive', 'PCT_SWATH_Kidney', 'HBV-Related Hepatocellular Carcinoma - Proteome', 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', 'Pediatric Brain Cancer Pilot Study - Proteome', 'Pediatric Brain Cancer Pilot Study - Phosphoproteome']

# GLOBAL_STUDY_SUBMITTER_ID_LIST = ['CPTAC GBM Discovery Study - Proteome', 'S043-1', 'S043-2', 'S044-1', 'S044-2', 'S043-1-CompRef', 'S043-2-CompRef', 'S044-1-CompRef', 'S044-2-CompRef', 'S046-1-CompRef', 'S046-2-CompRef', 'CPTAC GBM Discovery Study - Phosphoproteome', 'S046-1', 'S046-2', 'CPTAC GBM Discovery Study - CompRef Proteome', 'CPTAC GBM Discovery Study - CompRef Phosphoproteome', 'S037-1', 'S037-2', 'S037-3', 'S038-1', 'S038-2', 'S038-3', 'S039-1', 'S039-2', 'S015-1', 'S015-2', 'S016-1', 'S020-1', 'S020-2', 'S020-3', 'S020-4', 'ST25730263', 'HBV-Related Hepatocellular Carcinoma - Proteome', 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', 'Pediatric Brain Cancer Pilot Study - Proteome', 'Pediatric Brain Cancer Pilot Study - Phosphoproteome']

# GLOBAL_STUDY_SUBMITTER_NAME_LIST = ['CPTAC GBM Discovery Study - Proteome', 'CPTAC UCEC Discovery Study - Proteome', 'CPTAC UCEC Discovery Study - Phosphoproteome', 'CPTAC CCRCC Discovery Study - Proteome', 'CPTAC CCRCC Discovery Study - Phosphoproteme', 'CPTAC UCEC Discovery Study - CompRef Proteome ', 'CPTAC UCEC Discovery Study - CompRef Phosphoproteome', 'CPTAC CCRCC Discovery Study - CompRef Proteome', 'CPTAC CCRCC Discovery Study - CompRef Phosphoproteme', 'CPTAC LUAD Discovery Study - CompRef Proteome', 'CPTAC LUAD Discovery Study - CompRef Phosphoproteome', 'CPTAC GBM Discovery Study - Phosphoproteome', 'CPTAC LUAD Discovery Study - Proteome', 'CPTAC LUAD Discovery Study - Phosphoproteome', 'CPTAC GBM Discovery Study - CompRef Proteome', 'CPTAC GBM Discovery Study - CompRef Phosphoproteome', 'Prospective_Colon_VU_Proteome', 'Prospective_Colon_PNNL_Proteome_Qeplus', 'Prospective_Colon_PNNL_Phosphoproteome_Lumos', 'Prospective_Ovarian_JHU_Proteome', 'Prospective_Ovarian_PNNL_Proteome_Qeplus', 'Prospective_Ovarian_PNNL_Phosphoproteome_Lumos', 'Prospective_Breast_BI_Proteome', 'Prospective_Breast_BI_Phosphoproteome', 'TCGA_Breast_Cancer_Proteome', 'TCGA_Breast_Cancer_Phosphoproteome', 'TCGA_Colon_Cancer_Proteome', 'TCGA_Ovarian_JHU_Glycoproteome', 'TCGA_Ovarian_JHU_Proteome', 'TCGA_Ovarian_PNNL_Proteome', 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive', 'PCT_SWATH_Kidney', 'HBV-Related Hepatocellular Carcinoma - Proteome', 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome', 'Pediatric Brain Cancer Pilot Study - Proteome', 'Pediatric Brain Cancer Pilot Study - Phosphoproteome']


# 1
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - Proteome'] = 'cfe9f4a2-1797-11ea-9bfa-0a42f3c845fe'
# 2
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - Proteome'] = 'c935c587-0cd1-11e9-a064-0a9c39d33490'
# 3
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - Phosphoproteome'] = 'cb7220f5-0cd1-11e9-a064-0a9c39d33490'
# 4
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - Proteome'] = 'dbe94609-1fb3-11e9-b7f8-0a80fada099c'
# 5
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - Phosphoproteme'] = 'dd0a228f-1fb3-11e9-b7f8-0a80fada099c'
# 6
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - CompRef Proteome '] = 'de58670a-06ad-11ea-8c2e-0a7b46c3918d'
# 7
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC UCEC Discovery Study - CompRef Phosphoproteome'] = 'de58a7ea-06ad-11ea-8c2e-0a7b46c3918d'
# 8
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - CompRef Proteome'] = 'de58ab8e-06ad-11ea-8c2e-0a7b46c3918d'
# 9
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC CCRCC Discovery Study - CompRef Phosphoproteme'] = 'de58af5a-06ad-11ea-8c2e-0a7b46c3918d'
# 10
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - CompRef Proteome'] = 'de58b336-06ad-11ea-8c2e-0a7b46c3918d'
# 11
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - CompRef Phosphoproteome'] = 'de58b64c-06ad-11ea-8c2e-0a7b46c3918d'
# 12
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - Phosphoproteome'] = 'efc3143a-1797-11ea-9bfa-0a42f3c845fe'
# 13
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - Proteome'] = 'f1c59a53-ab7c-11e9-9a07-0a80fada099c'
# 14
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC LUAD Discovery Study - Phosphoproteome'] = 'f1c59f58-ab7c-11e9-9a07-0a80fada099c'
# 15
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - CompRef Proteome'] = 'a6d1361f-1797-11ea-9bfa-0a42f3c845fe'
# 16
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['CPTAC GBM Discovery Study - CompRef Phosphoproteome'] = '0ea91a54-1798-11ea-9bfa-0a42f3c845fe'
# 17
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_VU_Proteome'] = 'bb67ec40-57b8-11e8-b07a-00a098d917f8'
# 18
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Proteome_Qeplus'] = 'bbc1441e-57b8-11e8-b07a-00a098d917f8'
# 19
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Colon_PNNL_Phosphoproteome_Lumos'] = 'bc23a4a1-57b8-11e8-b07a-00a098d917f8'
# 20
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_JHU_Proteome'] = 'bc81da61-57b8-11e8-b07a-00a098d917f8'
# 21
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_PNNL_Proteome_Qeplus'] = 'bcdeeba0-57b8-11e8-b07a-00a098d917f8'
# 22
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Ovarian_PNNL_Phosphoproteome_Lumos'] = 'bd70311c-57b8-11e8-b07a-00a098d917f8'
# 23
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Breast_BI_Proteome'] = 'bdcd3802-57b8-11e8-b07a-00a098d917f8'
# 24
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Prospective_Breast_BI_Phosphoproteome'] = 'be2883cb-57b8-11e8-b07a-00a098d917f8'
# 25
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Proteome'] = 'b8da9eeb-57b8-11e8-b07a-00a098d917f8'
# 26
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Breast_Cancer_Phosphoproteome'] = 'b93bb1e9-57b8-11e8-b07a-00a098d917f8'
# 27
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Colon_Cancer_Proteome'] = 'b998098f-57b8-11e8-b07a-00a098d917f8'
# 28
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Glycoproteome'] = 'b9f2ccc5-57b8-11e8-b07a-00a098d917f8'
# 29
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_JHU_Proteome'] = 'ba4e17a5-57b8-11e8-b07a-00a098d917f8'
# 30
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_PNNL_Proteome'] = 'baa8ae46-57b8-11e8-b07a-00a098d917f8'
# 31
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'] = 'bb076b33-57b8-11e8-b07a-00a098d917f8'
# 32
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['PCT_SWATH_Kidney'] = 'ad18f195-f3c0-11e8-a44b-0a9c39d33490'
# 33
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['HBV-Related Hepatocellular Carcinoma - Proteome'] = 'f14e4c61-106f-11ea-9bfa-0a42f3c845fe'
# 34
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['HBV-Related Hepatocellular Carcinoma - Phosphoproteome'] = '37dfda3f-1132-11ea-9bfa-0a42f3c845fe'
# 35
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Pediatric Brain Cancer Pilot Study - Proteome'] = '58be6db8-f1f7-11e9-9a07-0a80fada099c'
# 36
# GLOBAL_STUDY_NAME_TO_STUDY_ID_DICT['Pediatric Brain Cancer Pilot Study - Phosphoproteome'] = '58be6cbb-f1f7-11e9-9a07-0a80fada099c'




# 1
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['CPTAC GBM Discovery Study - Proteome'] = 'CPTAC GBM Discovery Study - Proteome'
# 2
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-1'] = 'CPTAC UCEC Discovery Study - Proteome'
# 3
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-2'] = 'CPTAC UCEC Discovery Study - Phosphoproteome'
# 4
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-1'] = 'CPTAC CCRCC Discovery Study - Proteome'
# 5
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-2'] = 'CPTAC CCRCC Discovery Study - Phosphoproteme'
# 6
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-1-CompRef'] = 'CPTAC UCEC Discovery Study - CompRef Proteome '
# 7
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S043-2-CompRef'] = 'CPTAC UCEC Discovery Study - CompRef Phosphoproteome'
# 8
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-1-CompRef'] = 'CPTAC CCRCC Discovery Study - CompRef Proteome'
# 9
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S044-2-CompRef'] = 'CPTAC CCRCC Discovery Study - CompRef Phosphoproteme'
# 10
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-1-CompRef'] = 'CPTAC LUAD Discovery Study - CompRef Proteome'
# 11
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-2-CompRef'] = 'CPTAC LUAD Discovery Study - CompRef Phosphoproteome'
# 12
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['CPTAC GBM Discovery Study - Phosphoproteome'] = 'CPTAC GBM Discovery Study - Phosphoproteome'
# 13
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-1'] = 'CPTAC LUAD Discovery Study - Proteome'
# 14
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S046-2'] = 'CPTAC LUAD Discovery Study - Phosphoproteome'
# 15
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['CPTAC CCRCC Discovery Study - DIA Proteome'] = 'CPTAC CCRCC Discovery Study - DIA Proteome'
# 16
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['CPTAC GBM Discovery Study - CompRef Proteome'] = 'CPTAC GBM Discovery Study - CompRef Proteome'
# 17
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['CPTAC GBM Discovery Study - CompRef Phosphoproteome'] = 'CPTAC GBM Discovery Study - CompRef Phosphoproteome'
# 18
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-1'] = 'Prospective_Colon_VU_Proteome'
# 19
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-2'] = 'Prospective_Colon_PNNL_Proteome_Qeplus'
# 20
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S037-3'] = 'Prospective_Colon_PNNL_Phosphoproteome_Lumos'
# 21
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-1'] = 'Prospective_Ovarian_JHU_Proteome'
# 22
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-2'] = 'Prospective_Ovarian_PNNL_Proteome_Qeplus'
# 23
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S038-3'] = 'Prospective_Ovarian_PNNL_Phosphoproteome_Lumos'
# 24
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S039-1'] = 'Prospective_Breast_BI_Proteome'
# 25
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S039-2'] = 'Prospective_Breast_BI_Phosphoproteome'
# 26
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-1'] = 'TCGA_Breast_Cancer_Proteome'
# 27
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S015-2'] = 'TCGA_Breast_Cancer_Phosphoproteome'
# 28
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S016-1'] = 'TCGA_Colon_Cancer_Proteome'
# 29
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-1'] = 'TCGA_Ovarian_JHU_Glycoproteome'
# 30
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-2'] = 'TCGA_Ovarian_JHU_Proteome'
# 31
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-3'] = 'TCGA_Ovarian_PNNL_Proteome'
# 32
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['S020-4'] = 'TCGA_Ovarian_PNNL_Phosphoproteome_Velos_Qexatvive'
# 33
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['ST25730263'] = 'PCT_SWATH_Kidney'
# 34
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['HBV-Related Hepatocellular Carcinoma - Proteome'] = 'HBV-Related Hepatocellular Carcinoma - Proteome'
# 35
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['HBV-Related Hepatocellular Carcinoma - Phosphoproteome'] = 'HBV-Related Hepatocellular Carcinoma - Phosphoproteome'
# 36
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['Pediatric Brain Cancer Pilot Study - Proteome'] = 'Pediatric Brain Cancer Pilot Study - Proteome'
# 37
# GLOBAL_STUDY_SUBMITTER_ID_TO_STUDY_NAME_DICT['Pediatric Brain Cancer Pilot Study - Phosphoproteome'] = 'Pediatric Brain Cancer Pilot Study - Phosphoproteome'

