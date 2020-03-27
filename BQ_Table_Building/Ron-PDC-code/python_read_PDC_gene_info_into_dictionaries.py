# invocation:
#  python python_read_PDC_gene_info_into_dictionaries.py

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

# The input file contains a spreadsheet of Proteomic Data Commons gene info:

# info on the input file:
#
# (base) bash-3.2$ pwd
# /GDITWork/PDC_queries
# (base) bash-3.2$ 
# (base) bash-3.2$ ls -l -h pdc_genes_to_protein_mapping_file_as_of_1_22_20.csv
# -rw-rw-rw-@ 1 ronaldtaylor  staff   4.2M Jan 23 11:02 pdc_genes_to_protein_mapping_file_as_of_1_22_20.csv
# (base) bash-3.2$ 
# (base) bash-3.2$ head pdc_genes_to_protein_mapping_file_as_of_1_22_20.csv
#
# gene_name,ncbi_gene_id,authority,description,organism,chromosome,locus,proteins,assays,access,cud_label,updated,gene_uuid
#
# IL17RB,55540,HGNC:18015,interleukin 17 receptor B,Homo sapiens,3,3p21.1,C9IZN0;NP_061195.2;Q9NRM6;Q9NRM6-2;XP_005265367.1;XP_005265368.1;XP_005265369.1;XP_011532243.1;XP_016862293.1;XP_016862294.1;XP_016862295.1;XP_016862296.1;XP_024309402.1;XP_024309403.1;XP_024309404.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,004dd031-419c-11e9-9a07-0a80fada099c
#
#  MIOX,55586,HGNC:14522,myo-inositol oxygenase,Homo sapiens,22,22q13.33,A6PVH2;A6PVH4;NP_060054.4;Q9UGB7;Q9UGB7-2;XP_005261982.1;XP_011529007.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,005680e0-419d-11e9-9a07-0a80fada099c
#
# CREB3L1,90993,HGNC:18856,cAMP responsive element binding protein 3 like 1,Homo sapiens,11,11p11.2,E9PK33;H0YEU7;NP_443086.1;Q96BA8;Q96BA8-2;XP_006718443.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00775760-419a-11e9-9a07-0a80fada099c
# 
# ZNF548,147694,HGNC:26561,zinc finger protein 548,Homo sapiens,19,19q13.43,NP_001166244.1;NP_690873.2;Q8NEK5;Q8NEK5-2;XP_016881805.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00bc9c44-41a2-11e9-9a07-0a80fada099c
#
# SLC26A1,10861,HGNC:10993,solute carrier family 26 member 1,Homo sapiens,4,4p16.3,NP_071325.2;NP_998778.1;Q9H2B4,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00bf0430-41a0-11e9-9a07-0a80fada099c
#
# C15orf62,643338,HGNC:34489,chromosome 15 open reading frame 62,Homo sapiens,15,15q15.1,A8K5M9;NP_001123920.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00c3be0c-4199-11e9-9a07-0a80fada099c
#
# RNF225,646862,HGNC:51249,ring finger protein 225,Homo sapiens,19,19q13.43,M0QZC1;NP_001182064.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00e87d78-419f-11e9-9a07-0a80fada099c
#
# VWA3A,146177,HGNC:27088,von Willebrand factor A domain containing 3A,Homo sapiens,16,16p12.2,A6NCI4;A6NCI4-2;A6NCI4-3;A6NCI4-4;H3BUS3;H7BXL8;NP_775886.3;XP_011544044.1;XP_011544046.1;XP_011544047.1;XP_011544048.1;XP_016878436.1;XP_016878437.1;XP_016878438.1;XP_024305926.1,,NULL,Additional CPTAC3 Genes,8/19/2019 20:57,00e9726d-c2c4-11e9-9a07-0a80fada099c
#
# TOMM40L|Tomm40l,84134; 641376,HGNC:25756; MGI:3589,translocase of outer mitochondrial membrane 40 like; translocase of outer mitochondrial membrane 40-like,Homo sapiens; Mus musculus,1,1q23.3; 1|1 H3,D3YXS8;D3YY29;D3Z346;M0QWR6;NP_001032247.1;NP_001273302.1;NP_001273303.1;NP_115550.2;Q969M1;Q969M1-2;Q9CZR3;XP_006497016.1;XP_006711635.1;XP_011508359.1;XP_016857970.1;XP_016857971.1,,NULL,Additional_CPTAC3_Genes,6/4/2019 16:47,00f689b7-41a1-11e9-9a07-0a80fada099c
#
# (base) bash-3.2$ 
#

# Note: the input file has errors in many lines. That is, from presumed DOS/Windows entry of many lines, there is an embedded carriage return that shows up as "^M". For example:
#
# ABCA9,10350,HGNC:39,ATP binding cassette subfamily A member 9,Homo sapiens,17,17q24.2,H0Y4U7;NP_525022.2;Q8IUA7;Q8IUA7-3;Q8IUA7-4;XP_016879500.1;XP_016879501.1;XP_016879502.1;XP_016879503.1;XP_016879504.1,"CARRIAGE_RETURN_AS_CONTROL_M",NULL,NULL,6/4/2019 16:47,20dcda97-b817-11e8-907f-0a2705229b82
#
# This breaks my program.

# I fix this by removing all carriage returns in emacs.
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

# I have replaced 12,704 occurrences of control-M in emacs using the above method, substituting in the literal string "controlM" as a placeholder.
#
# Note: the change adds a pair of double quotes automatically around the replacement text. I then did another emacs replace-string, replacing
#  "controlM" with controlM

# %%%%%%%%%%%%%%%%%%%%%%%

# NOTE: there were a number of lines in the input gene info file where I had to change "," to a literal "comma" in the gene description field.

# I started to do this manually. I then switched to using this function, as seen below:
#    remove_delimiter_commas_from_within_double_quoted_fields(line):

# Such lines included the folllowing:

# BPNT1,10380,HGNC:1096,"3'(2'), 5'-bisphosphate nucleotidase 1",Homo sapiens,1,1q41,A6NF51;F8VRY7;F8VVW8;F8VZG4;F8W1J0;NP_001273078.1;NP_001273079.1;NP_001273080.1;NP_006076.4;O95861;O95861-2;O95861-3;O95861-4;XP_005273055.1;XP_005273056.1;XP_005273057.1;XP_005273061.1;XP_005273062.1;XP_006711177.1;XP_011507365.1;XP_011507366.1;XP_011507367.1;XP_016855532.1;XP_016855533.1;XP_016855534.1;XP_016855535.1,controlM,NULL,NULL,6/4/2019 16:47,f7412adc-b814-11e8-907f-0a2705229b82

# CNP,1267,HGNC:2158,"2',3'-cyclic nucleotide 3' phosphodiesterase",Homo sapiens,17,17q21.2,C9K0L8;K7EN66;K7ERC4;K7ERZ0;NP_001317145.1;NP_149124.3;P09543;P09543-2;XP_011522642.1,controlM,NULL,NULL,6/4/2019 16:47,f7a74718-b814-11e8-907f-0a2705229b82

# gene 3695 = DIS3      
# DIS3,22894,HGNC:20604,"DIS3 homolog, exosome endoribonuclease and 3'-5' exoribonuclease",Homo sapiens,13,13q21.33,F2Z2C0;G3V1J5;NP_001121698.1;NP_001309277.1;NP_001309278.1;NP_055768.3;Q9Y2L1;Q9Y2L1-2,controlM,NULL,NULL,6/4/2019 16:47,f7e878db-b814-11e8-907f-0a2705229b82

# and so on.

# All such lines now have "comma" substituted in for "," in their description fields.

# %%%%%%%%%%%%%%%%%%%%%%

# Note: here, gene_name means HUGO / HGNC gene_symbol

# The fields in the file are
# 1) gene_name
# 2) ncbi_gene_id
# 3) authority
# 4) description
# 5) organism
# 6) chromosome
# 7) locus
# 8) proteins
# 9) assays
# 10) access
# 11) cud_label
# 12) updated  (date)
# 13) gene_uuid

def read_PDC_gene_spreadsheet_into_dictionaries(inputfile, outfile):

   global GLOBAL_NUMBER_OF_GENES
   
   gene_name_to_ncbi_gene_id_dict={}
   gene_name_to_authority_dict={}
   gene_name_to_description_dict={}
   gene_name_to_organism_dict={}
   gene_name_to_chromosome_dict={}
   gene_name_to_locus_dict={}
   gene_name_to_proteins_dict={}
   gene_name_to_assays_dict={}    
   gene_name_to_access_dict={}
   gene_name_to_cud_label_dict={}
   gene_name_to_updated_date_dict={}
   gene_name_to_gene_uuid_dict={}

   protein_name_to_gene_name_dict={}                   
   
   Outfile1 = open(outfile, 'w')   

   print('')
   print('')

   print("-----")
   print("# read_PDC_gene_spreadsheet_into_dictionaries() called")
   print("# This function reads a Proteomic Data Commons (PDC) spreadsheet comma-delimited gene info file into a set of Python dictionaries with the gene_name as key")

   Outfile1.write("# ------\n")
   Outfile1.write("# This file stores the data from  a Proteomic Data Commons (PDC) spreadsheet comma-delimited gene info file in a set of Python dictionaries with the gene_name as key" + "\n")   

   Outfile1.write("# input file = " + inputfile  + "\n")

   Outfile1.write("# program that created this outfile = read_PDC_gene_info_into_dictionaries.py" + "\n")
   Outfile1.write("# name of this Python dictionary file = " + outfile + "\n")
   Outfile1.write("# ------\n")


   Outfile1.write("# read_PDC_gene_spreadsheet_into_dictionaries() function called" + "\n")
   Outfile1.write("# ------\n")

   dataLineNum = 0;
   
   with open(inputfile, 'r') as f:

    # There is a header line here.
    headerline = f.readline()
    headerlineWithoutNewline = headerline.strip()
    print("headerline = '" + headerline + "'")    
    print("headerline stripped = '" + headerlineWithoutNewline + "'")            

    while True:
      line = f.readline()
      if not line: break      
      GLOBAL_NUMBER_OF_GENES += 1
      dataLineNum += 1
      # 
      lineWithoutNewline = line.strip()
      
      processedLine = remove_delimiter_commas_from_within_double_quoted_fields(lineWithoutNewline)

      # print('data line ' + str(dataLineNum) + " = '" + lineWithoutNewline + "'")      
      # print('processedLine ' + str(dataLineNum) + " = '" + processedLine + "'")
      # print("")      

      # The column separator is a comma here.
      columns = processedLine.split(',')
      # print ('columns=', columns)
      #
      gene_name       = str(columns[0]).strip()
      ncbi_gene_id    = str(columns[1]).strip()
      authority       = str(columns[2]).strip()
      description     = str(columns[3]).strip()
      organism        = str(columns[4]).strip()
      chromosome      = str(columns[5]).strip()
      locus           = str(columns[6]).strip()
      proteins        = str(columns[7]).strip()
      assays          = str(columns[8]).strip()      
      access          = str(columns[9]).strip()
      cud_label       = str(columns[10]).strip()            
      updated_date    = str(columns[11]).strip()
      gene_uuid       = str(columns[12]).strip()

      description_without_double_quotes = description.replace('"', "")
      desc_without_single_quotes = description_without_double_quotes.replace("'", "_single_quote_")

      # sample case:
      if gene_name == "BPNT1":
          print("")
          print("Mods made - to allow storage in python dicts - to description field for gene " + gene_name + ":")          
          print("    description = " + description)
          print("    description without double quotes = " + description_without_double_quotes)
          print("    description without double and without single quotes = " + desc_without_single_quotes)          
          print("")
          
      gene_name_to_ncbi_gene_id_dict[gene_name] = ncbi_gene_id
      gene_name_to_authority_dict[gene_name]    = authority
      gene_name_to_description_dict[gene_name]  = desc_without_single_quotes
      gene_name_to_organism_dict[gene_name]     = organism
      gene_name_to_chromosome_dict[gene_name]   = chromosome
      gene_name_to_locus_dict[gene_name]        = locus
      gene_name_to_proteins_dict[gene_name]     = proteins
      gene_name_to_assays_dict[gene_name]       = assays
      gene_name_to_access_dict[gene_name]       = access
      gene_name_to_cud_label_dict[gene_name]    = access
      gene_name_to_updated_date_dict[gene_name] = updated_date
      gene_name_to_gene_uuid_dict[gene_name]    = gene_uuid

      # 2/20/20
      proteins_list = proteins.split(";")
      for protein_name in proteins_list:
         protein_name_to_gene_name_dict[protein_name] = gene_name

   f.close()

   print("number of data lines = " + str(dataLineNum) )

   NUMBER_OF_ENTRIES_IN_GENE_NAME_DICTIONARIES = len(gene_name_to_ncbi_gene_id_dict)
   
   Outfile1.write("# \n")
   Outfile1.write("# number of gene name entries read from input file = " + str(NUMBER_OF_ENTRIES_IN_GENE_NAME_DICTIONARIES) + "\n")

   Outfile1.write("# -----\n")
   Outfile1.write("# input file = " + inputfile + "\n")   
      
   Outfile1.write("# -----\n")
   Outfile1.write("\n")

   Outfile1.write("pdc_gene_name_to_ncbi_gene_id_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_authority_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_description_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_organism_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_chromosome_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_locus_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_proteins_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_assays_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_access_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_cud_label_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_updated_date_dict={}" + "\n")
   Outfile1.write("pdc_gene_name_to_gene_uuid_dict={}" + "\n")                              
   Outfile1.write("\n")
   Outfile1.write("pdc_protein_name_to_gene_name_dict={}" + "\n")
   Outfile1.write("\n")
   
   Outfile1.write("# -----\n")
   Outfile1.write("\n")

   gene_num = 0   
   for gene_name in sorted (gene_name_to_ncbi_gene_id_dict.keys() ):
     # print(key, value)
     gene_num += 1
     ncbi_gene_id = gene_name_to_ncbi_gene_id_dict[gene_name]
     authority    = gene_name_to_authority_dict[gene_name]
     description  = gene_name_to_description_dict[gene_name]
     organism     = gene_name_to_organism_dict[gene_name]
     chromosome   = gene_name_to_chromosome_dict[gene_name]
     locus        = gene_name_to_locus_dict[gene_name]
     proteins     = gene_name_to_proteins_dict[gene_name]
     assays       = gene_name_to_assays_dict[gene_name]     
     access       = gene_name_to_access_dict[gene_name]
     cud_label    = gene_name_to_cud_label_dict[gene_name]
     updated_date = gene_name_to_updated_date_dict[gene_name]
     gene_uuid    = gene_name_to_gene_uuid_dict[gene_name]                    

     
     Outfile1.write("\n")
     Outfile1.write("# gene " + str(gene_num) + " = " + gene_name + "\n")          
     Outfile1.write("pdc_gene_name_to_ncbi_gene_id_dict['" + gene_name  + "'] = '" + ncbi_gene_id + "'\n")
     Outfile1.write("pdc_gene_name_to_authority_dict['" + gene_name  + "']    = '" + authority + "'\n")
     Outfile1.write("pdc_gene_name_to_description_dict['" + gene_name  + "']  = '" + description + "'\n")
     Outfile1.write("pdc_gene_name_to_organism_dict['" + gene_name  + "']     = '" + organism + "'\n")
     Outfile1.write("pdc_gene_name_to_chromosome_dict['" + gene_name  + "']   = '" + chromosome + "'\n")
     Outfile1.write("pdc_gene_name_to_locus_dict['" + gene_name  + "']        = '" + locus + "'\n")
     Outfile1.write("pdc_gene_name_to_proteins_dict['" + gene_name  + "']     = '" + proteins + "'\n")
     Outfile1.write("pdc_gene_name_to_assays_dict['" + gene_name  + "']       = '" + assays + "'\n")
     Outfile1.write("pdc_gene_name_to_access_dict['" + gene_name  + "']       = '" + access + "'\n")
     Outfile1.write("pdc_gene_name_to_cud_label_dict['" + gene_name  + "']    = '" + cud_label + "'\n")
     Outfile1.write("pdc_gene_name_to_updated_date_dict['" + gene_name  + "'] = '" + updated_date + "'\n")
     Outfile1.write("pdc_gene_name_to_gene_uuid_dict['" + gene_name  + "']    = '" + gene_uuid + "'\n")

     proteins_list = proteins.split(";")
     Outfile1.write("\n")     
     for protein_name in proteins_list:
         Outfile1.write("pdc_protein_name_to_gene_name_dict['" + protein_name  + "']    = '" + gene_name + "'\n")        
     
   Outfile1.write("\n")
   Outfile1.write("#   ----  ENDFILE ---- \n")     
   Outfile1.close()

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def remove_delimiter_commas_from_within_double_quoted_fields(line):

   new_line = ""
   currently_in_double_quoted_term = False
   char_pos = 0
   for char in line:
      char_pos += 1
      if char == "\"" and currently_in_double_quoted_term == False:
         # We presumably are entering a double quoted term
         currently_in_double_quoted_term = True
         # We discard the double quote as unneeded         
         continue
      if char == "," and currently_in_double_quoted_term == True:
          # print("")
          # print("in line: '" + line + "'")
          # print("changing , to comma at char_pos " + str(char_pos) + " : '" + line + "'")
          new_line = new_line + " comma "
          continue
      if char == "\"" and currently_in_double_quoted_term == True:
         # We are exiting a term surrounded by double quotes
         currently_in_double_quoted_term = False
         # We discard the double quote as unneeded
         continue
      new_line = new_line + char
   return new_line

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

print("Start")

print('')
print('----------------')
print('Importing regular expression module re  ...')
import re

GLOBAL_NUMBER_OF_GENES = 0 

# on personal laptop
# 
# 
# 

# inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20.csv"
# inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20.csv"

# inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes.csv"
# inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes.csv"

# inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes_and_BPNT1_CNP_comma_fixes.csv"
# inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes_and_BPNT1_CNP_comma_fixes.csv"

# My manual comma fixes are incomplete in the file named below; I switched to handling such comma replacements automatically within this program.
# inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes_and_manual_comma_fixes.csv~"
# inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes_and_manual_comma_fixes.csv"

# inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes.csv"
# inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_fixes.csv"

inputfile      = "/GDITWork/PDC_queries/pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_instances_removed.csv"
inputfile_name = "pdc_genes_to_protein_mapping_file_as_of_1_22_20_with_12704_controlM_instances_removed.csv"


outfile        = "/GDITWork/PDC_queries/pdc_gene_info_dictionaries.py"

read_PDC_gene_spreadsheet_into_dictionaries(inputfile, outfile)

print("----- ")
print("NUMBER_OF_GENE entries read = " + str(GLOBAL_NUMBER_OF_GENES) )

print("input file  = " + inputfile )
print("output file = " + outfile )                                     

print("----- ")                  

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
print("Done.")

