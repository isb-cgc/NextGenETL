# ETL HOWTO: Metadata BQ table creation (currently performed on ISB internal machine)


---------------------------
A) Metadata BQ tables

1) We get release info from "GDC Support Team" <fitz@UCHICAGO.EDU> going to GDC-USERS-L@LIST.NIH.GOV. I subscribed to GDC-USERS-L at https://LIST.NIH.GOV.

2) The scripts that hit the case and file APIs are on an ISB server. This is being moved to the cloud.

3) There are three directories per release, e.g. rel17-current, rel17-legacy, and rel17-forBQ. Do the next two steps for both legacy and active.

4) In rel*-legacy/current, see the script run_try1.sh. It runs two python scripts, located in github project0/python, named queryByCase.py and queryByFile.py. Each hammer the GDC endpoints for hours or days. Run these. Note that traditionally, the `verboseFlag` has been set to `14`, causing the case log file in legacy to run about 842G, and the file log at 20.5G, when complete. One needs to set that flag to `0` to avoid these massive log files.

5) Table files are created with hex codes to keep download versions straight. Creates aliqMap, caseData, caseIDmap, fileData, fileList, and slidMap tsv files. There is a preprocessing step for fileData and caseData that writes original info into a tmp file and then outputs to the tsv file.

6) Failures of API calls (after 10 tries?) are de-facto silent (they do log to output, but grepping for ERROR in a .8 TB log file takes a large amount of time). This results in e.g. the legacy file count for a case appearing as zero. On way to check this quickly is to scan the caseData.bq.\*.tsv files for case entries that are missing almost all fields. The script `missing_data.sh` can be used to check for these cases. If there are any missing cases, they need to be rerun. (Note: 11/9/19: queryByCase.py does not allow a list of case IDs to be provided, just a single one as a command line argument. This is a work in progress.)

7) The next steps take place in the rel*-forBQ directory. The scripts in that directory relatively reference scripts in the ../scripts directory (transpose, createSchema.py, good_look.sh) so current directory of execution matters.

8) The script to handle caseData is more complex than the others, and separate. It is called "just_case.sh", and does cutting of fields out and merging the various files into "caseData.merge.t1". It looks like the hex codes are hand-entered here, and also *per-release hand-curation* of which fields are being kept and which are tossed. This script depends on the file just_case.header.KEEP. Note that comparing the schema of the current run versus the last run should reveal changes in the needed cuts.

9) Next, proc_release_tables.rel*.sh. It looks like the hex codes are hand-entered here, and merged into a single table with column headers glued into the first row. There is a "sort | uniq" step in there too, not sure why there would be duplicates? The output is e.g. "aliqMap.merge.t1".

10) SUPER-IMPORTANT! Both the case API and file API runs output a file named "fileData.bq.\*.tsv". The former file is about 624 MB, while the latter is 659 MB. The former, of course, will not have any files in it that are not related to a specific case. Thus, if that one is accidentally used to create the fileData BQ table, we end up with 75K fewer rows. Make SURE to specify the correct hex code for the file API version, and check that the legacy table size is consistent with past releases.

11) In proc_release_tables.rel*.sh, there is a cut operation that is performed on fileData tables to eliminate an error_type field. The tables are copied to \*.t1, cut to \*.t2, and moved back to \*.t1. This appears to need *per-release hand-curation.*

12) There is a script called "good_look.sh". It is run as part of the merge operations, and creates a "\*.look" file for each table. This file lists each column, with its number, the number of unique values in the column, and the most common and least common ten values in the table. Note that this reveals 76416 files with no cases__case_id in both rel16 and rel17. It is a good idea to skim these files and compare to previous release versions.

13) proc_release_tables.rel*.sh also creates schema json files, calling python ~/git_home/isb-cgc/examples-Python/python/createSchema.py.

14) The createSchema.py script will create files of the form e.g. aliqMap.t1.json. These are the raw schemas, and should be compared with the raw schemas of the last release. Any changes will require that the current table schema (soon to be stored in GitHub) needs to be updated to match the new schema. The new schema file names need to be dated.

15) The next two steps require google cloud tools. Make sure PATH=$PATH:/titan/cancerregulome10/CGC/google-tools/google-cloud-sdk/bin, and that you have run glcoud init on the machine.

16) If you just want to upload files to GCS, you cam use the script just_loadGCS.sh. This copies the files up into GCS buckets. It has hardcoded rel\* paths that must be edited. If you are putting stuff in a non-existent directory in the bucket, you need to make it first. If you skip this step, the following just_loadBQ.\*.sh script also uploads files to GCS.

17) Next is just_loadBQ.\*.sh, which has hardcoded table, schema date, and release directory IDs that must be edited.

18) The above steps put BQ tables in the isb-etl-open project BQ datasets. They then need to be copied across into the isb-cgc "release" GDC_metadata BQ dataset. This is currently done in the console.