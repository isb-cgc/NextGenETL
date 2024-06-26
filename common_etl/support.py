"""

Copyright 2019-2020, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from google.cloud import bigquery
from google.cloud import storage
from google.cloud import exceptions
from google.cloud.exceptions import NotFound
import shutil
import os
import sys
import requests
import copy
import urllib.parse as up
import time
import zipfile
import gzip
import threading
from json import loads as json_loads, dumps as json_dumps
from git import Repo


def checkToken(aToken):
    """
    Used by pickColumns, below:
    """
    if (aToken.find(';') >= 0):
        aList = aToken.split(';')
        aList.sort()
        newT = ''
        for bToken in aList:
            newT += bToken
            if (len(newT) > 0): newT += ';'
        if (newT[-1] == ';'): newT = newT[:-1]
        return (newT)

    elif (aToken.find(',') >= 0):
        aList = aToken.split(',')
        aList.sort()
        newT = ''
        for bToken in aList:
            newT += bToken
            if (len(newT) > 0): newT += ','
        if (newT[-1] == ','): newT = newT[:-1]
        return (newT)

    return (aToken)


def pickColumns(tokenList):
    """
    Sheila's legacy pickColumns() Function
    For a list of tokens skip some and sort on others. Note that we do not do any pruning
    anymore. This function is retained for archival reasons.
    """

    ## original lists from Nov 2016 ...
    skipCols = [14, 17, 18, 21, 22, 23, 24, 26, 27, 29, 30, 43, 44, 57, 88, 89, 90, 91, 107]
    sortCols = [2, 28, 31]

    ## new lists from May 2017 ...
    skipCols = [17, 18, 21, 22, 23, 24, 25, 26, 27, 29, 30, 43, 44, 57, 88, 89, 90, 91, 96, 108]
    ## maybe we don't skip anything?
    skipCols = []

    ## information that should be sorted for consistency, eg column #2 = Center,
    ## #14 = dbSNP_Val_Status, #28 = Validation_Method, #31 = Sequencer, #50 = Consequence, etc...
    sortCols = [2, 14, 28, 31, 50, 75, 85, 87, 109, 115, 116]
    ## new for June MAFs
    sortCols = [2, 14, 28, 31, 50, 51, 76, 86, 88, 110, 116, 117]
    ## new in Jan 2018 ... in some of the fields, the ORDER MATTERS!
    sortCols = []

    newList = []

    for ii in range(len(tokenList)):
        if ii not in skipCols:
            if ii in sortCols:
                newList += [checkToken(tokenList[ii])]
            else:
                newList += [tokenList[ii]]

    return newList


def write_MAFs(tumor, mutCalls, hdrPick, mutCallers, do_logging):
    """
    Sheila's function to write out MAFs for merging
    Original MAF table merged identical results from the different callers. This is the function
    to write out the results from the merged dictionaries.
    """
    with open("MAFLOG-WRITE-{}.txt".format(tumor), 'w') as log_file:
        outFilename = "mergeA." + tumor + ".maf"

        with open(outFilename, 'w') as fhOut:

            outLine = ''
            for aT in hdrPick:
                outLine += aT + '\t'
            fhOut.write("%s\n" % outLine[:-1])

            histCount = [0] * 10

            mutPrints = mutCalls.keys()
            log_file.write(" --> total # of mutPrints : {}\n".format(len(mutPrints)))

            for aPrint in mutPrints:

                if do_logging: log_file.write(" ")
                if do_logging: log_file.write(" looping over mutPrints ... {}\n".format(str(aPrint)))
                numCalls = len(mutCalls[aPrint])
                if do_logging: log_file.write("     numCalls = {}\n".format(numCalls))
                histCount[numCalls] += 1
                outLine = ''
                if (numCalls > 0):
                    if do_logging: log_file.write("     # of features = {}\n".format(len(mutCalls[aPrint][0])))
                    if do_logging: log_file.write("{}\n".format(str(mutCalls[aPrint][0])))

                    ## for each feature we want to see if the different callers
                    ## came up with different outputs ... so we create a vector
                    ## of all of the outputs [v], and a vector of only the unique
                    ## outputs [u]
                    for kk in range(len(mutCalls[aPrint][0])):
                        u = []
                        v = []
                        for ii in range(len(mutCalls[aPrint])):
                            if (mutCalls[aPrint][ii][kk] not in u):
                                u += [mutCalls[aPrint][ii][kk]]
                            v += [mutCalls[aPrint][ii][kk]]
                        if (len(u) > 1):
                            if do_logging: log_file.write(
                                "{} {} {} {}\n".format(str(kk), str(hdrPick[kk]), len(u), str(v)))

                        ## if we have nothing, then it's a blank field
                        if (len(v) == 0):
                            outLine += "\t"

                        ## if we only have one value, then just write that
                        elif (len(u) == 1 or len(v) == 1):
                            outLine += "%s\t" % v[0]

                        ## otherwise we need to write out the the values in the
                        ## order of the callers ...
                        else:
                            if do_logging: log_file.write(" looping over {}\n".format(mutCallers))
                            if do_logging: log_file.write("{}\n".format(str(u)))
                            if do_logging: log_file.write("{}\n".format(str(v)))
                            for c in mutCallers:
                                for ii in range(len(mutCalls[aPrint])):
                                    ## 3rd from the last feature is the 'caller'
                                    if (mutCalls[aPrint][ii][-3] == c):
                                        if do_logging: log_file.write("         found ! {}\n".format(str(c)))
                                        outLine += "%s|" % v[ii]
                            outLine = outLine[:-1] + "\t"
                    fhOut.write("%s\n" % outLine[:-1])

    return histCount


def read_MAFs(tumor_type, maf_list, program_prefix, extra_cols, col_count,
              do_logging, key_fields, first_token, file_info_func):
    """
    Sheila's function to read MAFs for merging.
    Original MAF table merged identical results from the different callers. This is the function to read
    in results and build merged dictionaries.
    """
    hdrPick = None
    mutCalls = {}
    with open("MAFLOG-READ-{}.txt".format(tumor_type), 'w') as log_file:

        for aFile in maf_list:
            file_info_list = file_info_func(aFile, program_prefix)
            if file_info_list[0] != (program_prefix + tumor_type):
                continue
            try:
                if do_logging: log_file.write(" Next file is <%s> \n" % aFile)
                toss_zip = False
                if aFile.endswith('.gz'):
                    dir_name = os.path.dirname(aFile)
                    use_file_name = aFile[:-3]
                    log_file.write("Uncompressing {}\n".format(aFile))
                    with gzip.open(aFile, "rb") as gzip_in:
                        with open(use_file_name, "wb") as uncomp_out:
                            shutil.copyfileobj(gzip_in, uncomp_out)
                    toss_zip = True
                else:
                    use_file_name = aFile

                log_file.write(" opening input file {}\n".format(use_file_name))
                log_file.write(" fileInfo : {}\n".format(str(file_info_list)))

                if os.path.isfile(use_file_name):
                    with open(use_file_name, 'r') as fh:
                        key_indices = []
                        for aLine in fh:
                            if aLine.startswith("#"):
                                continue
                            if aLine.startswith(first_token):
                                aLine = aLine.strip()
                                hdrTokens = aLine.split('\t')
                                if len(hdrTokens) != col_count:
                                    print("ERROR: incorrect number of header tokens! {} vs {}".format(col_count,
                                                                                                      len(hdrTokens)))
                                    print(hdrTokens)
                                    raise Exception()
                                    ## We no longer prune or sort columns, so assignment is now direct:
                                hdrPick = copy.copy(hdrTokens)
                                ## hdrPick = pickColumns ( hdrTokens )
                                ## since we're not skipping any columns, we should have 120 at this point
                                ## print " --> len(hdrPick) = ", len(hdrPick)
                                hdrPick += extra_cols
                                ## and 124 at this point ...
                                ## print " --> after adding a few more fields ... ", len(hdrPick)
                                for keef in key_fields:
                                    key_indices.append(hdrPick.index(keef))
                                continue

                            if hdrPick is None:
                                print("ERROR: Header row not found")
                                raise Exception()

                            aLine = aLine.strip()
                            tokenList = aLine.split('\t')
                            if len(tokenList) != len(hdrTokens):
                                print(
                                    "ERROR: incorrect number of tokens! {} vs {}".format(len(tokenList),
                                                                                         len(hdrTokens)))
                                raise Exception()

                            # This creates a key for a dictionary for each mutation belonging to a tumor sample:

                            mpl = [tokenList[x] for x in key_indices]
                            mutPrint = tuple(mpl)
                            if do_logging: log_file.write("{}\n".format(str(mutPrint)))

                            # list for each key:
                            if mutPrint not in mutCalls:
                                mutCalls[mutPrint] = []

                            ##infoList = pickColumns(tokenList) + file_info_list
                            ## Again, no longer pruning columns!
                            infoList = tokenList + file_info_list
                            if len(infoList) != len(hdrPick):
                                print(" ERROR: inconsistent number of tokens!")
                                raise Exception()

                            if do_logging: log_file.write(" --> infoList : {}\n".format(str(infoList)))
                            mutCalls[mutPrint] += [infoList]
                            if do_logging: log_file.write(
                                " --> len(mutCalls[mutPrint]) = {}\n".format(len(mutCalls[mutPrint])))

                        log_file.write(" --> done with this file ... {}\n".format(len(mutCalls)))
                else:
                    # If previous job died a nasty death, following finally statement may not get run, causing
                    # file manifest to hold onto a previously unzipped file that gets deleted. Catch that
                    # problem!
                    print('{} was not found'.format(use_file_name))
            finally:
                if toss_zip and os.path.isfile(use_file_name):
                    os.remove(use_file_name)

        log_file.write("\n")
        log_file.write(" DONE READING MAFs ... \n")
        log_file.write("\n")
    return mutCalls, hdrPick


def concat_all_merged_files(all_files, one_big_tsv):
    """
    Concatenate all Merged Files
    Gather up all merged files and glue them into one big one.
    """

    print("building {}".format(one_big_tsv))
    first = True
    header_id = None
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            with open(filename, 'r') as readfile:
                for line in readfile:
                    if line.startswith('#'):
                        continue
                    split_line = line.split("\t")
                    if first:
                        header_id = split_line[0]
                        print("Header starts with {}".format(header_id))
                        first = False
                    if not line.startswith(header_id) or first:
                        outfile.write(line)

    print("finished building {}".format(one_big_tsv))
    return


def build_pull_list_with_bq(manifest_table, indexd_table, project, tmp_dataset, tmp_bq,
                            tmp_bucket, tmp_bucket_file, local_file, do_batch):
    """
    IndexD using BQ Tables
    GDC provides us a file that allows us to not have to pound the IndexD API; we build a BQ table.
    Use it to resolve URIs
    """
    #
    # If we are using bq to build a manifest, we can use that table to build the pull list too!
    #

    sql = pull_list_builder_sql(manifest_table, indexd_table)
    success = generic_bq_harness(sql, tmp_dataset, tmp_bq, do_batch, True)
    if not success:
        return False
    success = bq_to_bucket_tsv(tmp_bq, project, tmp_dataset, tmp_bucket, tmp_bucket_file, do_batch, False)
    if not success:
        return False
    bucket_to_local(tmp_bucket, tmp_bucket_file, local_file)
    return True


def build_pull_list_with_bq_public(manifest_table, indexd_table, project, tmp_dataset, tmp_bq,
                                   tmp_bucket, tmp_bucket_file, local_file, do_batch):
    """
    IndexD using BQ Tables
    GDC provides us a file that allows us to not have to pound the IndexD API; we build a BQ table.
    Use it to resolve URIs
    """
    #
    # If we are using bq to build a manifest, we can use that table to build the pull list too!
    #

    sql = pull_list_builder_sql_public(manifest_table, indexd_table)
    success = generic_bq_harness(sql, tmp_dataset, tmp_bq, do_batch, True)
    if not success:
        return False
    success = bq_to_bucket_tsv(tmp_bq, project, tmp_dataset, tmp_bucket, tmp_bucket_file, do_batch, False)
    if not success:
        return False
    bucket_to_local(tmp_bucket, tmp_bucket_file, local_file)
    return True


def pull_list_builder_sql(manifest_table, indexd_table):
    """
    Generates SQL for above function
    """
    return '''
    SELECT b.gs_url
    FROM `{0}` as a JOIN `{1}` as b ON a.id = b.id
    '''.format(manifest_table, indexd_table)


#
# Like the above function, but uses the final public mapping table instead:
#


def pull_list_builder_sql_public(manifest_table, indexd_table):
    """
    Generates SQL for above function
    """
    return '''
    SELECT b.file_gdc_url
    FROM `{0}` as a JOIN `{1}` as b ON a.id = b.file_gdc_id
    '''.format(manifest_table, indexd_table)


def get_the_bq_manifest(file_table, filter_dict, max_files, project, tmp_dataset, tmp_bq,
                        tmp_bucket, tmp_bucket_file, local_file, do_batch):
    """
    Build a Manifest File Using ISB-CGC File Tables. This duplicates the manifest file returned by
    GDC API, but uses the BQ file table as the data source.
    """

    sql = manifest_builder_sql(file_table, filter_dict, max_files)

    print(sql)

    success = generic_bq_harness(sql, tmp_dataset, tmp_bq, do_batch, True)
    if not success:
        return False
    success = bq_to_bucket_tsv(tmp_bq, project, tmp_dataset, tmp_bucket, tmp_bucket_file, do_batch, True)
    if not success:
        return False
    bucket_to_local(tmp_bucket, tmp_bucket_file, local_file)
    return True


def manifest_builder_sql(file_table, filter_dict_list, max_files):
    """
    Generates SQL for above function
    """
    filter_list = []
    a_clause = "{} = '{}'"

    for filter in filter_dict_list:
        for key, val in filter.items():
            if isinstance(val, list):
                or_list = []
                for aval in val:
                    or_list.append(a_clause.format(key, aval))
                all_ors = ' OR '.join(or_list)
                full_clause = "({})".format(all_ors)
                filter_list.append(full_clause)
            else:
                filter_list.append(a_clause.format(key, val))

    all_filters = ' AND '.join(filter_list)
    all_filters = "WHERE {}".format(all_filters)

    limit_clause = "" if max_files is None else "LIMIT {}".format(max_files)

    return '''
    SELECT file_gdc_id as id,
           file_name as filename,
           md5sum as md5,
           file_size as size,
           file_state as state
    FROM `{0}`
    {1} {2}
    '''.format(file_table, all_filters, limit_clause)


def bq_to_bucket_tsv(src_table, project, dataset, bucket_name, bucket_file, do_batch, do_header):
    """
    Get a BQ Result to a Bucket TSV file
    Export BQ table to a cloud bucket
    """
    client = bigquery.Client(project=project)
    destination_uri = "gs://{}/{}".format(bucket_name, bucket_file)
    dataset_ref = client.dataset(dataset, project=project)
    table_ref = dataset_ref.table(src_table)

    job_config = bigquery.ExtractJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    location = 'US'
    job_config.field_delimiter = '\t'
    job_config.print_header = do_header

    extract_job = client.extract_table(table_ref, destination_uri, location="US", job_config=job_config)

    # Query
    extract_job = client.get_job(extract_job.job_id, location=location)
    job_state = extract_job.state

    while job_state != 'DONE':
        extract_job = client.get_job(extract_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(extract_job.job_id, extract_job.state))
        job_state = extract_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(extract_job.job_id))

    extract_job = client.get_job(extract_job.job_id, location=location)
    if extract_job.error_result is not None:
        print('Error result!! {}'.format(extract_job.error_result))
        return False
    return True


def bucket_to_local(bucket_name, bucket_file, local_file):
    """
    Get a Bucket File to Local
    Export a cloud bucket file to the local filesystem
    No leading / in bucket_file name!!
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(bucket_file)  # no leading / in blob name!!
    blob.download_to_filename(local_file)
    return


def bucket_to_bucket(source_bucket_name, bucket_file, target_bucket_name, target_bucket_file=None):
    """
    Get a Bucket File to another bucket
    No leading / in bucket_file name!!
    Target bucket is the same as source, unless provided
    """
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(bucket_file)  # no leading / in blob name!!

    destination_bucket = storage_client.bucket(target_bucket_name)

    if target_bucket_file is None:
        target_bucket_file = bucket_file
    source_bucket.copy_blob(source_blob, destination_bucket, target_bucket_file)
    return


def build_manifest_filter(filter_dict_list):
    """
    Build a manifest filter using the list of filter items you can get from a GDC search
    """
    # Note the need to double up the "{{" to make the format command happy:
    filter_template = '''
    {{
        "op": "in",
        "content": {{
            "field": "{0}",
            "value": [
                "{1}"
            ]
        }}
    }}
    '''

    prefix = '''
    {
      "op": "and",
      "content": [

    '''
    suffix = '''
      ]
    }
    '''

    filter_list = []
    for kv_pair in filter_dict_list:
        for k, v in kv_pair.items():
            if len(filter_list) > 0:
                filter_list.append(',\n')
            filter_list.append(filter_template.format(k, v.rstrip('\n')))

    whole_filter = [prefix] + filter_list + [suffix]
    return ''.join(whole_filter)


def get_the_manifest(filter_string, api_url, manifest_file, max_files=None):
    """
    This function takes a JSON filter string and uses it to download a manifest from GDC
    """

    #
    # 1) When putting the size and "return_type" : "manifest" args inside a POST document, the result comes
    # back as JSON, not the manifest format.
    # 2) Putting the return type as parameter in the URL while doing a post with the filter just returns
    # every file they own (i.e. the filter in the POST doc is ignored, probably to be expected?).
    # 3) Thus, put the filter in a GET request.
    #

    num_files = max_files if max_files is not None else 100000
    request_url = '{}?filters={}&size={}&return_type=manifest'.format(api_url,
                                                                      up.quote(filter_string),
                                                                      num_files)

    resp = requests.request("GET", request_url)

    if resp.status_code == 200:
        mfile = manifest_file
        with open(mfile, mode='wb') as localfile:
            localfile.write(resp.content)
        print("Wrote out manifest file: {}".format(mfile))
        return True
    else:
        print()
        print("Request URL: {} ".format(request_url))
        print("Problem downloading manifest file. HTTP Status Code: {}".format(resp.status_code))
        print("HTTP content: {}".format(resp.content))
        return False


def create_clean_target(local_files_dir):
    """
    GDC download client builds a tree of files in directories. This routine clears the tree out if it exists.
    """

    if os.path.exists(local_files_dir):
        print("deleting {}".format(local_files_dir))
        try:
            shutil.rmtree(local_files_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

        print("done {}".format(local_files_dir))

    if not os.path.exists(local_files_dir):
        os.makedirs(local_files_dir)


def build_pull_list_with_indexd(manifest_file, indexd_max, indexd_url, local_file):
    """
    Generate a list of gs:// urls to pull down from a manifest, using indexD
    """

    # Parse the manifest file for uids, pull out other data too as a sanity check:

    manifest_vals = {}
    with open(manifest_file, 'r') as readfile:
        first = True
        for line in readfile:
            if first:
                first = False
                continue
            split_line = line.rstrip('\n').split("\t")
            manifest_vals[split_line[0]] = {
                'filename': split_line[1],
                'md5': split_line[2],
                'size': int(split_line[3])
            }

    # Use IndexD to map to Google bucket URIs. Batch up IndexD calls to reduce API load:

    print("Pulling {} files from buckets...".format(len(manifest_vals)))
    max_per_call = indexd_max
    indexd_results = {}
    num_full_calls = len(manifest_vals) // max_per_call  # Python 3: // is classic integer floor!
    num_final_call = len(manifest_vals) % max_per_call
    all_calls = num_full_calls + (1 if (num_final_call > 0) else 0)
    uuid_list = []
    call_count = 0
    for uuid in manifest_vals:
        uuid_list.append(uuid)
        list_len = len(uuid_list)
        is_last = (num_final_call > 0) and (call_count == num_full_calls)
        if list_len == max_per_call or (is_last and list_len == num_final_call):
            request_url = '{}{}'.format(indexd_url, ','.join(uuid_list))
            resp = requests.request("GET", request_url)
            call_count += 1
            print("completed {} of {} calls to IndexD".format(call_count, all_calls))
            file_dict = json_loads(resp.text)
            for i in range(0, list_len):
                call_id = uuid_list[i]
                curr_record = file_dict['records'][i]
                curr_id = curr_record['did']
                manifest_record = manifest_vals[curr_id]
                indexd_results[curr_id] = curr_record
                if curr_record['did'] != curr_id or \
                        curr_record['hashes']['md5'] != manifest_record['md5'] or \
                        curr_record['size'] != manifest_record['size']:
                    raise Exception(
                        "Expected data mismatch! {} vs. {}".format(str(curr_record), str(manifest_record)))
            uuid_list.clear()

    # Create a list of URIs to pull, write to specified file:

    with open(local_file, mode='w') as pull_list_file:
        for uid, record in indexd_results.items():
            url_list = record['urls']
            gs_urls = [g for g in url_list if g.startswith('gs://')]
            if len(gs_urls) != 1:
                raise Exception("More than one gs:// URI! {}".format(str(gs_urls)))
            pull_list_file.write(gs_urls[0] + '\n')

    return


class BucketPuller(object):
    """Multithreaded  bucket puller"""

    def __init__(self, thread_count):
        self._lock = threading.Lock()
        self._threads = []
        self._total_files = 0
        self._read_files = 0
        self._thread_count = thread_count
        self._bar_bump = 0

    def __str__(self):
        return "BucketPuller"

    def reset(self):
        self._threads.clear()
        self._total_files = 0
        self._read_files = 0
        self._bar_bump = 0

    def pull_from_buckets(self, pull_list, local_files_dir):
        """
          List of all project IDs
        """
        self._total_files = len(pull_list)
        self._bar_bump = self._total_files // 100
        if self._bar_bump == 0:
            self._bar_bump = 1
        size = self._total_files // self._thread_count
        size = size if self._total_files % self._thread_count == 0 else size + 1
        chunks = [pull_list[pos:pos + size] for pos in range(0, self._total_files, size)]
        for i in range(0, self._thread_count):
            if i >= len(chunks):
                break
            th = threading.Thread(target=self._pull_func, args=(chunks[i], local_files_dir))
            self._threads.append(th)

        for i in range(0, len(self._threads)):
            self._threads[i].start()

        for i in range(0, len(self._threads)):
            self._threads[i].join()

        print_progress_bar(self._read_files, self._total_files)
        return

    def _pull_func(self, pull_list, local_files_dir):
        storage_client = storage.Client()
        for url in pull_list:
            path_pieces = up.urlparse(url)
            dir_name = os.path.dirname(path_pieces.path)
            make_dir = "{}{}".format(local_files_dir, dir_name)
            os.makedirs(make_dir, exist_ok=True)
            bucket = storage_client.bucket(path_pieces.netloc)
            blob = bucket.blob(path_pieces.path[1:])  # drop leading / from blob name
            full_file = "{}{}".format(local_files_dir, path_pieces.path)
            blob.download_to_filename(full_file)
            self._bump_progress()

    def _bump_progress(self):

        with self._lock:
            self._read_files += 1
            if (self._read_files % self._bar_bump) == 0:
                print_progress_bar(self._read_files, self._total_files)


def pull_from_buckets(pull_list, local_files_dir):
    """
    Run the "Download Client", which now justs hauls stuff out of the cloud buckets
    """

    # Parse the manifest file for uids, pull out other data too as a sanity check:

    num_files = len(pull_list)
    print("Begin {} bucket copies...".format(num_files))
    storage_client = storage.Client()
    copy_count = 0
    for url in pull_list:
        path_pieces = up.urlparse(url)
        dir_name = os.path.dirname(path_pieces.path)
        make_dir = "{}{}".format(local_files_dir, dir_name)
        os.makedirs(make_dir, exist_ok=True)
        bucket = storage_client.bucket(path_pieces.netloc)
        blob = bucket.blob(path_pieces.path[1:])  # drop leading / from blob name
        full_file = "{}{}".format(local_files_dir, path_pieces.path)
        blob.download_to_filename(full_file)
        copy_count += 1
        if (copy_count % 10) == 0:
            print_progress_bar(copy_count, num_files)
    print_progress_bar(num_files, num_files)


def build_file_list(local_files_dir):
    """
    Build the File List
    Using the tree of downloaded files, we build a file list. Note that we see the downloads
    (using the GDC download tool) bringing along logs and annotation.txt files, which we
    specifically omit.
    """
    print("building file list from {}".format(local_files_dir))
    all_files = []
    for path, dirs, files in os.walk(local_files_dir):
        if not path.endswith('logs'):
            for f in files:
                if f != 'annotations.txt':
                    if f.endswith('parcel'):
                        raise Exception
                    all_files.append('{}/{}'.format(path, f))

    print("done building file list from {}".format(local_files_dir))
    return all_files


def generic_bq_harness(sql, target_dataset, dest_table, do_batch, do_replace, project=None):
    """
    Handles all the boilerplate for running a BQ job
    """
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    write_depo = "WRITE_TRUNCATE" if do_replace else None
    return generic_bq_harness_write_depo(sql, target_dataset, dest_table, do_batch, write_depo, project=project)

def generic_bq_harness_write_depo(sql, target_dataset, dest_table, do_batch, write_depo, project=None):
    """
    Handles all the boilerplate for running a BQ job
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    if write_depo is not None:
        job_config.write_disposition = write_depo

    target_ref = client.dataset(target_dataset).table(dest_table)
    job_config.destination = target_ref
    print(target_ref)
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    query_job = client.get_job(query_job.job_id, location=location)
    job_state = query_job.state

    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        print('Error result!! {}'.format(query_job.error_result))
        return False
    return True

'''
----------------------------------------------------------------------------------------------
Use to run queries where we want to get the result back to use (not write into a table)
'''

def bq_harness_with_result(sql, do_batch, verbose=True, project=None):
    """
    Handles all the boilerplate for running a BQ job
    """

    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH
    location = 'US'

    # API request - starts the query
    query_job = client.query(sql, location=location, job_config=job_config)

    # Query
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        query_job = client.get_job(query_job.job_id, location=location)
        if verbose:
            print('Job {} is currently in state {}'.format(query_job.job_id, query_job.state))
        job_state = query_job.state
        if job_state != 'DONE':
            time.sleep(5)
    if verbose:
        print('Job {} is done'.format(query_job.job_id))

    query_job = client.get_job(query_job.job_id, location=location)
    if query_job.error_result is not None:
        if verbose:
            print('Error result!! {}'.format(query_job.error_result))
        return None

    results = query_job.result()

    return results


def upload_to_bucket(target_tsv_bucket, target_tsv_file, local_tsv_file):
    """
    Upload to Google Bucket
    Large files have to be in a bucket for them to be ingested into Big Query. This does this.
    This function is also used to archive files.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(target_tsv_bucket)
    blob = bucket.blob(target_tsv_file)
    print(blob.name)
    blob.upload_from_filename(local_tsv_file)


def csv_to_bq(schema, csv_uri, dataset_id, targ_table, do_batch, project=None):
    """
    Loads a csv file into BigQuery

    :param schema: Dictionary of field name (key) and description (value)
    :type schema: dict
    :param csv_uri: Bucket location of the file in the form of gs://working_bucket/filename.csv
    :type csv_uri: basestring
    :param dataset_id: Name of the dataset where the table will be created
    :type dataset_id: basestring
    :param targ_table: Name of the table to be created
    :type targ_table: basestring
    :param do_batch: Should the BQ job be run in Batch Mode? Slower but uses less quotas
    :type do_batch: bool
    :return: Whether the BQ job was completed
    :rtype: bool
    """
    return csv_to_bq_write_depo(schema, csv_uri, dataset_id, targ_table,
                                do_batch, bigquery.WriteDisposition.WRITE_TRUNCATE, project=project)


def csv_to_bq_write_depo(schema, csv_uri, dataset_id, targ_table, do_batch, write_depo, project=None):
    """
    Loads a csv file into BigQuery with option to specify disposition

    :param schema: Dictionary of field name (key) and description (value)
    :type schema: dict
    :param csv_uri: Bucket location of the file in the form of gs://working_bucket/filename.csv
    :type csv_uri: basestring
    :param dataset_id: Name of the dataset where the table will be created
    :type dataset_id: basestring
    :param targ_table: Name of the table to be created
    :type targ_table: basestring
    :param do_batch: Should the BQ job be run in Batch Mode? Slower but uses less quotas
    :type do_batch: bool
    :param write_depo: Should the table be overwritten or appended?
    :type write_depo: class
    :return: Whether the BQ job was completed
    :rtype: bool
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH

    schema_list = []
    for mydict in schema:
        use_mode = mydict['mode'] if "mode" in mydict else 'NULLABLE'
        schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                mode=use_mode, description=mydict['description']))

    job_config.schema = schema_list
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    if write_depo is not None:
        job_config.write_disposition = write_depo
    # Can make the "CSV" file a TSV file using this:
    job_config.field_delimiter = '\t'

    load_job = client.load_table_from_uri(
        csv_uri,
        dataset_ref.table(targ_table),
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    location = 'US'
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(load_job.job_id, load_job.state))
        job_state = load_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(load_job.job_id))

    load_job = client.get_job(load_job.job_id, location=location)
    if load_job.error_result is not None:
        print('Error result!! {}'.format(load_job.error_result))
        for err in load_job.errors:
            print(err)
        return False

    destination_table = client.get_table(dataset_ref.table(targ_table))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    return True


def concat_all_files(all_files, one_big_tsv, program_prefix, extra_cols, file_info_func, split_more_func):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path, and the extra_cols list maps these to extra column names. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    """
    print("building {}".format(one_big_tsv))
    first = True
    header_id = None
    hdr_line = None
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            toss_zip = False
            if filename.endswith('.zip'):
                dir_name = os.path.dirname(filename)
                print("Unzipping {}".format(filename))
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall(dir_name)
                use_file_name = filename[:-4]
                toss_zip = True
            elif filename.endswith('.gz'):
                dir_name = os.path.dirname(filename)
                use_file_name = filename[:-3]
                print("Uncompressing {}".format(filename))
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename
            if os.path.isfile(use_file_name):
                with open(use_file_name, 'r') as readfile:
                    file_info_list = file_info_func(use_file_name, program_prefix)
                    for line in readfile:
                        if line.startswith('#'):
                            continue
                        split_line = line.rstrip('\n').split("\t")
                        if first:
                            for col in extra_cols:
                                split_line.append(col)
                            header_id = split_line[0]
                            hdr_line = split_line
                            print("Header starts with {}".format(header_id))
                        else:
                            for i in range(len(extra_cols)):
                                split_line.append(file_info_list[i])
                        if not line.startswith(header_id) or first:
                            if split_more_func is not None:
                                split_line = split_more_func(split_line, hdr_line, first)
                            outfile.write('\t'.join(split_line))
                            outfile.write('\n')
                        first = False
            else:
                print('{} was not found'.format(use_file_name))

            if toss_zip and os.path.isfile(use_file_name):
                os.remove(use_file_name)

    return


def update_dir_from_git(local_repo, repo_url, repo_branch):
    """
    This function deletes the old directory and replaces it with the most current from GitHub
    :param local_repo: Where the local directory for the repository is
    :type local_repo: str
    :param repo_url: The URL for the directory to clone
    :type repo_url: str
    :param repo_branch: The branch to use for the repository
    :type repo_branch: str
    :return: Whether the function worked or not
    :rtype: bool
    """
    try:
        create_clean_target(local_repo)
        repo = Repo.clone_from(repo_url, local_repo)
        repo.git.checkout(repo_branch)
        return True
    except Exception as ex:
        print(f"pull_table_info_from_git failed: {str(ex)}")
        return False


def update_schema_tags(metadata_mapping_fp, params, program=None):  # todo docstring
    with open(metadata_mapping_fp, mode='r') as metadata_mapping:
        mappings = json_loads(metadata_mapping.read())

    schema = dict()

    if params['RELEASE']:
        schema['---tag-release---'] = str(params['RELEASE'])

    if params['RELEASE_ANCHOR']:
        schema['---tag-release-url-anchor---'] = str(params['RELEASE_ANCHOR'])

    if params['DATE']:
        schema['---tag-extracted-month-year---'] = params['DATE']

    if program is not None:
        schema['---tag-program---'] = program
        if 'program_label' in mappings[program]:
            schema['---tag-program-name-lower---'] = mappings[program]['program_label']
        else:
            schema['---tag-program-name-lower---'] = None

        if 'program_label_0' in mappings[program]:
            schema['---tag-program-name-lower-0---'] = mappings[program]['program_label_0']
        else:
            schema['---tag-program-name-lower-0---'] = None

        if 'program_label_1' in mappings[program]:
            schema['---tag-program-name-lower-1---'] = mappings[program]['program_label_1']
        else:
            schema['---tag-program-name-lower-1---'] = None

    return schema


def write_table_schema_with_generic(table_id, schema_tags=None, metadata_fp=None,
                                    field_desc_fp=None):  # todo fill in docstring
    """
    Create table metadata schema using generic schema files in BQEcosystem and schema tags defined in yaml config files.
    :param table_id: Table id for which metadata will be added
    :type table_id:
    :param schema_tags: dict of tags to substitute into generic schema file (used for customization)
    :type schema_tags:
    :param metadata_fp:
    :type metadata_fp:
    :param field_desc_fp:
    :type field_desc_fp:
    :return:
    :rtype:
    """

    if metadata_fp is not None:
        write_table_metadata_with_generic(metadata_fp, table_id, schema_tags)

    if field_desc_fp is not None:
        with open(field_desc_fp, mode='r') as field_desc:
            field_desc_dict = json_loads(field_desc.read())
        install_table_field_desc(table_id, field_desc_dict)

    return True


def write_table_metadata_with_generic(metadata_fp, table_id, schema_tags):  # todo fill in docstring
    """
    Updates the tags in the generic schema file then writes the schema to the table metadata in BigQuery.
    This function is an adaption of the add_generic_table_metadata function in utils.py
    :param metadata_fp:
    :type metadata_fp:
    :param table_id:
    :type table_id:
    :param schema_tags:
    :type schema_tags:
    """
    final_table_metadata = {}

    with open(metadata_fp, mode='r') as metadata_dict_file:
        metadata_dict = json_loads(metadata_dict_file.read())

        for main_key, main_value in metadata_dict.items():
            if type(main_value) is dict:
                final_table_metadata[main_key] = {}
                for sub_key, sub_value in metadata_dict[main_key].items():
                    if sub_value[1:4] == "---":
                        if schema_tags[sub_value.strip("{}")]:
                            final_table_metadata[main_key][sub_key] = sub_value.format(**schema_tags)
                        else:
                            print(f"{sub_key} skipped")
                    else:
                        print("no tags")
                        final_table_metadata[main_key][sub_key] = sub_value
            else:
                final_table_metadata[main_key] = main_value.format(**schema_tags)

    install_table_metadata(table_id, final_table_metadata)


def cluster_table(input_table_id, output_table_id, cluster_fields, do_batch):
    cluster_sql = cluster_sql_table(input_table_id, output_table_id, cluster_fields)
    return bq_harness_with_result(cluster_sql, do_batch, True)

def cluster_sql_table(input_table, output_table, cluster_fields):
    cluster_string = ', '.join(cluster_fields)
    sql = f'''
          CREATE TABLE `{output_table}` 
          CLUSTER BY {cluster_string} 
          AS SELECT * FROM `{input_table}`
    '''
    return sql

def build_combined_schema(scraped, augmented, typing_tups, holding_list, holding_dict):
    """
    Merge schema descriptions (if any) and ISB-added descriptions with inferred type data

    :param scraped: JSON file name with scraped table schema
    :type scraped: basestring
    :param augmented: JSON file name of table schema
    :type augmented: basestring
    :param typing_tups: List of tuples with (name, type)
    :type typing_tups: list
    :param holding_list: Filename for where to save a list of field names
    :type holding_list: basestring
    :param holding_dict: Filename for where to save a dictionary of fields and descriptions
    :type holding_dict: basestring
    :return: Whether the function succeeded
    :rtype: bool
    """
    schema_list = []
    if scraped is not None:
        with open(scraped, mode='r') as scraped_hold_list:
            schema_list = json_loads(scraped_hold_list.read())

    augment_list = []
    if augmented is not None:
        with open(augmented, mode='r') as augment_list_file:
            augment_list = json_loads(augment_list_file.read())

    full_schema_dict = {}
    for elem in schema_list:
        full_schema_dict[elem['name']] = elem
    for elem in augment_list:
        full_schema_dict[elem['name']] = elem

    #
    # Need to create two things: A full schema dictionary to update the final
    # table, and a typed list for the initial TSV import:
    #

    typed_schema = []
    for tup in typing_tups:
        if tup[0] in full_schema_dict:
            use_type = tup[1]
            existing = full_schema_dict[tup[0]]
            existing['type'] = use_type
            typed_schema.append(existing)
        else:
            no_desc = {
                "name": tup[0],
                "type": tup[1],
                "description": "No description"
            }
            typed_schema.append(no_desc)
    with open(holding_list, mode='w') as schema_hold_list:
        schema_hold_list.write(json_dumps(typed_schema))
        print("writing schema_hold_list to {}".format(holding_list))

    with open(holding_dict, mode='w') as schema_hold_dict:
        schema_hold_dict.write(json_dumps(full_schema_dict))
        print("writing schema_hold_dict to {}".format(holding_dict))

    return True


def typing_tups_to_schema_list(typing_tups, holding_list):
    """
    Need to create a typed list for the initial TSV import

    :param typing_tups: List of tuples with (name, type)
    :type typing_tups: list
    :param holding_list: Filename for where to save a list of field names
    :type holding_list: basestring
    :return: Whether the function succeeded
    :rtype: bool
    """
    typed_schema = []
    for tup in typing_tups:
        no_desc = {
            "name": tup[0],
            "type": tup[1],
            "description": "No description"
        }
        typed_schema.append(no_desc)
    with open(holding_list, mode='w') as schema_hold_list:
        schema_hold_list.write(json_dumps(typed_schema))

    return True


def create_schema_hold_list(typing_tups, field_schema, holding_list, static=True):  # todo docstrings

    with open(field_schema, mode='r') as field_schema_file:
        all_field_schema = json_loads(field_schema_file.read())

    typed_schema = []
    for tup in typing_tups:
        print(tup)
        field_dict = all_field_schema[tup[0]]
        if tup[1][0:4] != field_dict["type"][0:4]:
            print(f"{tup[0]} types do not match.")
            print(f"Dynamic type ({tup[1]}) does not equal static type ({field_dict['type']})")

        if field_dict["exception"] == "":
            if static:
                print(f"\tsetting type to static type {field_dict['type']}")
                tup = (tup[0], field_dict["type"])
                # tup[1] = str(field_dict["type"])
            else:
                print(f"\tsetting type to dynamic type ({tup[1]})")

        if field_dict["description"]:
            full_field_dict = {
                "name": tup[0],
                "type": tup[1],
                "description": field_dict["description"]
            }
            typed_schema.append(full_field_dict)
        else:
            print(f"{tup[0]} field description needs to be updated separately.")
            no_desc = {
                "name": tup[0],
                "type": tup[1],
                "description": "No description"
            }
            typed_schema.append(no_desc)

    with open(holding_list, mode='w') as schema_hold_list:
        schema_hold_list.write(json_dumps(typed_schema))

    return True


def update_schema(target_dataset, dest_table, schema_dict_loc):
    """
    Update the Schema of a Table
    Final derived table needs the schema descriptions to be installed.

    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param schema_dict_loc: Filename for where to dictionary of fields and descriptions is saved
    :type schema_dict_loc: basestring
    :return: Whether the function succeeded
    :rtype: bool
    """
    try:
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema = json_loads(schema_hold_dict.read())

        success = update_schema_with_dict(target_dataset, dest_table, full_schema)
        if not success:
            return False
        return True
    except Exception as ex:
        print(ex)
        return False

def retrieve_table_properties(target_dataset, dest_table, project=None):
    """
    retrieves BQ table metadata
    :param target_dataset:
    :type target_dataset:
    :param dest_table:
    :type dest_table:
    :param project:
    :type project:
    :return:
    :rtype:
    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_ref = client.dataset(target_dataset).table(dest_table)
        table_metadata = client.get_table(table_ref)
        return table_metadata
    except Exception as ex:
        print(ex)
        return False

def retrieve_table_schema(target_dataset, dest_table, project=None):
    """
    retrieve a schema from a table

    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param project: Project name
    :type project: basestring
    :return: Table schema
    :rtype: dict
    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_ref = client.dataset(target_dataset).table(dest_table)
        table = client.get_table(table_ref)
        return table.schema
    except Exception as ex:
        print(ex)
        return False


def update_table_schema(schema, add_dict):
    """
    Combine original field schema to a new dictionary of field schema

    :param schema: Original table field schema
    :type schema: dict
    :param add_dict: Dictionary of field schema to add to the original field schema
    :type add_dict: dict
    :return: A combined schema with the original and added field schema
    :rtype: dict
    """
    schema_dict = {field.name: field for field in schema}
    for key in add_dict:
        schema_dict[key] = bigquery.SchemaField(key, add_dict[key]['type'], u'NULLABLE', add_dict[key]['desc'])
    updated_schema = [schema_dict[key] for key in schema_dict]
    return updated_schema


def write_schema_to_table(target_dataset, dest_table, new_schema, project=None):
    """
    Update field schema on table

    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param new_schema: Dictionary of field schema to update table with
    :type new_schema: dict
    :param project: Project name
    :type project: basestring
    :return: Whether the function succeeded
    :rtype: bool
    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_ref = client.dataset(target_dataset).table(dest_table)
        table = client.get_table(table_ref)
        table.schema = new_schema
        table = client.update_table(table, ["schema"])
        return True
    except Exception as ex:
        print(ex)
        return False


def update_schema_with_dict(target_dataset, dest_table, full_schema, project=None):
    """
    Update the Schema of a Table

    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param full_schema: Dictionary of Table Schema
    :type full_schema: dict
    :param project: Project name
    :type project: basestring
    :return: Whether the function worked
    :rtype: bool
    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_ref = client.dataset(target_dataset).table(dest_table)
        table = client.get_table(table_ref)
        orig_schema = table.schema
        new_schema = []
        for old_sf in orig_schema:
            new_desc = full_schema[old_sf.name]['description']
            new_sf = bigquery.SchemaField(old_sf.name, old_sf.field_type, description=new_desc)
            new_schema.append(new_sf)
        table.schema = new_schema
        table = client.update_table(table, ["schema"])
    except Exception as ex:
        print(ex)
        return False

    return True


def update_description(target_dataset, dest_table, desc, project=None):
    """
    Update the description of a table

    :param project: Project setting for BQ Client
    :type dest_table: basestring
    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param desc: Description to update table with
    :type desc: basestring
    :return: Whether the function succeeded
    :rtype: bool
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    table = client.get_table(table_ref)
    table.description = desc
    table = client.update_table(table, ["description"])
    return True


def update_status_tag(target_dataset, dest_table, status, project=None):
    """
    Update the status tag of a big query table once a new version of the table has been created

    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param status: The value you want to change the table label to
    :type status: basestring
    :param project: Project name
    :type project: basestring
    :return: Whether the function works
    :rtype: bool
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    table = client.get_table(table_ref)
    table.labels = {"status": status}
    table = client.update_table(table, ["labels"])
    return True


def bq_table_exists(target_dataset, dest_table, project=None):
    """
    Does table exist?
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def bq_table_is_empty(target_dataset, dest_table, project=None):
    """
    Is table empty?
    """
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    table = client.get_table(table_ref)
    return table.num_rows == 0


def delete_table_bq_job(target_dataset, delete_table, project=None):
    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(delete_table)
    try:
        client.delete_table(table_ref)
        print('Table {}:{} deleted'.format(target_dataset, delete_table))
    except exceptions.NotFound as ex:
        print('Table {}:{} was not present'.format(target_dataset, delete_table))
    except Exception as ex:
        print(ex)
        return False

    return True


def confirm_google_vm():
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/id"
    meta_header = {"Metadata-Flavor": "Google"}

    try:
        resp = requests.request("GET", metadata_url, headers=meta_header)
    except Exception as ex:
        print("Not a Google VM: {}".format(ex))
        return False

    if resp.status_code == 200:
        return True
    else:
        print("Not a Google VM: {}".format(resp.status_code))
        return False


def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Ripped from Stack Overflow.
    https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    H/T to Greenstick: https://stackoverflow.com/users/2206251/greenstick

    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()
    return


def transfer_schema(target_dataset, dest_table, source_dataset, source_table, project=None):
    """
    Transfer description of schema from e.g. table to view

    Note, to get this working on a fresh Google VM, I do this:

    sudo apt-get install python3-venv
    python3 -m venv bqEnv
    source bqEnv/bin/activate
    python3 -m pip install google-api-python-client
    python3 -m pip install google-cloud-storage
    python3 -m pip install google-cloud-bigquery

    TDS = "view_data_set"
    DTAB = "view_name"
    SDS = "table_data_set"
    STAB = "table_name"
    transfer_schema(TDS, DTAB, SDS, STAB)

    :param target_dataset: Dataset name of the view to be updated
    :type target_dataset: basestring
    :param dest_table: Name of the view to be updated
    :type dest_table: basestring
    :param source_dataset: Dataset name of where the schema is coming from
    :type source_dataset: basestring
    :param source_table: Table name of where the schema is coming from
    :type source_table: basestring
    :return: Whether the function worked
    :rtype: bool

    """

    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    src_table_ref = client.dataset(source_dataset).table(source_table)
    trg_table_ref = client.dataset(target_dataset).table(dest_table)
    src_table = client.get_table(src_table_ref)
    trg_table = client.get_table(trg_table_ref)
    src_schema = src_table.schema
    trg_schema = []
    for src_sf in src_schema:
        trg_sf = bigquery.SchemaField(src_sf.name, src_sf.field_type, description=src_sf.description)
        trg_schema.append(trg_sf)
    trg_table.schema = trg_schema
    client.update_table(trg_table, ["schema"])
    return True

def list_schema(source_dataset, source_table, project=None):
    """
    List schema

    :param source_dataset: Dataset name
    :type source_dataset: basestring
    :param source_table: Table name
    :type source_table: basestring
    :return: whether the function worked
    :rtype: bool
    """

    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    src_table_ref = client.dataset(source_dataset).table(source_table)
    src_table = client.get_table(src_table_ref)
    src_schema = src_table.schema
    for src_sf in src_schema:
        print(src_sf.name, src_sf.field_type, src_sf.description)
    return True


'''
----------------------------------------------------------------------------------------------
Take the BQ Ecosystem json file for the table and break out the pieces into chunks that will
be arguments to the bq command used to create the table.
'''


def generate_table_detail_files(dict_file, file_tag):
    #
    # Read in the chunks and write them out into pieces the bq command can use
    #

    try:
        with open(dict_file, mode='r') as bqt_dict_file:
            bqt_dict = json_loads(bqt_dict_file.read())
        with open("{}_desc.txt".format(file_tag), mode='w+') as desc_file:
            desc_file.write(bqt_dict['description'])
        with open("{}_labels.json".format(file_tag), mode='w+') as label_file:
            label_file.write(json_dumps(bqt_dict['labels'], sort_keys=True, indent=4, separators=(',', ': ')))
            label_file.write('\n')
        with open("{}_schema.json".format(file_tag), mode='w+') as schema_file:
            schema_file.write(
                json_dumps(bqt_dict['schema']['fields'], sort_keys=True, indent=4, separators=(',', ': ')))
            schema_file.write('\n')
        with open("{}_friendly.txt".format(file_tag), mode='w+') as friendly_file:
            friendly_file.write(bqt_dict['friendlyName'])

    except Exception as ex:
        print(ex)
        return False

    return True


'''
----------------------------------------------------------------------------------------------
Take the staging files for a generic BQ metadata load and customize it for a single data set
using tags.
'''


def customize_labels_and_desc(file_tag, tag_map_list):
    """
    Updates schema files to fill in dynamic variables within the json files

    :param file_tag: File prefix for the workflow
    :type file_tag: basestring
    :param tag_map_list: List of tags to values
    :type tag_map_list: list
    :return: Whether the function worked
    :rtype: bool
    """
    try:
        with open("{}_desc.txt".format(file_tag), mode='r') as desc_file:
            desc = desc_file.read()
        with open("{}_labels.json".format(file_tag), mode='r') as label_file:
            labels = label_file.read()
        with open("{}_friendly.txt".format(file_tag), mode='r') as friendly_file:
            friendly = friendly_file.read()
        with open("{}_schema.json".format(file_tag), mode='r') as schema_file:
            schema = schema_file.read()

        for tag_val in tag_map_list:
            for tag in tag_val:
                brack_tag = '{{{}}}'.format(tag)
                desc = desc.replace(brack_tag, tag_val[tag])
                labels = labels.replace(brack_tag, tag_val[tag])
                friendly = friendly.replace(brack_tag, tag_val[tag])
                schema = schema.replace(brack_tag, tag_val[tag])

        with open("{}_desc.txt".format(file_tag), mode='w+') as desc_file:
            desc_file.write(desc)
        with open("{}_labels.json".format(file_tag), mode='w+') as label_file:
            label_file.write(labels)
        with open("{}_schema.json".format(file_tag), mode='w+') as schema_file:
            schema_file.write(schema)
        with open("{}_friendly.txt".format(file_tag), mode='w+') as friendly_file:
            friendly_file.write(friendly)

    except Exception as ex:
        print(ex)
        return False

    return True


'''
----------------------------------------------------------------------------------------------
Take the labels and description of a BQ table and get them installed
'''


def install_labels_and_desc(dataset, table_name, file_tag, project=None):
    """
    Update table schema

    :param dataset: Dataset Name
    :type dataset: basestring
    :param table_name: Table Name
    :type table_name: basestring
    :param file_tag: File prefix for the workflow
    :type file_tag: basestring
    :param project: Project Name
    :type project: basestring
    :return: Whether the function worked
    :rtype: bool
    """
    try:
        with open("{}_desc.txt".format(file_tag), mode='r') as desc_file:
            desc = desc_file.read()

        with open("{}_labels.json".format(file_tag), mode='r') as label_file:
            labels = json_loads(label_file.read())

        with open("{}_friendly.txt".format(file_tag), mode='r') as friendly_file:
            friendly = friendly_file.read()

        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_ref = client.dataset(dataset).table(table_name)
        table = client.get_table(table_ref)

        #
        # Noted 3/16/2020 that updating labels appears to be additive. Need to clear out
        # previous labels to handle label removals. Note that the setting of each existing label
        # to None is the only way this seems to work to empty them out (i.e. an empty dictionary
        # does not cut it).
        #

        replace_dict = {}
        for label in table.labels:
            replace_dict[label] = None
        table.description = None
        table.labels = replace_dict
        table.friendly_name = None
        client.update_table(table, ['description', 'labels', 'friendlyName'])
        table_ref = client.dataset(dataset).table(table_name)
        table = client.get_table(table_ref)
        table.description = desc
        table.labels = labels
        table.friendly_name = friendly
        client.update_table(table, ['description', 'labels', 'friendlyName'])

    except Exception as ex:
        print(ex)
        return False

    return True


def install_table_metadata(table_id, metadata):
    """
    Modify an existing BigQuery table's metadata (labels, friendly name, description) using metadata dict argument
    Function was adapted from update_table_metadata function in utils.py
    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']
    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']


def install_table_field_desc(table_id, new_descriptions):
    """
    Modify an existing table's field descriptions. Based on a function from utils.py called update_schema
    Function adapted from update_schema in utils.py
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    new_schema = []

    for schema_field in table.schema:
        field = schema_field.to_api_repr()
        name = field['name']

        if name in new_descriptions.keys() and new_descriptions[name]['exception'] == '':
            field['description'] = new_descriptions[name]['description']
        elif name in new_descriptions.keys() and new_descriptions[name]['exception'] is not None:
            print(f"Field {name} has an exception listed: {new_descriptions[name]['exception']}")
        else:
            print(f"{name} field is not listed in json")

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def find_most_recent_release(dataset, base_table, project=None):
    """

    This function iterates though all tables of a BigQuery versioned dataset to find the most recent release of version
    number of a certain data type.

    :param dataset: Dataset to search
    :type dataset: basestring
    :param base_table: The table name before the release number (must include _ before release number)
    :type base_table: basestring
    :param project: Which project is the data set in?
    :type project: basestring

    :returns: The highest version number of that table type in that dataset as a string
    """
    print('finding most recent release in ' + dataset)
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        release = ''  # variable for the most recent release
        table_create = ''  # the most recently created table
        # subset the tables for those that match the desired one
        table_subset = [t for t in client.list_tables(dataset) if t.table_id[:len(base_table)] == base_table]
        if not table_subset:
            print('No older versions to compare to')
            return False
        for t in table_subset:
            # If the table has a newer create date then the one in table_create date, replace current value
            if table_create < str(t.created):
                table_create = str(t.created)
                release = t.table_id[len(base_table):]
    except Exception as ex:
        print(ex)
        return False

    return release


'''
----------------------------------------------------------------------------------------------
Take the BQ Ecosystem json file for a dataset and break out the pieces into chunks that will
be arguments to the bq command used to update the dataset.
'''


def generate_dataset_desc_file(dict_file, file_tag):
    """
    Read in the chunks and write them out into pieces the bq command can use
    :param dict_file: Schema Json file
    :type dict_file: basestring
    :param file_tag: File prefix for the workflow
    :type file_tag: basestring
    :return: Whether the function worked
    :rtype: bool
    """

    try:
        with open(dict_file, mode='r') as bqt_dict_file:
            bqt_dict = json_loads(bqt_dict_file.read())
        with open("{}_desc.txt".format(file_tag), mode='w+') as desc_file:
            desc_file.write(bqt_dict['description'])

    except Exception as ex:
        print(ex)
        return False

    return True


'''
----------------------------------------------------------------------------------------------
Take the description of a BQ dataset and get it installed
'''


def install_dataset_desc(dataset_id, file_tag, project=None):
    """
    Update Dataset Description

    :param dataset_id: Dataset name
    :type dataset_id: basestring
    :param file_tag: Json schema file
    :type file_tag: basestring
    :param project: Project name
    :type project: basestring
    :return: Whether the function worked
    :rtype: bool
    """
    try:
        with open("{}_desc.txt".format(file_tag), mode='r') as desc_file:
            desc = desc_file.read()

        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        dataset = client.get_dataset(dataset_id)  # Make an API request.
        dataset.description = desc
        client.update_dataset(dataset, ["description"])

    except Exception as ex:
        print(ex)
        return False

    return True


'''
----------------------------------------------------------------------------------------------
Create a new BQ dataset
'''


def create_bq_dataset(dataset_id, file_tag, project=None, make_public=False):
    try:
        with open("{}_desc.txt".format(file_tag), mode='r') as desc_file:
            desc = desc_file.read()

        client = bigquery.Client() if project is None else bigquery.Client(project=project)

        full_dataset_id = "{}.{}".format(client.project, dataset_id)

        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = "US"
        dataset.description = desc

        if make_public:
            entry = bigquery.AccessEntry(
                role="READER",
                entity_type="specialGroup",
                entity_id="allAuthenticatedUsers",
            )

            entries = list(dataset.access_entries)
            entries.append(entry)
            dataset.access_entries = entries

        client.create_dataset(dataset)

    except Exception as ex:
        print(ex)
        return False

    return True


'''
----------------------------------------------------------------------------------------------
Publish a table by copying it.
Args of form: <source_table_proj.dataset.table> <dest_table_proj.dataset.table>
'''


def publish_table(source_table, target_table, overwrite=False):

    try:
        #
        # 3/11/20: Friendly names not copied across, so do it here!
        #

        src_toks = source_table.split('.')
        src_proj = src_toks[0]
        src_dset = src_toks[1]
        src_tab = src_toks[2]

        trg_toks = target_table.split('.')
        trg_proj = trg_toks[0]
        trg_dset = trg_toks[1]
        trg_tab = trg_toks[2]

        src_client = bigquery.Client(src_proj)

        job_config = bigquery.CopyJobConfig()
        if overwrite == True: job_config.write_disposition = "WRITE_TRUNCATE"
        job = src_client.copy_table(source_table, target_table, job_config=job_config)
        job.result()

        src_table_ref = src_client.dataset(src_dset).table(src_tab)
        s_table = src_client.get_table(src_table_ref)
        src_friendly = s_table.friendly_name

        trg_client = bigquery.Client(trg_proj)
        trg_table_ref = trg_client.dataset(trg_dset).table(trg_tab)
        t_table = src_client.get_table(trg_table_ref)
        t_table.friendly_name = src_friendly

        trg_client.update_table(t_table, ['friendlyName'])

    except Exception as ex:
        print(ex)
        return False

    return True

'''
----------------------------------------------------------------------------------------------
Are two tables exactly the same?
'''


def compare_two_tables(old_table, new_table, do_batch):
    old_table_spl, new_table_spl = old_table.split('.'), new_table.split('.')

    old_schema = retrieve_table_schema(old_table_spl[1], old_table_spl[2], old_table_spl[0])
    new_schema = retrieve_table_schema(new_table_spl[1], new_table_spl[2], new_table_spl[0])

    if len(old_schema) != len(new_schema):
        return 'Number of fields do not match'
    sql = compare_two_tables_sql(old_table, new_table)
    return bq_harness_with_result(sql, do_batch)


'''
----------------------------------------------------------------------------------------------
SQL for the compare_two_tables function
'''


def compare_two_tables_sql(old_table, new_table):
    return '''
        (
            SELECT * FROM `{0}`
            EXCEPT DISTINCT
            SELECT * from `{1}`
        )
        UNION ALL
        (
            SELECT * FROM `{1}`
            EXCEPT DISTINCT
            SELECT * from `{0}`
        )
    '''.format(old_table, new_table)


def evaluate_table_union(bq_results):
    """Evaluate whether two tables are identical by 
    using the count of distinct rows in their union
    return True/False"""
    if not bq_results:
        print('Table comparison failed')
        return Exception
    if bq_results == 'Number of fields do not match':
        print(bq_results)
        return 'different'
    row_difference = bq_results.total_rows
    if row_difference == 0:
        print('The tables are identical')
        return 'identical'
    else:
        print('The tables differ by {} rows'.format(row_difference))
        return 'different'


def remove_old_current_tables(old_current_table, previous_ver_table, table_temp, do_batch):
    project, dataset, table = old_current_table.split('.')
    compare = compare_two_tables(old_current_table, previous_ver_table, do_batch)
    if compare is not None:
        # Evaluate the two tables
        evaluate_compare = evaluate_table_union(compare)

        if not compare:
            print('compare_tables failed')
            return False
        # move old table to a temporary location
        elif compare and evaluate_compare == 'identical':
            print('Move old table to temp location')
            table_moved = publish_table(old_current_table, table_temp)

            if not table_moved:
                print('Old Table was not moved and will not be deleted')
                return False
            # remove old table
            elif table_moved:
                print('Deleting old table: {}'.format(old_current_table))
                delete_table = delete_table_bq_job(dataset, table.format('current'), project)

                if not delete_table:
                    print('delete table failed')
                    return False
    else:
        print('no previous table available for this data type')
        return False

    return True


def publish_tables_and_update_schema(scratch_table_id, versioned_table_id, current_table_id, release_friendly_name,
                                     base_table=None):

    # publish current and update old versioned
    if base_table:
        project, dataset, curr_table = current_table_id.split('.')

        # find the most recent table
        most_recent_release = find_most_recent_release(f"{dataset}_versioned", base_table, project)
        if most_recent_release:
            update_status_tag(f"{dataset}_versioned", f"{base_table}{most_recent_release}", "archived", project)
            # create or update current table and update older versioned tables
            if bq_table_exists(dataset, f"{curr_table}", project):
                print(f"Deleting old table: {current_table_id}")
                if not delete_table_bq_job(dataset, f"{curr_table}", project):
                    sys.exit(f'deleting old current table called {current_table_id} failed')

        print("Creating new current table")
        if not publish_table(scratch_table_id, current_table_id):
            sys.exit(f'creating a new current table called {current_table_id} failed')



    # publish versioned
    print(f"publishing {versioned_table_id}")
    if publish_table(scratch_table_id, versioned_table_id):
        print("Updating friendly name")
        client = bigquery.Client()
        table = client.get_table(versioned_table_id)
        friendly_name = table.friendly_name
        table.friendly_name = f"{friendly_name} {release_friendly_name} VERSIONED"
        client.update_table(table, ["friendly_name"])
    else:
        sys.exit(f'versioned publication failed for {versioned_table_id}')

    return True

'''
QC of BQ tables
'''

def qc_bq_table_metadata(table_id):
    project, dataset, table = table_id.split(".")

    table_properties = retrieve_table_properties(dataset, table, project)

    qc_string = f"""------------------
    QC for BigQuery Table: {table_id}
    \tCreated on {table_properties.created}
    \tNumber of Rows: {table_properties.num_rows}
    \tTable Friendly Name: {table_properties.friendly_name}
    \tTable Description: \n\t\t{table_properties.description}
    \tTable Tags:\n"""
    for label_name, label in table_properties.labels.items():
        qc_string = qc_string + f"\t\t{label_name}: {label}\n"

    return qc_string

def qc_bq_table_counts(fields, table_id):
    print(fields, table_id)

def sql_count_distinct_field(fields, table_id):

    formatted_fields = [f"COUNT(DISTINCT({field}))" for field in fields]

    sql_query= f"""
    SELECT {", ".join(formatted_fields)} 
    FROM `{table_id}` 
    """

    return sql_query
