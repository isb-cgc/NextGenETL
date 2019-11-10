import argparse
import json
import requests
import sys
import time
import uuid

from pandas.io.json import json_normalize


# Copyright 2019, Institute for Systems Biology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

## Note: can get a nice mapping from the endpoint like this:
##       curl 'https://gdc-api.nci.nih.gov/cases/_mapping'
##
##  cases_fields:  aliquot_ids analyte_ids case_id created_datetime portion_ids
##                 sample_ids slide_ids state submitter_aliquot_ids submitter_analyte_ids
##                 submitter_id submitter_portion_ids submitter_sample_ids updated_datetime
##
##  files_fields:  access acl created_datetime data_category data_format data_type
##                 error_type experimental_strategy file_id file_name file_size file_state
##                 md5sum platform state state_comment submitter_id tags type updated_datetime
##
## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

files_fields = []

## the higher this flag is set, the more larger the output log file
## (assuming the output of this program is dumped to a file)
#verboseFlag = 99
#verboseFlag = 14
# WJRL 11/10/19 NOW AN ARGUMENT!
#verboseFlag = 0 # WJRL 11/8/19

uuidStr = uuid.uuid1().hex[:8]

numFiles = 0


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_all_file_ids_from_file(fh):
    fileID_list = {}

    for aLine in fh:
        tokens = aLine.strip().split('\t')
        if (tokens[0] != 'id'):
            file_id = tokens[0]
            file_name = tokens[1]
            ## IMPORTANT: we need to ignore .bai and .tbi files because
            ## they are not queryable based on their GUIDs...
            if (file_name.endswith('.bai') or file_name.endswith('.tbi')): continue
            if (len(file_id) != 36):
                print
                " invalid file identifier ??? ", len(file_id), file_id
                sys.exit(-1)
            if (file_id not in fileID_list):
                fileID_list[file_id] = 1
            else:
                if (verboseFlag >= 3):
                    print
                    " already have this one in list ?!?! ", len(fileID_list), file_id

    if (verboseFlag >= 1): print
    " returning map with %d file ids " % len(fileID_list)

    return (fileID_list)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
## this function simply asks the "files" endpt for the "file_id" for ALL files
## using a page size of 1000 and continuing to request the next 1000 files
## (sorted by file_id) until they've all been retreived

def get_all_file_ids(files_endpt):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in get_all_file_ids ... ", files_endpt
        print
        " "

    maxSize = 1000
    fromStart = 0
    done = 0

    fileID_list = {}

    while not done:

        params = {'fields': 'file_id',
                  'sort': 'file_id:asc',
                  'from': fromStart,
                  'size': maxSize}

        try:
            if (verboseFlag >= 9): print
            " get request ", files_endpt, params
            response = requests.get(files_endpt, params=params, timeout=10.0)
        except:
            print
            " ERROR !!! requests.get() call FAILED ??? (a) "
            continue

        ## check response status_code
        try:
            if (verboseFlag >= 3): print
            " response status_code : ", response.status_code
            if (response.status_code != 200):
                print
                " --> BAD status_code returned !!! ", response.status_code
                continue
        except:
            print
            " ERROR just in looking for status_code ??? !!! "
            continue

        try:
            if (verboseFlag >= 9): print
            " now parsing json response ... "
            rj = response.json()
            if (verboseFlag >= 9): print
            json.dumps(rj, indent=4)
        except:
            print
            " failed to get information about files ??? "
            continue

        try:

            if (verboseFlag >= 3): print
            " now parsing info returned ... "

            iCount = rj['data']['pagination']['count']
            iFrom = rj['data']['pagination']['from']
            iPages = rj['data']['pagination']['pages']
            iTotal = rj['data']['pagination']['total']
            iSize = rj['data']['pagination']['size']
            if (verboseFlag >= 3): print
            iCount, iFrom, iPages, iTotal, iSize

            fromStart += iCount
            if (iCount == 0):
                if (verboseFlag >= 2): print
                " got nothing back ... (?) "
                done = 1

            for ii in range(iCount):
                if (verboseFlag >= 9): print
                ii, rj['data']['hits'][ii]
                file_id = rj['data']['hits'][ii]['file_id']
                if (verboseFlag >= 9): print
                file_id
                if (file_id not in fileID_list):
                    fileID_list[file_id] = 1
                else:
                    if (verboseFlag >= 3):
                        print
                        " already have this one in list ?!?! ", ii, iCount, file_id

            if (verboseFlag >= 1): print
            "         ", len(fileID_list)

        except:
            print
            " "
            print
            " --> setting DONE to TRUE now ... is this OK ??? "
            print
            " "
            done = 1

            ## if ( len(fileID_list) > 2000 ): done=1

    if (verboseFlag >= 1): print
    " returning map with %d file ids " % len(fileID_list)

    return (fileID_list)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def getFileInfo(fileID_list, files_endpt, dbName, file_fh2):
    global numFiles

    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in getFileInfo ... ", len(fileID_list), files_endpt
        print
        " "

    allFiles = fileID_list.keys()
    allFiles.sort()

    fileID_dict = {}

    ## outer loop is over ALL files ...
    for file_id in allFiles:

        if (verboseFlag >= 1):
            print
            " "
            print
            " "
            print
            " in getFileInfo ... looping over allFiles ... ", file_id

        ## get file info for this one file ...
        fileInfoVec = get_file_info(files_endpt, file_id)
        numFiles2 = len(fileInfoVec)
        if (verboseFlag >= 1):
            print
            " --> got back INFORMATION for %d files " % numFiles2

        numFiles += numFiles2

        for ii in range(numFiles2):

            fileInfo = fileInfoVec[ii]
            fileInfo = stripBlanks(fileInfo)
            if (len(fileInfo['file_id']) != 1):
                print
                " ERROR ??? HOW CAN THIS BE ??? NO file_id ??? or TOO MANY ??? "
                print
                fileInfo['file_id']
                print
                fileInfo
                print
                " FATAL ERROR in getFileInfo "
                sys.exit(-1)

            file_id = fileInfo['file_id'][0]
            ## print file_id
            ## print fileID_dict

            if (file_id not in fileID_dict):
                fileID_dict[file_id] = [fileInfo]
                writeOneFile4BQ(file_fh2, dbName, file_id, fileInfo)
                if (verboseFlag >= 3):
                    print
                    file_id
                    print
                    " creating fileID_dict entry for this file ", file_id
                    print
                    fileID_dict[file_id]
            else:

                if (verboseFlag >= 3):
                    print
                    " already know about this file ... no worries ... ", file_id

        if (verboseFlag >= 1):
            if (numFiles % 100 == 0):
                print
                "     working ... in getFileInfo ... %d " % (numFiles)

    if (verboseFlag >= 1):
        print
        " "
        print
        " returning dict with %d files " % (len(fileID_dict))
        print
        " "

    return (fileID_dict)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def stripBlanks(inD):
    newD = {}
    for aKey in inD.keys():
        bKey = aKey.strip()
        newD[bKey] = []
        for aItem in inD[aKey]:
            try:
                bItem = aItem.strip()
                newD[bKey] += [bItem]
            except:
                newD[bKey] += [aItem]

    if (newD != inD):
        print
        " stripBlanks made a difference! "

    return (newD)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def flattenJSON(inJ):
    if (verboseFlag >= 77):
        print
        " >>> in flattenJSON ... "
        print
        json.dumps(inJ, indent=4)

    outJ = {}

    def flatten(x, name=''):
        ## print " in flatten ... name=<%s> " % name
        if type(x) is dict:
            for a in x:
                ## print "     dict handling a=<%s> " % a
                flatten(x[a], name + a + '__')
        elif type(x) is list:
            i = 0
            for a in x:
                ## print "     list handling a=<%s> " % a
                flatten(a, name)
                i += 1
        else:
            if (x is not None):
                ## print "     leaf handling name=<%s> " % name
                if (name[:-2] in outJ):
                    outJ[name[:-2]] += [x]
                else:
                    outJ[name[:-2]] = [x]

    flatten(inJ)

    if (verboseFlag >= 77):
        print
        " "
        print
        " how does this look ??? "
        print
        " "
        print
        json.dumps(outJ, indent=4)

    return (outJ)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_file_info(files_endpt, file_id):
    if (verboseFlag >= 3):
        print
        " "
        print
        " >>> in get_file_info ... ", files_endpt, file_id
        print
        " "

    global files_fields

    filt = {'op': '=',
            'content': {
                'field': 'file_id',
                'value': [file_id]}}

    ## 3/29/19: removing file_state from list below:
    fieldsList = 'access,acl,created_datetime,data_category,data_format,' \
                 + 'cases.case_id,' \
                 + 'data_type,error_type,experimental_strategy,file_id,' \
                 + 'file_name,file_size,md5sum,origin,platform,' \
                 + 'revision,state,state_comment,submitter_id,tags,type,' \
                 + 'updated_datetime,' \
                 + 'index_files.file_id,index_files.file_name,index_files.file_size,' \
                 + 'metadata_files.file_id,metadata_files.file_name,metadata_files.file_size,metadata_files.type,' \
                 + 'analysis.workflow_type,analysis.workflow_link,' \
                 + 'analysis.input_files.file_id,' \
                 + 'associated_entities.case_id,associated_entities.entity_id,' \
                 + 'associated_entities.entity_submitter_id,' \
                 + 'associated_entities.entity_type,' \
                 + 'center.center_type,center.code,center.name,center.short_name,' \
                 + 'downstream_analyses.workflow_type,downstream_analyses.workflow_link,' \
                 + 'downstream_analyses.output_files.file_id,' \
                 + 'cases.project.dbgap_accession_number,cases.project.disease_type,cases.project.name,cases.project.project_id,' \
                 + 'cases.project.program.dbgap_accession_number,cases.project.program.name,' \
                 + 'archive.archive_id,archive.revision,archive.state,archive.submitter_id'

    ## print " fieldsList: <%s> " % fieldsList

    ## IMPORTANT: if we want to get information about index_files, we would
    ## need to add the following to this params dict:
    ##         'expand': 'index_files'
    ## but we are NOT going to do that at this time (04/04/2019)

    params = {'fields': fieldsList,
              'filters': json.dumps(filt),
              'sort': 'file_id:asc',
              'from': 0,
              'size': 2000}

    if (verboseFlag >= 9): print
    " get request ", files_endpt, params

    iTry = 0
    sleepTime = 0.1

    maxNumTry = 100
    maxNumTry = 10
    ## on a 3/28 run for active DR16 this was the retry pattern I saw:
    ##      442  >>>> trying again ...  2 0.1
    ##       10  >>>> trying again ...  3 0.11
    ##        2  >>>> trying again ...  4 0.121
    ##        1  >>>> trying again ...  5 0.1331`

    while (iTry < maxNumTry):
        if (iTry > 0):
            print
            " >>>> trying again ... ", iTry + 1, sleepTime
            time.sleep(sleepTime)
            sleepTime = sleepTime * 1.1
        iTry += 1

        try:
            if (verboseFlag >= 9): print
            " get request ", files_endpt, params
            response = requests.get(files_endpt, params=params, timeout=10.0)
        except:
            print
            " ERROR !!! requests.get() call FAILED ??? (c) "
            continue

        ## check response status_code
        try:
            if (verboseFlag >= 3): print
            " response status_code : ", response.status_code
            if (response.status_code != 200):
                print
                " --> BAD status_code returned !!! ", response.status_code
                continue
        except:
            print
            " ERROR just in looking for status_code ??? !!! "
            continue

        try:
            if (verboseFlag >= 9): print
            " now parsing json response ... "
            rj = response.json()

            numFiles = len(rj['data']['hits'])
            if (verboseFlag >= 9):
                print
                "     --> got back information for %d files " % numFiles
            if (verboseFlag >= 13):
                print
                json.dumps(rj, indent=4)

            if (numFiles != 1):
                print
                " ERROR ??? did not get back all of the files we were expecting ??? !!! "
                print
                "     expecting 1 ... got back %d " % (numFiles)
                sys.exit(-1)

            if (numFiles != 1):
                print
                " ERROR ??? number of files does not match expected number ??? (%d,1) " % (numFiles)

            ## if the number of files we get back is greater than or equal to
            ## what's expected, let's just go with it ...
            if (numFiles >= 1):

                fileInfoVec = []

                for ii in range(numFiles):

                    fileInfo = rj['data']['hits'][ii]
                    fileInfo = flattenJSON(fileInfo)

                    fileInfoVec += [fileInfo]

                    fields = fileInfo.keys()
                    fields.sort()

                    for aField in fields:
                        if (aField not in files_fields):
                            if (verboseFlag >= 1):
                                print
                                " adding new field to files_fields list : ", aField
                            files_fields += [aField]

                return (fileInfoVec)

                ## otherwise let's go back and try again ...

        except:
            print
            " ERROR in get_file_info ??? failed to get any information about this file ??? ", file_id

            ## go back and try again ...

    ## returning EMPTY HANDED ???
    if (verboseFlag >= 1):
        print
        " "
        print
        " --> returning EMPTY HANDED from get_file_info ??? ERROR ??? ", file_id
        print
        " "

    return ({})

    ## sys.exit(-1)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_fileID_list(caseInfo):
    file_count = 0
    fileID_list = []

    try:
        file_count = caseInfo["summary__file_count"][0]
        fileID_list = caseInfo["files__file_id"]
    except:
        pass

    if (len(fileID_list) != file_count):
        print
        " WARNING ??? !!! the number of file IDs returned is not as expected ??? ", file_count, len(fileID_list)
        print
        json.dumps(caseInfo, indent=4)
        return (file_count, fileID_list)

    if (verboseFlag >= 7):
        print
        len(fileID_list)
        print
        fileID_list

    return (file_count, fileID_list)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def examineFilesInfo(fileID_dict):
    files_fields.sort()

    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in examineFilesInfo ... ", len(fileID_dict)
        print
        " "
        print
        files_fields

    ## and now let's do the same for each of the FILEs ...

    fileFieldValues = {}
    for aField in files_fields:
        fileFieldValues[aField] = {}

    for file_id in fileID_dict:

        if (len(fileID_dict[file_id]) > 1):
            print
            " WHAT (file) ??? "
            print
            len(fileID_dict[file_id])
            print
            " FATAL ERROR in examineFilesInfo "
            sys.exit(-1)

        fileInfo = fileID_dict[file_id][0]

        if (verboseFlag >= 9):
            print
            " for this file_id : ", file_id
            print
            " got this : "
            print
            fileInfo
            print
            " "
            print
            " "

        for aField in files_fields:
            ## print " "
            ## print " aField=<%s> " % aField
            ## print " fileFieldValues[aField] = ", fileFieldValues[aField]
            ## try:
            ##     print " fileInfo[aField] = ", fileInfo[aField]
            ## except:
            ##     print " nothing found for this aField "
            if (len(fileFieldValues[aField]) < 50):
                try:
                    for aValue in fileInfo[aField]:
                        if (aValue not in fileFieldValues[aField]):
                            fileFieldValues[aField][aValue] = 1
                        else:
                            fileFieldValues[aField][aValue] += 1
                            ## print fileFieldValues[aField]
                except:
                    pass

    for aField in files_fields:
        numV = len(fileFieldValues[aField])
        if (numV > 0 and numV < 50):
            if (verboseFlag >= 2): print
            aField, fileFieldValues[aField]

    if (verboseFlag >= 1):
        print
        " "
        print
        " "

    return


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def mergeStrings(aVec, nMax):
    mStr = ''
    uVec = []
    for ii in range(len(aVec)):
        if (aVec[ii] not in uVec): uVec += [aVec[ii]]

    if (len(uVec) > nMax): return ("multi")

    uVec.sort()

    for ii in range(len(uVec)):
        mStr += '%s' % uVec[ii]
        if (ii < len(uVec) - 1): mStr += ';'

    return (mStr)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def writeFileTable4BigQuery(fh, dbName, fileID_dict):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in writeFileTablesBigQuery ... ", len(fileID_dict)
        print
        " "

    hdrFlag = 1
    hdrLine = ''

    for file_id in fileID_dict:

        fileInfo = fileID_dict[file_id][0]

        if (verboseFlag >= 1):
            print
            " for this file_id : ", file_id
            print
            " got this : "
            print
            fileInfo
            print
            " "
            print
            " "
            print
            " files_fields : "
            print
            files_fields

        ## build up the output line ...

        if (hdrFlag):
            hdrLine = 'dbName'
            hdrLine += '\tfile_id'

        outLine = '%s' % dbName
        outLine += '\t%s' % file_id
        for aField in files_fields:
            if (aField != "file_id"):
                if (hdrFlag): hdrLine += '\t%s' % aField
                try:
                    if (len(fileInfo[aField]) == 0):
                        outLine += '\t'
                    else:
                        mStr = mergeStrings(fileInfo[aField], 8)
                        outLine += '\t%s' % mStr
                except:
                    outLine += '\t'

        if (hdrFlag):
            fh.write("%s\n" % hdrLine)
            hdrFlag = 0

        fh.write("%s\n" % outLine)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def writeOneFile4BQ(fh, dbName, file_id, fileInfo):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in writeOneFile4BQ ... ", file_id
        print
        " "

    if (1):

        if (verboseFlag >= 1):
            print
            " for this file_id : ", file_id
            print
            " got this : "
            print
            fileInfo
            print
            " "
            print
            " "
            print
            " files_fields : "
            print
            files_fields

        ## build up the output line ...

        outLine = '%s' % dbName
        outLine += '\t%s' % file_id
        for aField in files_fields:
            if (aField != "file_id"):
                try:
                    if (len(fileInfo[aField]) == 0):
                        outLine += '\t'
                    else:
                        mStr = mergeStrings(fileInfo[aField], 8)
                        outLine += '\t%s' % mStr
                except:
                    outLine += '\t'

        fh.write("%s\n" % outLine)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def main(args):

    global verboseFlag

    GDC_endpts = {}

    if args.verbosity is None:
        verboseFlag = 0
    else:
        verboseFlag = args.verbosity

    ## define where the data is going to come from ...
    if (args.endpoint.lower().find("leg") >= 0):
        GDC_endpts['legacy'] = {}
        ## GDC_endpts['legacy']['files'] = "https://gdc-api.nci.nih.gov/legacy/files"
        GDC_endpts['legacy']['files'] = "https://api.gdc.cancer.gov/legacy/files"

    elif (args.endpoint.lower().find("act") >= 0):
        GDC_endpts['active'] = {}
        ## GDC_endpts['active']['files'] = "https://gdc-api.nci.nih.gov/files"
        GDC_endpts['active']['files'] = "https://api.gdc.cancer.gov/files"

    else:
        print
        " invalid endpoint flag : ", args.endpoint
        print
        " should be either legacy or active "
        sys.exit(-1)

    fName = "fileData.bq." + uuidStr + ".tsv"
    file_fh = file(fName, 'w')

    fName = "fileData.bq." + uuidStr + ".tmp"
    file_fh2 = file(fName, 'w')

    fName = "fileList." + uuidStr + ".txt"
    file_fh3 = file(fName, 'w')

    for dbName in GDC_endpts.keys():

        print
        " "
        print
        " "
        print
        " get all of the file GUIDs ... either from the API or from the input file provided "

        try:
            if (args.id_list != ''):
                id_fh = file(args.id_list, 'r')
                fileID_list = get_all_file_ids_from_file(id_fh)

        except:
            print
            " Querying GDC database %s for all files " % dbName
            fileID_list = get_all_file_ids(GDC_endpts[dbName]['files'])

        allIDs = fileID_list.keys()
        allIDs.sort()
        for a in allIDs:
            file_fh3.write("%s\n" % a)
        file_fh3.close()

        fileID_dict = getFileInfo(fileID_list, GDC_endpts[dbName]['files'], dbName, file_fh2)

        print
        " DONE processing %s database " % dbName
        print
        " "
        files_fields.sort()
        print
        " files fields : "
        for aField in files_fields:
            print
            "     ", aField
        print
        " "

        print
        " "
        print
        " "

        examineFilesInfo(fileID_dict)

        writeFileTable4BigQuery(file_fh, dbName, fileID_dict)

    file_fh.close()
    file_fh2.close()


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

if __name__ == '__main__':
    print
    " "
    print
    " RUNNING ... ", uuidStr
    print
    " "

    parser = argparse.ArgumentParser(description="Query the GDC endpoints for file metadata")
    parser.add_argument("-v", "--verbosity", type=int, help="Verbosity (0 to 99) Can get ginormous if > 0")
    parser.add_argument("-e", "--endpoint", type=str, help="either legacy or active", required=True)
    parser.add_argument("-i", "--id_list", type=str, help="input file with list of file ids")
    args = parser.parse_args()

    main(args)

    ## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

