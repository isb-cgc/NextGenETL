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

cases_fields = []
files_fields = []

## the higher this flag is set, the more larger the output log file
## (assuming the output of this program is dumped to a file)
#verboseFlag = 999
#verboseFlag = 14
# WJRL 11/10/19 NOW AN ARGUMENT!
#verboseFlag = 0 # WJRL 11/8/19

uuidStr = uuid.uuid1().hex[:8]

numCases = 0
numFiles = 0

fhQ = 0
fhS = 0


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
## this function simply asks the "cases" endpt for the "case_id" and "submitter_id"
## for ALL cases, using a page size of 4000 and continuing to request the next
## 4000 samples (sorted by case_id) until they've all been retreived

def get_all_case_ids(cases_endpt):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in get_all_case_ids ... ", cases_endpt
        print
        " "

    maxSize = 4000
    maxSize = 1000
    fromStart = 0
    done = 0

    caseID_map = {}

    while not done:

        params = {'fields': 'submitter_id,case_id',
                  'sort': 'case_id:asc',
                  'from': fromStart,
                  'size': maxSize}

        try:
            if (verboseFlag >= 9): print
            " get request ", cases_endpt, params
            response = requests.get(cases_endpt, params=params, timeout=60.0)
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
            if (verboseFlag >= 3): print
            json.dumps(rj, indent=4)
        except:
            print
            " failed to get information about cases ??? "
            continue

        ## expecting something like this in each rj['data']['hits']:
        ##    {
        ##        "case_id": "5bc515c8-7727-4d69-98e9-31bbcb748550",
        ##        "submitter_id": "TCGA-GN-A26A"
        ##    }

        try:

            if (verboseFlag >= 3): print
            " now parsing info returned ... "

            iCount = rj['data']['pagination']['count']
            iFrom = rj['data']['pagination']['from']
            iPages = rj['data']['pagination']['pages']
            iTotal = rj['data']['pagination']['total']
            iSize = rj['data']['pagination']['size']
            if (verboseFlag >= 3): print
            "pagination info: ", iCount, iFrom, iPages, iTotal, iSize

            fromStart += iCount
            if (iCount == 0):
                if (verboseFlag >= 2): print
                " got nothing back ... (?) "
                done = 1

            for ii in range(iCount):
                if (verboseFlag >= 3): print
                ii, rj['data']['hits'][ii]
                case_id = rj['data']['hits'][ii]['case_id']
                submitter_id = rj['data']['hits'][ii]['submitter_id']
                if (verboseFlag >= 3): print
                case_id, submitter_id
                if (case_id not in caseID_map):
                    caseID_map[case_id] = submitter_id
                else:
                    if (verboseFlag >= 3):
                        print
                        " already have this one in dict ?!?! ", ii, iCount, case_id, submitter_id

            if (verboseFlag >= 1): print
            "         ", len(caseID_map)

        except:
            print
            " "
            print
            " --> setting DONE to TRUE now ... is this OK ??? "
            print
            " "
            done = 1

            ## temporary hack for early exit ... remove when not needed ...
            ## if ( len(caseID_map) > 9 ): done = 1

    if (verboseFlag >= 1): print
    " returning map with %d case ids " % len(caseID_map)

    ## write out this mapping as a two-column output file
    if (cases_endpt.find("legacy") >= 0):
        fName = "caseIDmap.legacy." + uuidStr + ".tsv"
    else:
        fName = "caseIDmap.active." + uuidStr + ".tsv"
    fh = file(fName, 'w', 0)
    allKeys = caseID_map.keys()
    allKeys.sort()
    for aKey in allKeys:
        fh.write('%s\t%s\n' % (aKey, caseID_map[aKey]))
    fh.close()

    ## temporary hack to stop now, with just the caseID_map output ...
    ## sys.exit(-1)

    return (caseID_map)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def getCaseAndFileInfo(cases_endpt, files_endpt, caseID_map, \
                       dbName, case_fh2, file_fh2):
    global numCases
    global numFiles

    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in getCaseAndFileInfo ... ", cases_endpt, files_endpt, len(caseID_map)
        print
        " "

    allCases = caseID_map.keys()
    allCases.sort()

    caseID_dict = {}
    fileID_dict = {}

    ## outer loop is over ALL cases ...
    for case_id in allCases:

        if (verboseFlag >= 1):
            print
            " "
            print
            " "
            print
            " in getCaseAndFileInfo ... looping over allCases ... ", case_id

        check1 = 0

        while not check1:

            ## first, get information about this specific case
            caseInfo = get_case_info(cases_endpt, case_id)
            caseInfo = stripBlanks(caseInfo)
            numCases += 1

            if (case_id not in caseID_dict):
                if (verboseFlag >= 3):
                    print
                    case_id
                    print
                    " creating caseID_entry for this case ", case_id
                caseID_dict[case_id] = [caseInfo]
            else:
                ## this doesn't ever appear to happen, fortunately
                print
                " already know about this case ??? !!! "
                print
                " --> updating information just in case ... "
                caseID_dict[case_id] = [caseInfo]

            ## write out the information for this case ...
            writeOneCase4BQ(case_fh2, dbName, case_id, caseInfo)

            ## from the caseInfo, we can extract all of the file_id's ...
            (numFiles0, fileID_list) = get_fileID_list(caseInfo)
            numFiles1 = len(fileID_list)

            ## if these two counts check out, we can continue ...
            if (numFiles0 == numFiles1): check1 = 1

        if (verboseFlag >= 1):
            print
            " --> got back %d files for this CASE " % numFiles1

        ## get all files at once for this case ...
        fileInfoVec = get_file_info_by_case(files_endpt, case_id, numFiles1)
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
                " FATAL ERROR in getCaseAndFileInfo "
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
                    " creating fileID_dict entry for this case and file ", case_id, file_id
                    print
                    fileID_dict[file_id]
            else:

                if (verboseFlag >= 3):
                    print
                    " already know about this file ... no worries ... ", file_id

        if (verboseFlag >= 1):
            if (numCases % 100 == 0):
                print
                "     working ... in getCaseAndFileInfo ... %d ... %d " % (numCases, numFiles)

    if (verboseFlag >= 1):
        print
        " "
        print
        " returning dicts with %d cases and %d files " % (len(caseID_dict), len(fileID_dict))
        print
        " "

    return (caseID_dict, fileID_dict)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def unpackList(aList):
    outName = ''
    outList = []

    for aVal in aList:
        print
        aVal
        try:
            aKey = aVal.keys()[0]
            if (len(aVal.keys()) > 1):
                print
                " WARNING ??? WHAT IS THIS ??? ", aVal
            if (len(outName) < 1):
                outName = aKey
            else:
                if (outName != aKey):
                    print
                    " WARNING ??? inconsistent values ??? ", outName, aKey
            zVal = aVal[aKey]
            if (zVal not in outList): outList += [zVal]
        except:
            print
            " TRY failed in unpackList ??? ", aVal
            print
            " "

    print
    " from unpackList : ", outName, outList
    return (outName, outList)


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
## note that at this point some of the strings could still have leading/trailing
## blanks...

def getCaseTree(caseInfo):
    if (verboseFlag >= 5):
        print
        " "
        print
        " >>> in getCaseTree ... "
        print
        " "

    if (verboseFlag >= 33):
        print
        " raw dump : "
        print
        caseInfo
        print
        " ---------- "
        print
        " "

    cKeys = caseInfo.keys()
    if (verboseFlag >= 33): print
    " caseInfo.keys : ", cKeys

    program_name = caseInfo['project']['program']['name']
    project_id = caseInfo['project']['project_id']

    case_gdc_id = caseInfo['case_id'].strip()
    case_barcode = caseInfo['submitter_id'].strip()

    if ('sample_ids' not in cKeys):
        print
        "     --> this case has no samples, it seems ... ", case_gdc_id, case_barcode
        return

    ## we should have a matching number of sample_ids and submitter_sample_ids
    sample_gdc_ids = caseInfo['sample_ids']
    sample_barcodes = caseInfo['submitter_sample_ids']
    if (verboseFlag >= 33):
        print
        len(sample_gdc_ids), sample_gdc_ids
        print
        len(sample_barcodes), sample_barcodes

    if (verboseFlag >= 33): print
    " digging into the samples ... "
    v = caseInfo["samples"]
    numSamples = len(v)
    if (verboseFlag >= 33): print
    " numSamples : ", numSamples

    for iSamp in range(numSamples):
        if (verboseFlag >= 33): print
        " iSamp : ", iSamp
        u = v[iSamp]
        uKeys = u.keys()

        if (verboseFlag >= 33):
            print
            uKeys
            print
            " sample_gdc_id : ", u['sample_id']
            print
            " sample_barcode : ", u['submitter_id']
            print
            " sample_type : ", u['sample_type']
            print
            " sample_type_id : ", u['sample_type_id']
            try:
                print
                " sample_is_ffpe : ", u['is_ffpe']
            except:
                print
                " NO is_ffpe field ... "
            try:
                print
                " sample_preservation_method : ", u['preservation_method']
            except:
                print
                " NO preservation_method field ... "

        sample_gdc_id = u['sample_id'].strip()
        sample_barcode = u['submitter_id'].strip()
        sample_type = u['sample_type'].strip()
        sample_type_id = u['sample_type_id'].strip()

        try:
            sample_is_ffpe = str(u['is_ffpe'])
        except:
            sample_is_ffpe = ''

        try:
            if (u['preservation_method'] is None):
                sample_preservation_method = ''
            else:
                sample_preservation_method = u['preservation_method'].strip()
        except:
            sample_preservation_method = ''

        if ('portions' in uKeys):
            w = u['portions']
            numPortions = len(w)
            if (verboseFlag >= 33): print
            " numPortions : ", numPortions

            for iPort in range(numPortions):
                if (verboseFlag >= 33): print
                " iPort : ", iPort
                x = w[iPort]
                xKeys = x.keys()
                if (verboseFlag >= 33):
                    print
                    " x : ", x
                    print
                    " xKeys : ", xKeys

                try:
                    if (verboseFlag >= 33):
                        print
                        " portion_gdc_id : ", x['portion_id']
                        print
                        " portion_barcode : ", x['submitter_id']
                    portion_gdc_id = x['portion_id'].strip()
                    portion_barcode = x['submitter_id'].strip()
                except:
                    if (verboseFlag >= 33): print
                    " failed to get portion_id and submitter_id ??? ", xKeys
                    portion_gdc_id = "NA"
                    portion_barcode = "NA"

                if ('slides' in xKeys):
                    y = x['slides']
                    if (verboseFlag >= 33): print
                    " y : ", y
                    numSlides = len(y)
                    if (verboseFlag >= 33): print
                    " numSlides : ", numSlides
                    if (numSlides == 0):
                        print
                        " hmmmm no slides for this portion ? "
                        slide_gdc_id = "NA"
                        slid_gdc_barcode = "NA"
                        fhS.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                  (program_name, project_id, \
                                   case_gdc_id, case_barcode, \
                                   sample_gdc_id, sample_barcode, \
                                   sample_type_id, sample_type, \
                                   portion_gdc_id, portion_barcode, \
                                   slide_gdc_id, slide_barcode))

                    for iSlide in range(numSlides):
                        if (verboseFlag >= 33):
                            print
                            " slide_gdc_id : ", y[iSlide]['slide_id']
                            print
                            " slide_barcode : ", y[iSlide]['submitter_id']

                        slide_gdc_id = y[iSlide]['slide_id'].strip()
                        slide_barcode = y[iSlide]['submitter_id'].strip()

                        fhS.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                  (program_name, project_id, \
                                   case_gdc_id, case_barcode, \
                                   sample_gdc_id, sample_barcode, \
                                   sample_type_id, sample_type, \
                                   portion_gdc_id, portion_barcode, \
                                   slide_gdc_id, slide_barcode))
                        if (verboseFlag >= 66): print
                        " done with iSlide ", iSlide

                if ('analytes' in xKeys):
                    z = x['analytes']
                    numAnalytes = len(z)
                    if (verboseFlag >= 33): print
                    " numAnalytes : ", numAnalytes
                    if (numAnalytes == 0):
                        print
                        " hmmmm no analytes for this portion ? "
                        analyte_gdc_id = "NA"
                        analyte_barcode = "NA"
                        aliquot_gdc_id = "NA"
                        aliquot_barcode = "NA"
                        fhQ.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                  (program_name, project_id, \
                                   case_gdc_id, case_barcode, \
                                   sample_gdc_id, sample_barcode, \
                                   sample_type_id, sample_type, \
                                   sample_is_ffpe, sample_preservation_method, \
                                   portion_gdc_id, portion_barcode, \
                                   analyte_gdc_id, analyte_barcode, \
                                   aliquot_gdc_id, aliquot_barcode))

                    for iA in range(numAnalytes):
                        a = z[iA]
                        aKeys = a.keys()
                        if (verboseFlag >= 33):
                            print
                            iA
                            print
                            " a : ", a
                            print
                            " aKeys : ", aKeys
                        try:
                            if (verboseFlag >= 33):
                                print
                                " analyte_gdc_id : ", a['analyte_id']
                                print
                                " analyte_barcode : ", a['submitter_id']
                            analyte_gdc_id = a['analyte_id'].strip()
                            analyte_barcode = a['submitter_id'].strip()
                        except:
                            if (verboseFlag >= 33): print
                            " failed to get analyte_id and submitter_id ??? ", aKeys
                            analyte_gdc_id = "NA"
                            analyte_barcode = "NA"

                        if ('aliquots' in aKeys):
                            b = a['aliquots']
                            numAliquots = len(b)
                            if (verboseFlag >= 33): print
                            " numAliquots : ", numAliquots
                            if (numAliquots == 0):
                                print
                                " hmmmm no aliquots for this analyte ? "
                                aliquot_gdc_id = "NA"
                                aliquot_barcode = "NA"
                                fhQ.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                          (program_name, project_id, \
                                           case_gdc_id, case_barcode, \
                                           sample_gdc_id, sample_barcode, \
                                           sample_type_id, sample_type, \
                                           sample_is_ffpe, sample_preservation_method, \
                                           portion_gdc_id, portion_barcode, \
                                           analyte_gdc_id, analyte_barcode, \
                                           aliquot_gdc_id, aliquot_barcode))

                            for iB in range(numAliquots):
                                c = b[iB]
                                if (verboseFlag >= 33):
                                    print
                                    iB
                                    print
                                    " c : ", c
                                    print
                                    " aliquot_gdc_id : ", c['aliquot_id']
                                    print
                                    " aliquot_barcode : ", c['submitter_id']

                                aliquot_gdc_id = c['aliquot_id'].strip()
                                aliquot_barcode = c['submitter_id'].strip()
                                fhQ.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                          (program_name, project_id, \
                                           case_gdc_id, case_barcode, \
                                           sample_gdc_id, sample_barcode, \
                                           sample_type_id, sample_type, \
                                           sample_is_ffpe, sample_preservation_method, \
                                           portion_gdc_id, portion_barcode, \
                                           analyte_gdc_id, analyte_barcode, \
                                           aliquot_gdc_id, aliquot_barcode))

                        else:
                            print
                            " hmmmm no aliquots for this analyte ? "
                            aliquot_gdc_id = "NA"
                            aliquot_barcode = "NA"

                            fhQ.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                                      (program_name, project_id, \
                                       case_gdc_id, case_barcode, \
                                       sample_gdc_id, sample_barcode, \
                                       sample_type_id, sample_type, \
                                       sample_is_ffpe, sample_preservation_method, \
                                       portion_gdc_id, portion_barcode, \
                                       analyte_gdc_id, analyte_barcode, \
                                       aliquot_gdc_id, aliquot_barcode))

                else:
                    print
                    " hmmmm no analytes for this portion ? "
                    analyte_gdc_id = "NA"
                    analyte_barcode = "NA"
                    aliquot_gdc_id = "NA"
                    aliquot_barcode = "NA"

                    fhQ.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
                              (program_name, project_id, \
                               case_gdc_id, case_barcode, \
                               sample_gdc_id, sample_barcode, \
                               sample_type_id, sample_type, \
                               sample_is_ffpe, sample_preservation_method, \
                               portion_gdc_id, portion_barcode, \
                               analyte_gdc_id, analyte_barcode, \
                               aliquot_gdc_id, aliquot_barcode))


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_case_info(cases_endpt, case_id):
    if (verboseFlag >= 3):
        print
        " "
        print
        " >>> in get_case_info ... ", cases_endpt, case_id
        print
        " "

    global cases_fields

    filt = {'op': '=',
            'content': {
                'field': 'case_id',
                'value': [case_id]}}

    if (0):
        fieldsList = 'aliquot_ids,case_id,sample_ids,slide_ids,state,' \
                     + 'submitter_aliquot_ids,submitter_id,' \
                     + 'submitter_sample_ids,submitter_slide_ids,' \
                     + 'files.file_id,' \
                     + 'project.dbgap_accession_number,project.disease_type,project.name,project.project_id,' \
                     + 'project.program.dbgap_accession_number,project.program.name,' \
                     + 'summary.file_count'

    if (1):
        ## removing top-level aliquot_ids, slide_ids, and the submitter_* versions too
        ## and instead added the samples.portions.* fields ...

        ## added preservation_method and is_ffpe fields to the samples.* (04/03/2019)
        fieldsList = 'case_id,sample_ids,state,' \
                     + 'submitter_id,' \
                     + 'submitter_sample_ids,' \
                     + 'files.file_id,' \
                     + 'samples.sample_id,' \
                     + 'samples.submitter_id,' \
                     + 'samples.sample_type,' \
                     + 'samples.sample_type_id,' \
                     + 'samples.tumor_code,' \
                     + 'samples.preservation_method,' \
                     + 'samples.is_ffpe,' \
                     + 'samples.pathology_report_uuid,' \
                     + 'samples.portions.analytes.aliquots.aliquot_id,' \
                     + 'samples.portions.analytes.aliquots.submitter_id,' \
                     + 'samples.portions.analytes.analyte_id,' \
                     + 'samples.portions.analytes.submitter_id,' \
                     + 'samples.portions.portion_id,' \
                     + 'samples.portions.submitter_id,' \
                     + 'samples.portions.slides.slide_id,' \
                     + 'samples.portions.slides.submitter_id,' \
                     + 'project.dbgap_accession_number,project.disease_type,project.name,project.project_id,' \
                     + 'project.program.dbgap_accession_number,project.program.name,' \
                     + 'summary.file_count'

        ## print " fieldsList: <%s> " % fieldsList

    params = {'fields': fieldsList,
              'filters': json.dumps(filt)}

    if (verboseFlag >= 9): print
    " get request ", cases_endpt, params

    iTry = 0
    sleepTime = 0.1
    ## outer loop for multiple retries ...
    while (iTry < 10):

        if (iTry == 9):
            print
            " HOLY COW WHAT IS GOING ON ??? !!! "

        if (iTry > 0):
            print
            " >>>> trying again ... ", iTry + 1, sleepTime
            time.sleep(sleepTime)
            sleepTime = sleepTime * 1.5
            if (sleepTime > 60.): sleepTime = 60.
        iTry += 1

        try:
            if (verboseFlag >= 9): print
            " get request ", cases_endpt, params
            response = requests.get(cases_endpt, params=params, timeout=60.0)
        except:
            print
            " ERROR !!! requests.get() call FAILED ??? (b) "

        try:
            if (verboseFlag >= 9): print
            " now parsing json response ... "
            rj = response.json()
            if (verboseFlag >= 5):
                print
                json.dumps(rj, indent=4)
            if (len(rj['data']['hits']) != 1):
                if (len(rj['data']['hits']) >= 1):
                    print
                    " HOW DID THIS HAPPEN ??? more than one case ??? !!! ", len(rj['data']['hits'])
                else:
                    print
                    " NOTHING came back for this case ??? ", case_id
            caseInfo = rj['data']['hits'][0]

            ## before we flatten this structure, we need to get the
            ## complete case -> sample -> aliquot relationship ...
            try:
                caseTree = getCaseTree(caseInfo)
            except:
                print
                " WARNING !!! failed in getCaseTree !!! "

            if (verboseFlag >= 9): print
            " calling flattenJSON ... "
            caseInfo = flattenJSON(caseInfo)

            fields = caseInfo.keys()
            fields.sort()

            for aField in fields:
                if (aField not in cases_fields):
                    if (verboseFlag >= 1):
                        print
                        " adding new field to cases_fields list : <%s> " % aField
                    cases_fields += [aField]

            return (caseInfo)

        except:
            print
            " ERROR in get_case_info ??? failed to get any information about this case ??? ", case_id

    return ({})

    ## sys.exit(-1)


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def get_file_info_by_case(files_endpt, case_id, numExpected):
    if (verboseFlag >= 3):
        print
        " "
        print
        " >>> in get_file_info_by_case ... ", files_endpt, case_id
        print
        " "

    global files_fields

    maxNumFiles = 1500

    filt = {'op': '=',
            'content': {
                'field': 'cases.case_id',
                'value': [case_id]}}

    fieldsList = 'access,acl,created_datetime,data_category,data_format,' \
                 + 'cases.case_id,' \
                 + 'data_type,error_type,experimental_strategy,file_id,' \
                 + 'file_name,file_size,file_state,md5sum,origin,platform,' \
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

    params = {'fields': fieldsList,
              'filters': json.dumps(filt),
              'sort': 'file_id:asc',
              'from': 0,
              'size': maxNumFiles}

    if (verboseFlag >= 9): print
    " get request ", files_endpt, params

    iTry = 0
    sleepTime = 0.1
    while (iTry < 10):

        if (iTry == 9):
            print
            " HOLY COW WHAT IS GOING ON ??? !!! "

        if (iTry > 0):
            print
            " >>>> trying again ... ", iTry + 1, sleepTime
            time.sleep(sleepTime)
            sleepTime = sleepTime * 1.5
            if (sleepTime > 60.): sleepTime = 60.
        iTry += 1

        try:
            if (verboseFlag >= 9): print
            " get request ", files_endpt, params
            response = requests.get(files_endpt, params=params, timeout=60.0)
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

            if (numFiles >= maxNumFiles):
                print
                " ERROR ??? need to increase maxNumFiles limit !!! ??? "
                sys.exit(-1)

            if (numFiles < numExpected):
                print
                " ERROR ??? did not get back all of the files we were expecting ??? !!! "
                print
                "     expecting %d ... got back %d " % (numExpected, numFiles)
                sys.exit(-1)

            if (numFiles != numExpected):
                print
                " ERROR ??? number of files does not match expected number ??? (%d,%d) " % (numFiles, numExpected)

            ## if the number of files we get back is greater than or equal to
            ## what's expected, let's just go with it ...
            if (numFiles >= numExpected):

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
            " ERROR in get_file_info_by_case ??? failed to get any information about this case ??? ", case_id

            ## go back and try again ...

    ## returning EMPTY HANDED ???
    if (verboseFlag >= 1):
        print
        " "
        print
        " --> returning EMPTY HANDED from get_file_info_by_case ??? ERROR ??? ", case_id, numExpected
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

def examineCasesInfo(caseID_dict):
    cases_fields.sort()

    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in examineCasesInfo ... ", len(caseID_dict)
        print
        " "
        print
        cases_fields

    ## let's explore the various properties that have been collected
    ## for each of the CASEs ...

    caseFieldValues = {}
    for aField in cases_fields:
        caseFieldValues[aField] = {}

    for case_id in caseID_dict:

        if (len(caseID_dict[case_id]) > 1):
            print
            " WHAT ??? "
            print
            len(caseID_dict[case_id])
            print
            " FATAL ERROR in examineCasesInfo "
            sys.exit(-1)

        caseInfo = caseID_dict[case_id][0]

        if (verboseFlag >= 9):
            print
            " for this case_id : ", case_id
            print
            " got this : "
            print
            caseInfo
            print
            " "
            print
            " "

        if (1):
            for aField in cases_fields:
                ## print " "
                ## print " aField=<%s> " % aField
                ## print " caseFieldValues[aField] = ", caseFieldValues[aField]
                ## try:
                ##     print " caseInfo[aField] = ", caseInfo[aField]
                ## except:
                ##     print " nothing found for this aField "
                if (len(caseFieldValues[aField]) < 50):
                    try:
                        for aValue in caseInfo[aField]:
                            if (aValue not in caseFieldValues[aField]):
                                caseFieldValues[aField][aValue] = 1
                            else:
                                caseFieldValues[aField][aValue] += 1
                                ## print caseFieldValues[aField]
                    except:
                        pass

    for aField in cases_fields:
        numV = len(caseFieldValues[aField])
        if (numV > 0 and numV < 50):
            if (verboseFlag >= 2): print
            aField, caseFieldValues[aField]

    if (verboseFlag >= 1):
        print
        " "
        print
        " "

    return


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

    ## 04/05/2019 -- removing the SORT call here
    ## uVec.sort()

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

def writeCaseTable4BigQuery(fh, dbName, caseID_dict):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in writeCaseTable4BigQuery ... ", len(caseID_dict)
        print
        " "

    hdrFlag = 1
    hdrLine = ''

    for case_id in caseID_dict:

        caseInfo = caseID_dict[case_id][0]

        if (verboseFlag >= 1):
            print
            " for this case_id : ", case_id
            print
            " got this : "
            print
            caseInfo
            print
            " "
            print
            " "
            print
            " cases_fields : "
            print
            cases_fields

        ## build up the output line ...

        if (hdrFlag):
            hdrLine = 'dbName'
            hdrLine += '\tcase_id'

        outLine = '%s' % dbName
        outLine += '\t%s' % case_id
        for aField in cases_fields:
            if (aField != "case_id"):
                if (hdrFlag): hdrLine += '\t%s' % aField
                try:
                    if (len(caseInfo[aField]) == 0):
                        outLine += '\t'
                    else:
                        mStr = mergeStrings(caseInfo[aField], 8)
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

def writeOneCase4BQ(fh, dbName, case_id, caseInfo):
    if (verboseFlag >= 1):
        print
        " "
        print
        " >>> in writeOneCase4BQ ... ", case_id
        print
        " "

    if (1):

        if (verboseFlag >= 1):
            print
            " for this case_id : ", case_id
            print
            " got this : "
            print
            caseInfo
            print
            " "
            print
            " "
            print
            " cases_fields : "
            print
            cases_fields

        ## build up the output line ...

        outLine = '%s' % dbName
        outLine += '\t%s' % case_id
        for aField in cases_fields:
            if (aField != "case_id"):
                try:
                    if (len(caseInfo[aField]) == 0):
                        outLine += '\t'
                    else:
                        mStr = mergeStrings(caseInfo[aField], 8)
                        outLine += '\t%s' % mStr
                except:
                    outLine += '\t'

        fh.write("%s\n" % outLine)
        if (verboseFlag >= 1): print
        " wrote output line "


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

def main(args):
    global fhQ
    global fhS
    global verboseFlag

    GDC_endpts = {}

    if args.verbosity is None:
        verboseFlag = 0
    else:
        verboseFlag = args.verbosity

    ## define where the data is going to come from ...
    if (args.endpoint.lower().find("leg") >= 0):
        GDC_endpts['legacy'] = {}
        ## GDC_endpts['legacy']['cases'] = "https://gdc-api.nci.nih.gov/legacy/cases"
        ## GDC_endpts['legacy']['files'] = "https://gdc-api.nci.nih.gov/legacy/files"
        GDC_endpts['legacy']['cases'] = "https://api.gdc.cancer.gov/legacy/cases"
        GDC_endpts['legacy']['files'] = "https://api.gdc.cancer.gov/legacy/files"

    elif (args.endpoint.lower().find("act") >= 0):
        GDC_endpts['active'] = {}
        ## GDC_endpts['active']['cases'] = "https://gdc-api.nci.nih.gov/cases"
        ## GDC_endpts['active']['files'] = "https://gdc-api.nci.nih.gov/files"
        GDC_endpts['active']['cases'] = "https://api.gdc.cancer.gov/cases"
        GDC_endpts['active']['files'] = "https://api.gdc.cancer.gov/files"

    else:
        print
        " invalid endpoint flag : ", args.endpoint
        print
        " should be either legacy or active "
        sys.exit(-1)

    fName = "caseData.bq." + uuidStr + ".tsv"
    case_fh = file(fName, 'w', 0)
    fName = "fileData.bq." + uuidStr + ".tsv"

    file_fh = file(fName, 'w', 0)

    fName = "caseData.bq." + uuidStr + ".tmp"
    case_fh2 = file(fName, 'w', 0)

    fName = "fileData.bq." + uuidStr + ".tmp"
    file_fh2 = file(fName, 'w', 0)

    fName = "aliqMap.bq." + uuidStr + ".tsv"
    fhQ = file(fName, 'w', 0)

    fName = "slidMap.bq." + uuidStr + ".tsv"
    fhS = file(fName, 'w', 0)

    for dbName in GDC_endpts.keys():

        if args.case_ids is not None:
            caseID_map = {}
            with open(args.case_ids) as f:
                caseID_map[f.readline()] = ''

        elif args.case_id is None:
            print
            " "
            print
            " "
            print
            " Querying GDC database %s for all cases and files " % dbName

            ## the first step is to use the "cases" endpt and get the "case_id"
            ## and "submitter_id" for ALL cases known to this endpt
            caseID_map = get_all_case_ids(GDC_endpts[dbName]['cases'])
        else:
            caseID_map = {}
            caseID_map[args.case_id] = ''

        (caseID_dict, fileID_dict) = \
            getCaseAndFileInfo(GDC_endpts[dbName]['cases'], \
                               GDC_endpts[dbName]['files'], \
                               caseID_map, dbName, case_fh2, file_fh2)

        print
        " DONE processing %s database " % dbName
        print
        " "
        cases_fields.sort()
        files_fields.sort()
        print
        " cases fields : "
        for aField in cases_fields:
            print
            "     ", aField
        print
        " "
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

        examineCasesInfo(caseID_dict)
        examineFilesInfo(fileID_dict)

        writeCaseTable4BigQuery(case_fh, dbName, caseID_dict)
        writeFileTable4BigQuery(file_fh, dbName, fileID_dict)

    case_fh.close()
    file_fh.close()


## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

if __name__ == '__main__':
    print
    " "
    print
    " RUNNING ... ", uuidStr
    print
    " "

    parser = argparse.ArgumentParser(description="Query the GDC endpoints for case and file metadata")
    parser.add_argument("-v", "--verbosity", type=int, help="Verbosity (0 to 999) Can get ginormous if > 0")
    parser.add_argument("-e", "--endpoint", type=str, help="either legacy or active", required=True)
    parser.add_argument("-i", "--case_id", type=str, help="single case GUID")
    parser.add_argument("-s", "--case_ids", type=str, help="file with multiple case GUIDs")
    args = parser.parse_args()

    main(args)

    ## -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

