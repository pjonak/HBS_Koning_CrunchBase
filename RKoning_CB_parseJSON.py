import json
import _io
import os
import pandas
import numpy
import copy
import random
import datetime

I_grid = False
I_print = True
I_log = False

if I_grid:
    folderPath_root = "/export/home/dor/pjonak/Projects/RKoning_CB/"
else:
    # Get current directory of where this code file is
    # Get project root directory by going to the parent directory
    folderPath_root = os.path.dirname( os.path.realpath(__file__) )
    folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

# Identify where data is held
if I_grid:
    folderPath_data = "dataset/"
    folderPath_SW = "SW_responses/"
    folderPath_BW = "BW_responses/"
    folderPath_tables = "tables/"
else:
    folderPath_data = "dataset\\crunchbase_2017_02_06\\"
    folderPath_SW = "SW_responses\\"
    folderPath_BW = "BW_responses\\"
    folderPath_tables = "tables\\"

# Which file are we looking to load?
if I_grid:
    fileName_CB = "organizations"
    fileName_CB = fileName_CB + "_trim.csv"

else:
    fileName_CB = "organizations"
    fileName_CB = "organizations_sample"
    fileName_CB = fileName_CB + "_intel.csv"




# Get file lists for each folder
fileName_dat_SW = "dat__intel.csv"
fileName_dat_BW = "dat__intel.csv"

fileName_table_DomainTech = "table_DomainTech.csv"
fileName_table_Tech = "table_Tech.csv"
fileName_table_TechTag = "table_TechTag.csv"
fileName_table_TechCat = "table_TechCat.csv"
fileName_table_Visit = "table_Visit.csv"


dateFormat = "%Y-%m-%d"









# def initTable_Org() -> pandas.DataFrame:
#     initTable = pandas.DataFrame({
#         'id_lookup': numpy.empty(1, dtype=int),
#         'domain_lookup': numpy.empty(1, dtype=str),
#         'name_lookup': numpy.empty(1, dtype=str),
#         'vertical': numpy.empty(1, dtype=str),
#         'ARank': numpy.empty(1, dtype=str),
#         'QRank': numpy.empty(1, dtype=str),
#         'IndexFirst': numpy.empty(1, dtype=int),
#         'IndexLast': numpy.empty(1, dtype=int),
#         'IndexFirst_date': numpy.empty(1, dtype=str),
#         'IndexLast_date': numpy.empty(1, dtype=str),
#         'name_cb': numpy.empty(1, dtype=str),
#         'domain_cb': numpy.empty(1, dtype=str),
#         'desc': numpy.empty(1, dtype=str),
#         'category_list': numpy.empty(1, dtype=str),
#         'category_group_list': numpy.empty(1, dtype=str),
#         'founded_on': numpy.empty(1, dtype=str),
#         'first_funding_on': numpy.empty(1, dtype=str),
#         'employee_count': numpy.empty(1, dtype=str),
#         'domain_sw': numpy.empty(1, dtype=str)
#     })
#     initTable['IndexFirst_date'] = pandas.to_datetime(initTable['IndexFirst_date'])
#     initTable['IndexLast_date'] = pandas.to_datetime(initTable['IndexLast_date'])
#     return initTable

def initTable_DomainTech() -> pandas.DataFrame:
    initTable = pandas.DataFrame({
        'id_lookup': numpy.empty(1, dtype=int),
        'id_tech': numpy.empty(1, dtype=int),
        'DetectedFirst': numpy.empty(1, dtype=int),
        'DetectedLast': numpy.empty(1, dtype=int),
        'DetectedFirst_date': datetime.datetime.now().strftime("%Y-%m-%d"),
        'DetectedLast_date': datetime.datetime.now().strftime("%Y-%m-%d")
        # 'DetectedFirst_date': pandas.to_datetime(numpy.empty(1, dtype=str), format=dateFormat),
        # 'DetectedLast_date': pandas.to_datetime(numpy.empty(1, dtype=str), format=dateFormat)
    })
    initTable['id_lookup'].iloc[0] = 0
    initTable['id_tech'].iloc[0] = 0
    initTable['DetectedFirst_date'].iloc[0] = datetime.datetime.now().strftime("%Y-%m-%d")
    initTable['DetectedLast_date'].iloc[0] = datetime.datetime.now().strftime("%Y-%m-%d")
    return initTable

def initTable_Tech() -> pandas.DataFrame:
    initTable =  pandas.DataFrame({
        'id_tech': numpy.empty(1, dtype=int),
        'name_tech': numpy.empty(1, dtype=str),
        'id_techTag': numpy.empty(1, dtype=int),
        'id_techCat': numpy.empty(1, dtype=int)
    })
    initTable['id_tech'].iloc[0] = 0
    initTable['name_tech'].iloc[0] = "none"
    initTable['id_techTag'].iloc[0] = 0
    initTable['id_techCat'].iloc[0] = 0
    return initTable

def initTable_TechTag() -> pandas.DataFrame:
    initTable = pandas.DataFrame({
        'id_techTag': numpy.empty(1, dtype=int),
        'name_techTag': numpy.empty(1, dtype=str)
    })
    initTable['id_techTag'].iloc[0] = 0
    initTable['name_techTag'].iloc[0] = "none"
    return initTable

def initTable_TechCat() -> pandas.DataFrame:
    initTable = pandas.DataFrame({
        'id_techCat': numpy.empty(1, dtype=int),
        'name_techCat': numpy.empty(1, dtype=str)
    })
    initTable['id_techCat'].iloc[0] = 0
    initTable['name_techCat'].iloc[0] = "none"
    return initTable

def initTable_Visit() -> pandas.DataFrame:
    initTable = pandas.DataFrame({
        'id_lookup': numpy.empty(1, dtype=int),
        'visit_date': pandas.to_datetime(numpy.empty(1, dtype=str), format=dateFormat),
        'visit_count': numpy.empty(1, dtype=int),
        'visit_granularity': numpy.empty(1, dtype=str),
    })
    initTable['id_lookup'].iloc[0] = 0
    initTable['visit_count'].iloc[0] = 0
    return initTable

def CB_specialColType_range(dat: str) -> str:
    # From RKoning_CB_LoadandTrim_Orgs

    #   employee count is given as a range
    #       e.g. 1-10
    #   the CSV table shows this as 'Jan-10' or '10-Jan'

    # Remove excess white space
    dat = dat.replace(" ", "")

    if len(dat) >= 4:
        if not dat[0].isdigit() or not dat[-1].isdigit():
            # Given either 'Nov-50' or '10-Jan'
            if dat[0] == 'J' or dat[-1] == 'n' or dat[-1] == 'y':
                return "1-10"
            elif dat[0] == 'N' or dat[-1] == 'v' or dat[-1] == 'r':
                return "11-50"
            else:
                # Unknown range
                return ""
        else:
            return dat
    else:
        return ""

def strCleanup1(strIn: str) -> str:
    return strIn.encode('utf8').decode('ascii', 'ignore').replace(" ","_").replace("/","_").replace("\\","_").replace(":","_").lower()

def main():

    if I_log:
        # Prepare log file
        #   Assume ending is ".py"
        logPath = folderPath_root + os.path.basename( os.path.realpath(__file__) )[0:-3] + ".log"
        hFileLog = open(logPath, 'w', encoding="utf8")
        hFileLog.write("Log file for:\n\t" + os.path.realpath(__file__) +"\n")

    # Verify file exists
    fullPath = folderPath_root + folderPath_data + fileName_CB
    if os.path.exists(fullPath):
        if I_print or I_log:
            tempMsg = "Found CB Organizations file"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        I_flag = True
    else:
        if I_print or I_log:
            tempMsg = "CB Organizations file not found\n\tSearch path: " + fullPath
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        I_flag = False

    if I_flag:
        # Load data
        if I_print or I_log:
            tempMsg = "Loading data: " + fileName_CB
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        datOrg = pandas.read_csv(fullPath, sep=',', encoding='utf-8', index_col=0)
        if I_print or I_log:
            tempMsg = "\tDone"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")

        # Make sure we're organized by date
        #   Sorting by date
        #       column = 'founded_on'
        datOrg = datOrg.ix[datOrg['founded_on'].sort_values(ascending=False).index]

        # Fix employee_count if it got auto-formatted
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iRow in range(0, datOrg.shape[0]):
            datOrg['employee_count'].iloc[iRow] = CB_specialColType_range(
                datOrg['employee_count'].iloc[iRow] )
        pandas.options.mode.chained_assignment = 'warn'



        # Ready to add BuiltWith data to Org data
        #   Prepare CB Org data columns
        colList = ['id_lookup', 'name_bw', 'domain_bw', 'vertical', 'ARank', 'QRank', 'IndexFirst', 'IndexLast']
        colList_date = ['IndexFirst_date', 'IndexLast_date']
        colList = colList + colList_date
        colList = colList + ['domain_sw']
        colList = colList + ['filename_bw', 'filename_sw', 'I_response_bw', 'I_response_sw']

        # Get list of current column names
        #   If one of the new columns isn't present, add it
        colList_orig = numpy.asarray(list(datOrg))
        for colName in colList:
            if not any(colList_orig==colName):
                datOrg[colName] = numpy.nan
        for colName in colList_date:
            # print(datOrg[colName])
            # print( pandas.to_datetime(datOrg[colName]) )
            datOrg[colName] = pandas.to_datetime(datOrg[colName])

        # Get filenames and I_response values
        #   Load response dat file first, then SW dat file
        #       Match each row to Organizations data
        #       Record filename and I_response
        #       Verify I_response
        I_haveBW = False
        I_haveSW = False
        iBW = 0 # First entry = index 0
        folderList_response = [folderPath_BW, folderPath_SW]
        fileList_response = [fileName_dat_BW, fileName_dat_SW]
        colList_fn_response = ['bw_filename', 'sw_filename']
        colList_fn_Org = ['filename_bw', 'filename_sw']
        colList_I_response = ['bw_I_response', 'sw_I_response']
        colList_I_Org = ['I_response_bw', 'I_response_sw']

        pandas.options.mode.chained_assignment = None
        if I_print or I_log:
            tempMsg = "Verifying that we have BuiltWith and SimilarWeb response files"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        for i in range(0, 2):
            if not os.path.exists(folderPath_root + folderList_response[i] + fileList_response[i]):
                if I_print or I_log:
                    tempMsg = "\tError: Could not find response dat file!\n\t\t" + \
                              folderPath_root + folderList_response[i] + fileList_response[i]
                    if I_print:
                        print(tempMsg)
                    if I_log:
                        hFileLog.write(tempMsg + "\n")
            else:
                datResponse = pandas.read_csv(
                    folderPath_root + folderList_response[i] + fileList_response[i],
                    index_col=0)

                if i == iBW:
                    I_haveBW = True
                else:
                    I_haveSW = True

                # Now we need to match each row to Organizations data
                #   We preserved the index column
                #   There is less data in the response file so start with that
                for iRow in range(0, datResponse.shape[0]):
                    # Get index
                    idx = int(datResponse.iloc[iRow].name)
                    # Record filename and I_response
                    datOrg[colList_fn_Org[i]].ix[idx] = datResponse[colList_fn_response[i]].iloc[iRow]
                    datOrg[colList_I_Org[i]].ix[idx] = datResponse[colList_I_response[i]].iloc[iRow]
                    # Verify I_response
                    if datOrg[colList_I_Org[i]].ix[idx] == 1:
                        if not os.path.exists(folderPath_root + folderList_response[i] + datOrg[colList_fn_Org[i]].ix[idx]):
                            if I_print or I_log:
                                tempMsg = "\tError: Could not find response file!\n\t\t" + \
                                          folderPath_root + folderList_response[i] + datOrg[colList_fn_Org[i]].ix[idx]
                                if I_print:
                                    print(tempMsg)
                                if I_log:
                                    hFileLog.write(tempMsg + "\n")
                            datOrg[colList_I_Org[i]].ix[idx] = 0
        pandas.options.mode.chained_assignment = 'warn'

        # We now know which of our files has data
        # Prepare to load BuiltWith meta data
        id_lookup_last = max(datOrg["id_lookup"])
        if numpy.isnan(id_lookup_last):
            id_lookup_last = 0

        # Ready to load BuiltWith data
        if I_print or I_log:
            tempMsg = "Loading BuiltWith meta data"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        pandas.options.mode.chained_assignment = None
        for iRow in range(0, datOrg.shape[0]):
            if datOrg['I_response_bw'].iloc[iRow] == 1:
                # Verify that we don't already have information for this row
                if not isinstance(datOrg['name_bw'].iloc[iRow], str) or len(datOrg['name_bw'].iloc[iRow]) == 0:
                    # Load data
                    hFile = _io.open(folderPath_root + folderPath_BW + datOrg['filename_bw'].iloc[iRow])
                    datJ = json.loads(hFile.read())
                    hFile.close()

                    # Validate that the response has data
                    #   Gotta love those double negatives!
                    if not not datJ['Errors']:
                        if I_print or I_log:
                            tempMsg = "\tError detected for " + datOrg['domain'].iloc[iRow] + \
                                      "\n\t\tRow=" + str(iRow+1) + \
                                      "\n\t\tindex=" + str(int(datOrg.iloc[iRow].name))
                            if I_print:
                                print(tempMsg)
                            if I_log:
                                hFileLog.write(tempMsg + "\n")

                    else:

                        if len(list(datJ['Results'])) == 1:
                            iLU = 0
                        else:
                            iLU = -1
                            if I_print or I_log:
                                tempMsg = "\tMultiple lookup results for " + datOrg['domain'].iloc[iRow] + \
                                          "\n\t\tRow=" + str(iRow + 1) + \
                                          "\n\t\tindex=" + str(int(datOrg.iloc[iRow].name))
                                if I_print:
                                    print(tempMsg)
                                if I_log:
                                    hFileLog.write(tempMsg + "\n")
                            for i in range(0, len(list(datJ['Results']))):
                                if datJ['Results'][i]['Lookup'].lower() == datOrg['domain'].iloc[iRow]:
                                    iLU = i
                                    if I_print or I_log:
                                        tempMsg = "\t\t\tUsing results index = " + str(iLU)
                                        if I_print:
                                            print(tempMsg)
                                        if I_log:
                                            hFileLog.write(tempMsg + "\n")
                                    break

                        if iLU != -1:
                            id_lookup_last += 1
                            datOrg["id_lookup"].iloc[iRow] = int(id_lookup_last)
                            datOrg["domain_bw"].iloc[iRow] = datJ['Results'][iLU]['Lookup'].lower()
                            datOrg["name_bw"].iloc[iRow] = datJ['Results'][iLU]['Meta']['CompanyName']
                            datOrg["vertical"].iloc[iRow] = datJ['Results'][iLU]['Meta']['Vertical']
                            datOrg["ARank"].iloc[iRow] = datJ['Results'][iLU]['Meta']['ARank']
                            datOrg["QRank"].iloc[iRow] = datJ['Results'][iLU]['Meta']['QRank']
                            datOrg["IndexFirst"].iloc[iRow] = int(datJ['Results'][iLU]['FirstIndexed']) / 1000
                            datOrg["IndexLast"].iloc[iRow] = int(datJ['Results'][iLU]['LastIndexed']) / 1000
                            datOrg["IndexFirst_date"].iloc[iRow] = datetime.datetime.fromtimestamp(
                                datOrg["IndexFirst"].iloc[iRow]).date()
                            datOrg["IndexLast_date"].iloc[iRow] = datetime.datetime.fromtimestamp(
                                datOrg["IndexLast"].iloc[iRow]).date()
        pandas.options.mode.chained_assignment = 'warn'

        # Save progress
        if I_print or I_log:
            tempMsg = "Saving data: " + fileName_CB
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        datOrg.to_csv(folderPath_root + folderPath_data + fileName_CB, sep=',', encoding='utf-8')
        if I_print or I_log:
            tempMsg = "\tDone"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")


        # Build tables related to BuiltWith?
        #   DomainTech
        #   Tech
        #   TechTag
        #   TechCat
        if I_haveBW:
            # Initialize
            if I_print or I_log:
                tempMsg = "Initializing tables for BuiltWith data"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")

            #   DomainTech
            if I_print or I_log:
                tempMsg = "\tDomainTech"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            tableDomainTech_init = initTable_DomainTech()
            if os.path.exists(folderPath_root + folderPath_tables + fileName_table_DomainTech):
                tableDomainTech = pandas.read_csv(
                    folderPath_root + folderPath_tables + fileName_table_DomainTech,
                    index_col=0, header=0, sep=',', encoding='utf-8')
            else:
                tableDomainTech = copy.deepcopy(tableDomainTech_init)

            #   Tech
            if I_print or I_log:
                tempMsg = "\tTech"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            tableTech_init = initTable_Tech()
            if os.path.exists(folderPath_root + folderPath_tables + fileName_table_Tech):
                tableTech = pandas.read_csv(
                    folderPath_root + folderPath_tables + fileName_table_Tech,
                    index_col=0, header=0, sep=',', encoding='utf-8')
            else:
                tableTech = copy.deepcopy(tableTech_init)

            #   TechTag
            if I_print or I_log:
                tempMsg = "\tTechTag"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            tableTechTag_init = initTable_TechTag()
            if os.path.exists(folderPath_root + folderPath_tables + fileName_table_TechTag):
                tableTechTag = pandas.read_csv(
                    folderPath_root + folderPath_tables + fileName_table_TechTag,
                    index_col=0, header=0, sep=',', encoding='utf-8')
            else:
                tableTechTag = copy.deepcopy(tableTechTag_init)

            #   TechCat
            if I_print or I_log:
                tempMsg = "\tTechCat"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            tableTechCat_init = initTable_TechCat()
            if os.path.exists(folderPath_root + folderPath_tables + fileName_table_TechCat):
                tableTechCat = pandas.read_csv(
                    folderPath_root + folderPath_tables + fileName_table_TechCat,
                    index_col=0, header=0, sep=',', encoding='utf-8')
            else:
                tableTechCat = copy.deepcopy(tableTechCat_init)

            # Finished initializing
            # Get data from BW response files
            #   Be mindful that some reporting of errors or warnings was done earlier!
            if I_print or I_log:
                tempMsg = "Loading BuiltWith response data"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            for iRow in range(0, datOrg.shape[0]):
                if datOrg['I_response_bw'].iloc[iRow] == 1 and \
                            isinstance(datOrg['name_bw'].iloc[iRow], str) and \
                            len(datOrg['name_bw'].iloc[iRow]) > 0:

                    I_match_DT_lookup = tableDomainTech["id_lookup"] == datOrg["id_lookup"].iloc[iRow]
                    I_match_DT_lookup_any = any(I_match_DT_lookup)

                    # Load data
                    hFile = _io.open(folderPath_root + folderPath_BW + datOrg['filename_bw'].iloc[iRow])
                    datJ = json.loads(hFile.read())
                    hFile.close()

                    # Validate that the response has data
                    if not datJ['Errors']:
                        if len(list(datJ['Results'])) == 1:
                            iLU = 0
                        else:
                            iLU = -1
                            for i in range(0, len(list(datJ['Results']))):
                                if datJ['Results'][i]['Lookup'].lower() == datOrg['domain'].iloc[iRow]:
                                    iLU = i
                                    break

                        if iLU != -1:
                            for iPaths in range(0, len(list(datJ['Results'][iLU]['Result']['Paths']))):
                                for iTech in range(0, len(list(datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'])) ):
                                    # Get tech, tag and category
                                    I_addName = False
                                    techName = strCleanup1(
                                            datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Name'] )
                                    detectedFirst = int( datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                             'FirstDetected'] )/1000
                                    detectedLast = int( datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                            'LastDetected'] )/1000

                                    I_addTag = False
                                    techTag = datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag']
                                    if techTag is None:
                                        techTag = "none"
                                    else:
                                        techTag = strCleanup1(techTag)

                                    techCatList = datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Categories']
                                    if techCatList is None:
                                        techCatList = "None"
                                    if isinstance(techCatList, list):
                                        for iCat in range(0, len(techCatList)):
                                            techCatList[iCat] = strCleanup1(techCatList[iCat])
                                    else:
                                        techCatList = [strCleanup1(techCatList)]


                                    # Want to convert name, tag and cat to IDs
                                    #   Initialize IDs for tag and category
                                    id_tech = None
                                    id_tag = None
                                    idList_cat = [None]*len(techCatList)

                                    # Check to see if we have the Tag
                                    I_match = tableTechTag["name_techTag"] == techTag
                                    if not any(I_match):
                                        I_addTag = True
                                    else:
                                        id_tag = int( tableTechTag["id_techTag"].iloc[numpy.where(I_match)[0]] )


                                    if I_addTag:
                                        id_tag = 1 + max(tableTechTag["id_techTag"])
                                        tableTechTag_init["id_techTag"].iloc[0] = id_tag
                                        tableTechTag_init["name_techTag"].iloc[0] = techTag

                                        tableTechTag = tableTechTag.append(tableTechTag_init)


                                    # Check to see if we have the Category
                                    for iCat in range(0, len(techCatList)):
                                        I_addCat = False

                                        # Search for match
                                        I_match = tableTechCat["name_techCat"] == techCatList[iCat]
                                        if not any(I_match):
                                            I_addCat = True
                                        else:
                                            idList_cat[iCat] = int( tableTechCat["id_techCat"].iloc[numpy.where(I_match)[0]] )

                                        if I_addCat:
                                            idList_cat[iCat] = 1 + max(tableTechCat["id_techCat"])
                                            tableTechCat_init["id_techCat"].iloc[0] = idList_cat[iCat]
                                            tableTechCat_init["name_techCat"].iloc[0] = techCatList[iCat]

                                            tableTechCat = tableTechCat.append(tableTechCat_init)

                                    # Check to see if we have the Tech name
                                    I_match = tableTech["name_tech"]==techName
                                    if not any(I_match):
                                        I_addName = True
                                    else:
                                        # Only take the first one as multiple categories = multiple rows of same tech name
                                        idxListTech = numpy.where(I_match)[0]
                                        idxTech = idxListTech[0]
                                        id_tech = tableTech["id_tech"].iloc[idxTech]

                                    if I_addName:
                                        # Add tech to tableTech
                                        id_tech = 1 + max(tableTech["id_tech"])
                                        for iCat in range(0, len(idList_cat)):
                                            tableTech_init["id_tech"].iloc[0] = id_tech
                                            tableTech_init["name_tech"].iloc[0] = techName
                                            tableTech_init["id_techTag"].iloc[0] = id_tag
                                            tableTech_init["id_techCat"].iloc[0] = idList_cat[iCat]

                                            tableTech = tableTech.append(tableTech_init)

                                    # Check
                                    if not I_addName and (I_addTag or I_addCat):
                                        if I_print or I_log:
                                            tempMsg = "\tAlready had name but new Tag or Cat!\n\t\t" + techName + \
                                                "\n\t\tiRow=" + str(iRow+1)
                                            if I_print:
                                                print(tempMsg)
                                            if I_log:
                                                hFileLog.write(tempMsg + "\n")

                                    # Add to DomainTech?
                                    I_addDomainTech = False

                                    # id_lookup = datOrg["id_lookup"].iloc[iRow]
                                    # I_match = tableDomainTech["id_lookup"] == id_lookup
                                    if not I_match_DT_lookup_any:
                                        I_addDomainTech = True
                                    else:
                                        I_match = tableDomainTech["id_tech"].iloc[ numpy.where(I_match_DT_lookup)[0] ] == id_tech
                                        if not any(I_match):
                                            I_addDomainTech = True

                                    if I_addDomainTech:

                                        I_match_DT_lookup = numpy.append(I_match_DT_lookup, True)
                                        if not I_match_DT_lookup_any:
                                            I_match_DT_lookup_any = True

                                        tableDomainTech_init["id_lookup"].iloc[0] = datOrg["id_lookup"].iloc[iRow]
                                        tableDomainTech_init["id_tech"].iloc[0] = id_tech
                                        tableDomainTech_init["DetectedFirst"].iloc[0] = detectedFirst
                                        tableDomainTech_init["DetectedLast"].iloc[0] = detectedLast


                                        tableDomainTech_init["DetectedFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                            detectedFirst).date().strftime(dateFormat)
                                        tableDomainTech_init["DetectedLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                            detectedLast).date().strftime(dateFormat)

                                        tableDomainTech = tableDomainTech.append(tableDomainTech_init)

                                    elif id_tech > 0:
                                        # Already have this Tech associated with this Domain
                                        #   Meta data may be different

                                        # What row are we at?
                                        rowDT = numpy.arange(len(I_match_DT_lookup))[
                                            numpy.where(I_match_DT_lookup)[0] ][
                                            numpy.where(I_match)[0] ][0]

                                        # Verify meta data
                                        if detectedFirst < tableDomainTech["DetectedFirst"].iloc[rowDT]:
                                            for idx in idxListTech:
                                                tableDomainTech["DetectedFirst"].iloc[rowDT] = detectedFirst
                                                tableDomainTech["DetectedFirst_date"].iloc[rowDT] = datetime.datetime.fromtimestamp(
                                                    detectedFirst).date()
                                        if detectedLast > tableDomainTech["DetectedLast"].iloc[rowDT]:
                                            for idx in idxListTech:
                                                tableDomainTech["DetectedLast"].iloc[rowDT] = detectedLast
                                                tableDomainTech["DetectedLast_date"].iloc[rowDT] = datetime.datetime.fromtimestamp(
                                                    detectedLast).date()

                        pandas.options.mode.chained_assignment = 'warn'

            # Save progress
            if I_print or I_log:
                tempMsg = "Saving BuiltWith tables"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            datOrg.to_csv(folderPath_root + folderPath_data + fileName_CB, sep=',', encoding='utf-8')
            if I_print or I_log:
                tempMsg = "\tDone"
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            tableDomainTech.to_csv(folderPath_root + folderPath_tables + fileName_table_DomainTech, sep=',', encoding='utf-8')
            tableTech.to_csv(folderPath_root + folderPath_tables + fileName_table_Tech, sep=',', encoding='utf-8')
            tableTechTag.to_csv(folderPath_root + folderPath_tables + fileName_table_TechTag, sep=',', encoding='utf-8')
            tableTechCat.to_csv(folderPath_root + folderPath_tables + fileName_table_TechCat, sep=',', encoding='utf-8')



    # Ready for SimilarWeb data
    if I_print or I_log:
        tempMsg = "Loading SimilarWeb data"
        if I_print:
            print(tempMsg)
        if I_log:
            hFileLog.write(tempMsg + "\n")

    tableVisit_init = initTable_Visit()
    if os.path.exists(folderPath_root + folderPath_tables + fileName_table_Visit):
        tableVisit = pandas.read_csv(
            folderPath_root + folderPath_tables + fileName_table_Visit,
            index_col=0, header=0, sep=',', encoding='utf-8')
    else:
        tableVisit = copy.deepcopy(tableVisit_init)

    if not os.path.exists(folderPath_root + folderPath_SW + fileName_dat_SW):
        if I_print or I_log:
            tempMsg = "\tError: Could not find dat file for SimilarWeb responses!\n\t" + \
                      folderPath_root + folderPath_SW + fileName_dat_SW
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
    else:
        for iRow in range(0, datOrg.shape[0]):
            if datOrg['I_response_sw'].iloc[iRow] == 1:
                # Load data
                hFile = _io.open(folderPath_root + folderPath_SW + datOrg['filename_sw'].iloc[iRow])
                datJ = json.loads(hFile.read())
                hFile.close()

                # Ensure there are no errors


                # Get id_lookup and granularity
                id_lookup = datOrg['id_lookup'].iloc[iRow]
                granularity = datJ['meta']['request']['granularity']

                # Prepare I_match for id_lookup and granularity
                #   If we add to the table, we can simply add a TRUE
                I_match_lookupANDgran = numpy.logical_and(
                    tableVisit["id_lookup"] == id_lookup,
                    tableVisit["visit_granularity"] == granularity )

                for iVisit in range(0, len(datJ['visits'])):
                    visitCt = datJ['visits'][iVisit]['visits']
                    visitDate = datetime.datetime.strptime(datJ['visits'][iVisit]['date'], "%Y-%m-%d").date()

                    # Verify we don't already have this data
                    I_add = False
                    if not any(I_match_lookupANDgran):
                        I_add = True
                    else:
                        I_match = tableVisit["visit_date"].iloc[numpy.where(I_match_lookupANDgran)[0]] == visitDate
                        if not any(I_match):
                            I_add = True

                    if I_add:
                        if tableVisit["id_lookup"].iloc[0] == 0:
                            tableVisit["id_lookup"].iloc[0] = id_lookup
                            tableVisit["visit_date"].iloc[0] = visitDate
                            tableVisit["visit_count"].iloc[0] = visitCt
                            tableVisit["visit_granularity"].iloc[0] = granularity
                            I_match_lookupANDgran.iloc[0] = True
                        else:
                            tableVisit_init["id_lookup"].iloc[0] = id_lookup
                            tableVisit_init["visit_date"].iloc[0] = visitDate
                            tableVisit_init["visit_count"].iloc[0] = visitCt
                            tableVisit_init["visit_granularity"].iloc[0] = granularity

                            tableVisit = tableVisit.append(tableVisit_init)
                            I_match_lookupANDgran = numpy.append(I_match_lookupANDgran, True)

        tableVisit.to_csv(folderPath_root + folderPath_tables + fileName_table_Visit, sep=',', encoding='utf-8')




    return
if __name__ == '__main__':
    main()