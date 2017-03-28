import os
import copy
import pandas
import numpy
# import matplotlib.pyplot as plt
import datetime
import random
import time
import json

folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

folderPath_data = "dataset\\crunchbase_2017_02_06\\"

fileName = "organizations"
# fileName = "organizations_sample"
fileName_data = fileName + "_load_v3_part2_v3.csv"
fileName_save = fileName + "_load_v3_part2_v3__trim.csv"


I_sample = False
nSample = 2000

I_histograms = False

I_isolate_yr = True
isolate_yr_colList = ['founded_on']
isolate_yr_start = datetime.datetime.strptime('01/01/2008','%m/%d/%Y')
isolate_yr_end = datetime.datetime.strptime('12/31/2016','%m/%d/%Y')



# Which columns are we interested in?
#   first_funding_on and last_funding_on are the same
# colList = ['employee_count', 'first_funding_on', 'last_funding_on',
#            'founded_on','facebook_url']
colList_main = ['employee_count', 'first_funding_on', 'founded_on', 'facebook_url']


#   Identify which columns need particular attention paid
#       using JSON
colTypeList = '{"founded_on": "date", "first_funding_on": "date"}'



def getIdxList_validEntry(datColumn: pandas.core.frame.DataFrame, idxList: list = None) -> list:
    # Initialize idxList
    if idxList is None:
        idxList = [0] * len(datColumn)

    # Go through rows looking for empty data
    #   -> turn idxList[idx] to False

    if len(str(datColumn.dtype)) > 3 and str(datColumn.dtype)[0:4] == 'date':
        i = 0
        i2 = 0

        for datRow in datColumn:
            # What are the different ways the entry could be empty?
            if pandas.isnull(datRow):
                i = i + 1
            else:
                # Not empty, record index
                idxList[i2] = i
                i = i + 1
                i2 = i2 + 1

        # Did we find any valid values?
        if i2 == 0:
            return None
        else:
            return idxList[0:i2]

    else:

        i = 0
        i2 = 0

        for datRow in datColumn:
            # What are the different ways the entry could be empty?
            if pandas.isnull(datRow):
                i = i + 1
            elif len(datRow)==0:
                i = i + 1
            elif len(datRow)==1 and datRow=="-":
                i = i + 1
            else:
                # Not empty, record index
                idxList[i2] = i
                i = i + 1
                i2 = i2 + 1

        # Did we find any valid values?
        if i2 == 0:
            return None
        else:
            return idxList[0:i2]



def getIdxList_missingCol(dat: pandas.core.frame.DataFrame,
                          colList: list,
                          colName_comp: str) -> \
        pandas.core.frame.DataFrame:

    # Initialize
    idxList = [None] * len(colList)
    ctList = [0] * len(colList)

    # Go through each column and idxList from one column to the next
    i = 0
    for colName in colList:
        if colName == colName_comp:
            # No sense in comparing idxList to itself
            idxList[i] = list()
            ctList[i] = 0
        else:
            # Get distinct values via
            #   set( idxList of interest ) - set( reference idxList )
            idxList[i] = list(set(dat["idxList"][colName]) - set(dat["idxList"][colName_comp]))
            ctList[i] = len(idxList[i])
        i = i + 1

    # Save to dat, with headers
    dat["Missing " + colName_comp] = idxList
    dat["missCt " + colName_comp] = ctList

    return dat



def getIdxList_trimEmpCt(datColumn: pandas.core.frame.DataFrame, idxList: list) -> list:

    # We know there are no empty values so only remove those with less than 10 employees
    #   employee_count = '1-10'
    for i in range(0,len(idxList))[::-1]:
        if len(datColumn.iloc[idxList[i]]) == 4 and datColumn.iloc[idxList[i]][0:4] == '1-10':
            del idxList[i]

    return idxList


def getUniqueValues(datColumn: pandas.core.frame.DataFrame, idxList: list) -> list:
    # Go through dat to find all possible values for this column
    valList = [datColumn.iloc[idxList[0]]]

    for iIdx in range(1,len(idxList)):
        # current value is
        #       datColumn[idxList[iIdx]]
        # compare to what we have in valList
        for iVal in range(0,len(valList)):

            if datColumn.iloc[idxList[iIdx]] == valList[iVal]:
                # Have value
                break

            elif iVal == len(valList)-1:
                # At the last possible value in valList and wasn't a match
                #   Add new entry to valList
                valList = valList + [datColumn.iloc[idxList[iIdx]]]

    return valList



def enum_EmpCt(datColumn: pandas.core.frame.DataFrame, idxList: list) -> (list, list):
    # Get employee_count option list
    empCt_str = getUniqueValues(datColumn, idxList)
    # Re-organize by length
    empCt_str.sort(key=len)
    #   there are some unexpected value ranges
    #       need more careful sorting
    I_flag = True
    while I_flag:
        I_flag = False
        for iStr in range(0, len(empCt_str) - 1):
            if len(empCt_str[iStr]) == len(empCt_str[iStr + 1]):
                valRange_1 = [empCt_str[iStr].split("-", 1)[0], empCt_str[iStr].split("-", 1)[1]]
                valRange_2 = [empCt_str[iStr + 1].split("-", 1)[0], empCt_str[iStr + 1].split("-", 1)[1]]

                if valRange_1[0] > valRange_2[0] or \
                        (valRange_1[0] == valRange_2[0] and valRange_1[1] > valRange_2[1]):
                    I_flag = True
                    temp = empCt_str[iStr]
                    empCt_str[iStr] = empCt_str[iStr + 1]
                    empCt_str[iStr + 1] = temp
                    break

    return empCt_str, list(range(0,len(empCt_str)))



def main():

    pandas.options.mode.chained_assignment = None


    # Load data
    print("Loading data")
    print("\t" + folderPath_root + folderPath_data + fileName_data)
    datOrig = pandas.read_csv(folderPath_root + folderPath_data + fileName_data, index_col=0)
    print("\tNumber of rows = " + str(len(datOrig)) )

    # Remove entries without homepage url
    print("Removing entries without homepage URLs")
    datOrig = datOrig.iloc[ getIdxList_validEntry(datOrig['homepage_url'], [0] * len(datOrig)) ,:]
    print("\tNumber of rows = " + str(len(datOrig)))


    # Take a sample
    if I_sample:
        print("Taking a sample of " + str(nSample) + " companies")
        datOrig = datOrig.iloc[ sorted(random.sample(range(0,len(datOrig)),nSample)) ,:]
        print("\tNumber of rows = " + str(len(datOrig)))



    # Isolate companies with any employee count
    print("Remove companies with no employee_count")
    dat = copy.deepcopy(datOrig).iloc[ numpy.where( datOrig['enum_employee_count'] >= 0 )[0] ]

    # Make sure date columns are in the right datatype
    print("Converting date columns to datetime")
    colType_json = json.loads(colTypeList)
    for colName in colType_json:
        if colType_json[colName] == "date":
            if colName in datOrig.columns:
                for iRow in range(0, dat.shape[0]):
                    if isinstance(dat[colName].iloc[iRow], float):
                        dat[colName].iloc[iRow] = None
                    else:
                        try:
                            dat[colName].iloc[iRow] = pandas.to_datetime(dat[colName].iloc[iRow])
                        except:
                            dat[colName].iloc[iRow] = None
    print(dat.shape[0])
    # Isolate by year
    if I_isolate_yr:
        print("Isolating by year")
        for colIsolate in isolate_yr_colList:
            if colIsolate in dat.columns:
                dat = dat.iloc[numpy.where(
                    (dat[colIsolate].dt.year <= isolate_yr_end.year) &
                    (dat[colIsolate].dt.year >= isolate_yr_start.year))[0]]
    print( dat.shape[0] )

    # Save to file
    print("Saving to file")
    dat.to_csv(folderPath_root + folderPath_data + fileName_save, sep=',', encoding='utf-8')
    print("Done")

    return

if __name__ == '__main__':
    main()