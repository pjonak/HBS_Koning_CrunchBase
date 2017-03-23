import os
import copy
import pandas
import numpy
import matplotlib.pyplot as plt
import datetime
import random
import time

folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

folderPath_data = "dataset\\crunchbase_2017_02_06\\"

fileName_data = "organizations_cleaned.csv"
# fileName_data = "organizations_sample_cleaned.csv"



I_sample = True
nSample = 200

I_histograms = False

I_isolate_yr = True
isolate_yr_start = datetime.datetime.strptime('01/01/2008','%m/%d/%Y')
isolate_yr_end = datetime.datetime.strptime('12/31/2015','%m/%d/%Y')



def getIdxList_validEntry(datColumn: pandas.core.frame.DataFrame, idxList: list = None) -> list:
    # Initialize idxList
    if idxList is None:
        idxList = [0] * len(datColumn)

    # Go through rows looking for empty data
    #   -> turn idxList[idx] to False
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
    datOrig = pandas.read_csv(folderPath_root + folderPath_data + fileName_data)
    print("\tNumber of rows = " + str(len(datOrig)) )

    # Remove entries without homepage url
    print("Removing entries without homepage URLs")
    # datOrig = datOrig.irow(
    #     getIdxList_validEntry(datOrig['homepage_url'], [0] * len(datOrig)) )
    idxList = getIdxList_validEntry(datOrig['homepage_url'], [0] * len(datOrig))
    datOrig = datOrig.iloc[idxList,:]
    print("\tNumber of rows = " + str(len(datOrig)))



    # Take a sample
    if I_sample:
        print("Taking a sample of " + str(nSample) + " companies")
        datOrig = datOrig.iloc[ sorted(random.sample(range(0,len(datOrig)),nSample)) ,:]
        print("\tNumber of rows = " + str(len(datOrig)))



    # Which columns are we interested in?
    #   first_funding_on and last_funding_on are the same
    # colList = ['employee_count', 'first_funding_on', 'last_funding_on',
    #            'founded_on','facebook_url']
    colList = ['employee_count', 'first_funding_on',
               'founded_on', 'facebook_url']

    # Get list of companies with entries in each column of interest
    print("Getting list of companies with entries in each column of interest")
    #   Initialize
    idxList = [0] * len(datOrig)
    datIdx = pandas.DataFrame(index=colList,columns=["idxList", "count"])

    for colName in colList:
        datIdx["idxList"][colName] = getIdxList_validEntry(datOrig[colName], idxList)
        datIdx["count"][colName] = len( datIdx["idxList"][colName] )


    # Get all indices in each column which don't appear in the other columns
    print("Get missing indices")
    for colName in colList:
        print("\t" + colName)
        datIdx = getIdxList_missingCol(datIdx,colList,colName)

    # # print(dat)
    # print(dat[['count',
    #            'missCt employee_count',
    #            'missCt first_funding_on',
    #            'missCt founded_on',
    #            'missCt facebook_url']])

    # Update to reflect employee_count > 10
    print("Get employee_count > 10")
    datIdx2 = copy.deepcopy( datIdx[['idxList','count']] )
    datIdx2['idxList']['employee_count'] = getIdxList_trimEmpCt(
        datOrig['employee_count'],
        datIdx2['idxList']['employee_count'] )
    datIdx2['count']['employee_count'] = len( datIdx2['idxList']['employee_count'] )

    print("Get missing indices: Round 2: employee_count > 10")
    for colName in colList:
        print("\t" + colName)
        datIdx2 = getIdxList_missingCol(datIdx2, colList, colName)

    # # print(dat2)
    # print(dat2[['count',
    #            'missCt employee_count',
    #            'missCt first_funding_on',
    #            'missCt founded_on',
    #            'missCt facebook_url']])







    print("Enumerating employee_count")
    empCt_str, empCt_idx = enum_EmpCt(datOrig['employee_count'], datIdx2['idxList']['employee_count'])
    empCt_r = range(0,len(empCt_str))

    print("\tAdd new column to data")
    dat = copy.deepcopy(datOrig[colList])
    dat['enum_empCt'] = pandas.Series()
    for iIdx in range(0,len(datIdx2['idxList']['employee_count'])):
        for iEnum in empCt_r:
            if datOrig['employee_count'].iloc[datIdx2['idxList']['employee_count'][iIdx]] == empCt_str[iEnum]:
                dat['enum_empCt'].iloc[datIdx2['idxList']['employee_count'][iIdx]] = iEnum
                break



    print("Binning founded_on by year")
    print("\tIsolating year")
    dat['YR__founded_on'] = pandas.Series()
    for iIdx in range(0,len(datIdx2['idxList']['founded_on'])):
        dat['YR__founded_on'].iloc[datIdx2['idxList']['founded_on'][iIdx]] = int(
            dat['founded_on'].iloc[datIdx2['idxList']['founded_on'][iIdx]][::-1].split('/',1)[0][::-1] )

    print("\tGetting bins")
    # Get current year
    cYr = int(time.strftime("%Y"))
    # Interested in year difference, not absolute difference
    foundYr_num = [2,5,10,20,40,80]
    foundYr_r = range(0,len(foundYr_num)) # for the loop ahead
    foundYr_n = len(foundYr_num)-1 # for the loop ahead
    # Get graph ticklabels
    foundYr_str = [None]*len(foundYr_num)
    for iBin in foundYr_r:
        foundYr_str[iBin] = str(cYr-foundYr_num[iBin])

    print("\tBinning")
    # Initialize
    dat['bin_YR__founded_on'] = pandas.Series()
    # Go through each year and see which bin it belongs in
    for iIdx in range(0, len(datIdx2['idxList']['founded_on'])):
        # What is the difference between current year and year of interest?
        dYr = cYr - dat['YR__founded_on'].iloc[datIdx2['idxList']['founded_on'][iIdx]]

        # Compare to bins
        for iBin in foundYr_r:
            if foundYr_num[iBin] >= dYr:
                # Stop, found our bin
                dat['bin_YR__founded_on'].iloc[datIdx2['idxList']['founded_on'][iIdx]] = iBin
                break
            elif iBin == foundYr_n:
                # End of binds
                dat['bin_YR__founded_on'].iloc[datIdx2['idxList']['founded_on'][iIdx]] = iBin+1




    if I_histograms:
        iFig = 0
        print("Plotting histogram - employee_count")
        strTitle = "Histogram of Employee Count (All)"
        iFig = iFig+1
        plt.figure(iFig)
        plt.hist(dat['enum_empCt'].iloc[datIdx2['idxList']['employee_count']],
                 bins=numpy.arange(len(empCt_str))-0.5 )
        plt.ylabel("Frequency [Count]")
        plt.title(strTitle)
        plt.xticks(empCt_r, empCt_str, rotation=20)
        plt.xlabel("Count Ranges")
        # plt.show()

        print("Plotting histogram - employee_count - missing founded_on")
        strTitle = "Histogram of Employee Count (Missing founded_on)"
        iFig = iFig + 1
        plt.figure(iFig)
        plt.hist(dat['enum_empCt'].iloc[datIdx2['Missing founded_on']['employee_count']],
                 bins=numpy.arange(len(empCt_str))-0.5 )
        plt.ylabel("Frequency [Count]")
        plt.title(strTitle)
        plt.xticks(empCt_r, empCt_str, rotation=20)
        plt.xlabel("Count Ranges")
        # plt.show()

        print("Plotting histogram - founded_on")
        strTitle = "Histogram of Founded On, Year (All)"
        iFig = iFig + 1
        plt.figure(iFig)
        plt.hist(dat['bin_YR__founded_on'].iloc[datIdx2['idxList']['founded_on']],
                 bins=numpy.arange(len(foundYr_str))-0.5 )
        plt.ylabel("Frequency [Count]")
        plt.title(strTitle)
        plt.xticks(foundYr_r, foundYr_str, rotation=20)
        plt.xlabel("Founded On or After [Year]")
        # plt.show()

        print("Plotting histogram - founded_on - missing employee_count")
        strTitle = "Histogram of Founded On, Year (Missing employee_count)"
        iFig = iFig + 1
        plt.figure(iFig)
        plt.hist(dat['bin_YR__founded_on'].iloc[datIdx2['Missing employee_count']['founded_on']],
                 bins=numpy.arange(len(foundYr_str))-0.5 )
        plt.ylabel("Frequency [Count]")
        plt.title(strTitle)
        plt.xticks(foundYr_r, foundYr_str, rotation=20)
        plt.xlabel("Founded On or After [Year]")
        # plt.show()


        plt.show()



    datTemp = copy.deepcopy( dat['YR__founded_on'].iloc[
                                 sorted(random.sample(
                                     datIdx2['Missing employee_count']['founded_on'], 20)) ] )
    print(datTemp[datTemp>2007])

    print('done')



    # print("Change date columns to datetime")
    # print(list(dat))
    # print(dat[['first_funding_on','founded_on']])
    # dat['first_funding_on'] = pandas.to_datetime(dat['first_funding_on'])
    # dat['founded_on'] = pandas.to_datetime(dat['founded_on'])
    # print(dat[['first_funding_on', 'founded_on']])

    return

if __name__ == '__main__':
    main()