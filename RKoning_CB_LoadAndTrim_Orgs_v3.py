import os
import csv # read CrunchBase CSV files, for cleaning
import _io # file opening and writing
import pandas
import numpy
import json
import time


I_yr_cutoff = True
yr_cutoff = 1678


# Get current directory of where this code file is
# Get project root directory by going to the parent directory
folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

# Identify where data is held
folderPath_data = "dataset\\crunchbase_2017_02_06\\"

# Which file are we looking to load?
fileName = "organizations"
# fileName = "organizations_sample"

# What are the suffixes we will be using?
suffix_load = "load_v3"
suffix_enum = "part2_v3"

# What are the columns we are interested in?
#   Columns to keep
colList_main = ['company_name','domain','homepage_url',
                'short_description','category_list','category_group_list',
                'employee_count', 'first_funding_on',
               'founded_on', 'facebook_url',
                'closed_on','uuid']

#   Identify which columns need particular attention paid
#       using JSON
colTypeList = '{"founded_on": "date", "first_funding_on": "date", ' + \
              '"employee_count": "range"}'

#   Columns for enumeration
#       ! Must be in colTypeList !
colList_enum = ['employee_count']

#   Columns for exploratory analysis
colList_eda = ['employee_count', 'first_funding_on',
               'founded_on', 'facebook_url']





def CB_colName_2_colIdx(colList: list, matchList: list) -> list:
    # Initialize output
    idxList = [-1]*len(colList)
    # Go through each entry in colList and find match
    iCol = 0
    for colName in colList:
        iMatch = 0
        for matchName in matchList:
            if colName == matchName:
                idxList[iCol] = iMatch
                break
            iMatch += 1
        iCol += 1
    return idxList

def CB_colName_2_colIdx__p2(idxList: list) -> list:
    idxList2 = [None]
    iIdx = 0
    iIdx2 = 0
    for idx in idxList:
        if idx != -1:
            if idxList2[0] is None:
                idxList2[0] = iIdx
            else:
                idxList2 = idxList2 + [iIdx]
            iIdx2 += 1
        iIdx += 1
    return sorted(idxList2)

def CB_colName_2_colIdx__pType(colList: list, matchJSON: dict) -> list:
    # Go through each item in colList to find
    idxList = [None]
    iCol = 0
    for colName in colList:
        if colName in matchJSON:
            if idxList[0] is None:
                idxList[0] = iCol
            else:
                idxList = idxList + [iCol]
        iCol += 1
    return idxList



def CB_specialColType(datRow: numpy.ndarray, datHeader: list,
                      colType_json: dict, colIdxList_type: list) -> numpy.ndarray:
    for idx in colIdxList_type:
        if datRow[idx] is not None and len(datRow[idx]) > 0:
            colType = colType_json[datHeader[idx]]

            if colType == "date":
                datRow[idx] = CB_specialColType_date(datRow[idx])
            elif colType == "range":
                datRow[idx] = CB_specialColType_range(datRow[idx])

    return datRow

def CB_specialColType_date(dat: str) -> str:
    # The format we want is MM/DD/YYYY
    #   Sometimes we get
    #       YYYY-MM-DD
    #       00DD-MM-YY
    #
    I_valid = False
    # Remove excess white space
    dat = dat.replace(" ", "")
    # Ensure length can hold enough information and is not a bizarre entry such as "0001 - 01 - 01 BC"
    if len(dat) >= 6 and dat[-1].isdigit():
        if dat.find("/") != -1:
            # Either
            #   MM / DD / YYYY
            #       or
            #   DD / MM / YYYY
            #   YYYY / MM / DD
            if dat[1] == "/" or dat[2] == "/":
                # Assume correct ( MM / DD / YYYY )
                try:
                    temp = dat.split('/', 2)
                    dat = str(int(temp[0])) + "/" + str(int(temp[1])) + "/" + str(int(temp[2]))
                    I_valid = True
                except:
                    print("error converting to date: " + dat)

            elif dat[4] == "/" and dat[0] != 0:
                # Assume YYYY / MM / DD
                try:
                    temp = dat.split('/', 2)
                    dat = str(int(temp[1])) + "/" + str(int(temp[2])) + "/" + str(int(temp[0]))
                    I_valid = True
                except:
                    print("error converting to date: " + dat)

        elif dat.find("-") != -1:
            # Either
            #   00??-??-??
            #       or
            #   YYYY-MM-DD
            if dat[0] != "0":
                # Assume YYYY - MM - DD
                try:
                    temp = dat.split('-', 2)
                    dat = str(int(temp[1])) + "/" + str(int(temp[2])) + "/" + str(int(temp[0]))
                    I_valid = True
                except:
                    print("error converting to date: " + dat)

    if I_valid:
        if I_yr_cutoff and int(dat[::-1].split('/', 1)[0][::-1]) < yr_cutoff:
            return "1/1/" + str(yr_cutoff)
        else:
            return dat
    else:
        return ""

def CB_specialColType_range(dat: str) -> str:
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
                print("CB_specialColType_range: Unknown range: " + dat)
                return ""
        else:
            return dat
    else:
        return ""


def CB_enumerateColumn(datCol: pandas.core.frame.DataFrame, colType: str) -> list:
    if colType == "range":
        # Get unique list of ranges
        enumList_str = [None]
        I_empty = True
        for datRow in datCol:
            if datRow is not None and isinstance(datRow, str) and len(datRow) > 1:
                if I_empty:
                    enumList_str[0] = datRow
                    I_empty = False
                else:
                    I_match = False
                    for enumStr in enumList_str:
                        if len(enumStr) == len(datRow) and enumStr == datRow:
                            I_match = True
                            break
                    if not I_match:
                        enumList_str = enumList_str + [datRow]

        # Sort
        #   Re-organize by length
        enumList_str.sort(key=len)
        #   there are some unexpected value ranges
        #       need more careful sorting
        I_flag = True
        while I_flag:
            I_flag = False
            for iStr in range(0, len(enumList_str) - 1):
                if len(enumList_str[iStr]) == len(enumList_str[iStr + 1]):
                    valRange_1 = [enumList_str[iStr].split("-", 1)[0], enumList_str[iStr].split("-", 1)[1]]
                    valRange_2 = [enumList_str[iStr + 1].split("-", 1)[0], enumList_str[iStr + 1].split("-", 1)[1]]

                    if valRange_1[0] > valRange_2[0] or \
                            (valRange_1[0] == valRange_2[0] and valRange_1[1] > valRange_2[1]):
                        I_flag = True
                        temp = enumList_str[iStr]
                        enumList_str[iStr] = enumList_str[iStr + 1]
                        enumList_str[iStr + 1] = temp
                        break

        enumList_str = numpy.asarray(enumList_str)
        enumList_val = numpy.asarray(list(range(0,len(enumList_str))))

        # Get enumerated value for this column
        datEnum = [None]*len(datCol)
        iRow = 0
        for datRow in datCol:
            if datRow is not None and isinstance(datRow, str) and len(datRow) > 1:
                datEnum[iRow] = int( enumList_val[enumList_str==datRow] )
            iRow += 1

        return datEnum

    else:
        # Unknown column
        return None


def main():
    # Verify file exists
    fullPath = folderPath_root + folderPath_data + fileName + '.csv'
    if os.path.exists(fullPath):
        print("File found")
        I_flag = True
    else:
        print("File not found\n\tSearch path: " + fullPath)
        I_flag = False

    if I_flag:
        print("Opening data: " + fileName)
        # We want to stream open the file plus stream save into a new one
        #   Open the files first
        hFile_read = _io.open(folderPath_root + folderPath_data + fileName +
                          ".csv", encoding="utf8")
        hFile_write = _io.open(folderPath_root + folderPath_data + fileName +
                           "_" + suffix_load + ".csv", 'w', encoding="utf8")
        #   Load CSV reader and writer objects
        hReader = csv.reader(hFile_read, delimiter=',')
        hWriter = csv.writer(hFile_write, delimiter=',', lineterminator='\n')
        print("\tDone")

        # Get header information
        print("Identifying headers")
        #   Get headers - first row
        #       get as numpy array, easier to work with
        datHeader = numpy.asarray( next(hReader) )
        #   Identify which columns we are interested in keeping, particular at what index
        colIdxList = CB_colName_2_colIdx(datHeader, colList_main)
        #   Get column indices which have a match
        colIdxList_main = CB_colName_2_colIdx__p2(colIdxList)
        #   Remove excess data
        datHeader = datHeader[colIdxList_main]

        #   Identify which columns have a colType associated and which colIdx that would be
        #       This colIdx is based on the truncated list
        colType_json = json.loads(colTypeList)
        colIdxList_type = CB_colName_2_colIdx__pType(datHeader, colType_json)

        #   Save headers
        hWriter.writerow(datHeader)

        print("\tDone")

        # Ready to move on to the rest of the data
        print("Reading data")
        iRow = 0
        while True:
            iRow += 1
            I_flag = True

            # Read row and truncate
            #   shouldn't use csv reader for input to ensure we can't get past the tricky characters
            #       datLine.encode(encodingRead)[0:-1].decode(encodingWrite, 'ignore')
            #       then break up by commas
            try:
                datRow = numpy.asarray( next(hReader) )[colIdxList_main]

            except csv.Error:
                I_flag = False
                print("\tReading error at row " + str(iRow))
            except StopIteration:
                I_flag = False
                print("\tReached end of data at row " + str(iRow))
                break

            if I_flag:
                # Clean up the special columns, and save
                try:
                    hWriter.writerow(
                        CB_specialColType(datRow, datHeader, colType_json, colIdxList_type) )
                except csv.Error:
                    print("\tWriting error at row " + str(iRow))

        # Close files
        hFile_read.close()
        hFile_write.close()
        print("\tDone reading data")

        # Re-open with pandas
        print("Re-opening data as a DataFrame")
        dat = pandas.read_csv(folderPath_root + folderPath_data + fileName +
                              "_" + suffix_load + ".csv",
                              sep=',', encoding='utf-8', dtype=str)

        # Remove entries that do not have homepage specified
        print("Removing entries that do not have a homepage specified")
        len1 = len(dat['homepage_url'])
        dat = dat.iloc[numpy.arange(len(dat['homepage_url']))[numpy.array(dat['homepage_url'].notnull())]]
        print("\tDone\n\t\tRemoved " + str(len1-len(dat['homepage_url'])) + " entries")


        # Enumerate specified columns
        #   You may get all rows which an enumerated value by:
        #       idxList = dat[ column name ].notnull()
        #
        #       example: To view rows with enumerated employee_count
        #           dat['enum_employee_count'][ dat['enum_employee_count'].notnull() ]
        print("Enumerating specified columns")
        for colName in colList_enum:
            if colName in colType_json:
                tempCol = CB_enumerateColumn(dat[colName], colType_json[colName])

                if tempCol is not None:
                    dat['enum_' + colName] = tempCol

            else:
                print("\tSkipping: You did not define column " + colName + " in the initial JSON string")

        print("Storing data in\n\t\t" +
              folderPath_root + folderPath_data + fileName +
              "_" + suffix_load + "_" + suffix_enum + ".csv" )
        dat.to_csv(folderPath_root + folderPath_data + fileName +
                           "_" + suffix_load + "_" + suffix_enum + ".csv", sep=',', encoding='utf-8')
        print("\tDone")

    return
if __name__ == '__main__':
    main()