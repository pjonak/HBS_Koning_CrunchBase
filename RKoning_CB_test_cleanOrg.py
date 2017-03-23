import os
import csv # read CrunchBase CSV files, for cleaning
import time
import _io # file opening and writing
import pandas

folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

folderPath_data = "dataset\\crunchbase_2017_02_06\\"

fileName_data = "organizations"
# fileName_data = "organizations_sample"



def cleanData_CB(filename: str) -> (bool, str):
    # Generic function to receive CrunchBase CSV files for cleaning
    #   Identify which CSV file was given, then go to specific cleaning operations
    if len(filename)>=3 and filename[0:3]=='org':
        # Organizations.csv or Organizations_Descriptions.csv
        #   Assume only Organizations.csv for now
        return cleanData_CB__org(filename)
    else:
        return False, ""

def cleanData_CB__org(filename: str) -> (bool, str):
    # Cleaning function for specific CrunchBase CSV file
    #   ORGANIZATIONS.CSV

    # Open file plus a clean file to receive the cleaned data
    hFileRead, hFileWrite, filename_clean = cleanData_CB__openFiles(filename)

    # Load CSV reader and writer objects
    hReader = csv.reader(hFileRead, delimiter=',')
    hWriter = csv.writer(hFileWrite, delimiter=',', lineterminator='\n')

    # Get first row - headers
    headerData = next(hReader)
    hWriter.writerow(headerData)

    # Get data
    iRow = 0
    while True:
        try:
            # Increment counter / row number
            iRow = iRow + 1

            # Read row
            rowData = next(hReader)

            # Clean lines
            #       Col 18 = "first_funding_on"
            #           Meant to be DATE data
            #           Some entries don't make sense
            #               e.g. 0001-01-01 BC
            #           Some needs to be reformated
            #               e.g. 0023-06-16 is probably 23/06/2016
            #       Col 19 = "last_funding_on"
            #           Meant to be DATE data
            #           Some entries don't make sense
            #               e.g. 0001-01-01 BC
            #       Col 21 = employee_count
            #           Meant to be STRING data representing integer ranges
            #                   1-10
            #                   11-50
            #                   51-100
            #                   101-500
            #           Jan-10 is actually 1-10
            #           Nov-50 is actually 11-50

            iCol = 17
            if len(rowData[iCol]) > 6 and (rowData[iCol][4] == '-' or rowData[iCol][5] == '-'):
                rowData[iCol] = cleanData_CB__Date2Date(rowData[iCol])

            iCol = 18
            if len(rowData[iCol]) > 6 and (rowData[iCol][4] == '-' or rowData[iCol][5] == '-'):
                rowData[iCol] = cleanData_CB__Date2Date(rowData[iCol])

            iCol = 19
            if len(rowData[iCol]) > 6 and (rowData[iCol][4] == '-' or rowData[iCol][5] == '-'):
                rowData[iCol] = cleanData_CB__Date2Date(rowData[iCol])

            iCol = 21
            if len(rowData[iCol]) > 2:
                if not rowData[iCol][0].isdigit() or not rowData[iCol][-1].isdigit():
                    rowData[iCol] = cleanData_CB__Date2Range(rowData[iCol])

            # Save to clean file
            try:
                hWriter.writerow(rowData)
            except:
                "Error writing at " + str(iRow)


        except csv.Error:
            print("Error at " + str(iRow))
        except StopIteration:
            break

    # Clsoe files
    cleanData_CB__closeFiles(hFileRead, hFileWrite)

    return True, filename_clean



def cleanData_CB__Date2Range(strIn: str) -> str:
    # Specific to CrunchBase data - ORGANIZATIONS.CSV
    #   employee count is given as a range
    #       e.g. 1-10
    #   the CSV table shows this as 'Jan-10' or '10-Jan'

    # Given either 'Nov-50' or '10-Jan'
    if strIn[0] == 'J' or strIn[-1] == 'n' or strIn[-1] == 'y':
        return "1-10"
    elif strIn[0] == 'N' or strIn[-1] == 'v' or strIn[-1] == 'r':
        return "11-50"
    else:
        print("Clean_Dat2Range: " + strIn)
        return "NaN"



def cleanData_CB__Date2Date(strIn: str) -> str:
    # Specific to CrunchBase data - ORGANIZATIONS.CSV
    #   DATE entries are usually given as
    #       DD / MM / YY
    #   clean those which do not follow this syntax

    if not strIn[-1].isdigit():
        # Some bizarre entries include "0001 - 01 - 01 BC"
        return ""
    else:
        # Assume syntax is
        #   00DD - MM - YY
        # or
        #   YYYY - MM - DD

        # Delimit
        temp = strIn.split('-',2)
        # Remove potential whitespace
        for i in range(0,3):
            temp[i] = temp[i].strip()

        if strIn[0] == '0':
            # Get day and month
            d = temp[0][2:4]
            m = temp[1]

            # Year is trickier as we are given the last 2 digits
            #   Get current year with time.strftime("%Y")
            #           = '2017'
            #   Take last two digits, covert to integer, compare to current date
            if int(temp[2]) <= int(time.strftime("%Y")[2:4]):
                y = "20" + temp[2]
            else:
                y = "19" + temp[2]

        else:
            d = temp[2]
            m = temp[1]
            y = temp[0]

        return d + "/" + m + "/" + y



def cleanData_CB__openFiles(filename: str) -> (_io.TextIOWrapper, _io.TextIOWrapper, str):
    # Generic function to open CrunchBase CSV files along with new CSV files to receive cleaned data
    return open(folderPath_root + folderPath_data + filename + ".csv", encoding="utf8"), \
           open(folderPath_root + folderPath_data + filename + "_cleaned.csv", 'w', encoding="utf8"), \
           filename + "_cleaned"

def cleanData_CB__closeFiles(hFileRead: _io.TextIOWrapper, hFileWrite: _io.TextIOWrapper):
    # Generic function to close files associated with CrunchBase cleaning routine
    hFileRead.close()
    hFileWrite.close()
    return



def main():
    print("Loading data: " + fileName_data)
    I_flag, filename = cleanData_CB(fileName_data)

    print("Done")
    print("\tClean flag = " + str(I_flag))
    print("\tClean filename = " + filename)


    return
if __name__ == '__main__':
    main()