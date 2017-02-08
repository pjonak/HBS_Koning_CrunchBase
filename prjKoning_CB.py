# Project details:
#   For Rem Koning (faculty)
#
#   CrunchBase provides details on 100,000s of companies
#   SimilarWeb provides page view data
#   BuiltWith provides technology stack of websites
#
#       Combine CrunchBase data with SimilarWeb and BuiltWith data
#
# Author: Paul Jonak (HBS RCS)
# Last edit was 2017 - 02 - 07
#
# Goal of this code:
#   Proof of concept
#       Load organizations.csv
#       Get a sample of 10 large companies with defined homepages
#       Get website stats from SimilarWeb
#       Get website stats from BuiltWith
#       Organize data


# Import modules
import pandas
import csv
import time
import _io
import numpy
import json

# Prepare constants
folderPath_root_cb = "C:\\Users\\pjonak\\Documents\\Projects\\" + \
                     "Koning\\Crunchbase_SimilarWeb_BuiltWith\\"
folderPath_data_cb = folderPath_root_cb + "dataset\\crunchbase_2017_02_06\\"
# folderPath_data_cb = "C:\\Users\\pjonak\\Documents\\Projects\\" + \
#                      "Koning\\Crunchbase_SimilarWeb_BuiltWith\\" + \
#                      "dataset\\crunchbase_2017_02_06\\"




def cleanData_CB(filename: str) -> (bool, str):
    if len(filename)>=3 and filename[0:3]=='org':
        # Organizations.csv or Organizations_Descriptions.csv
        #   Assume only Organizations.csv for now
        return cleanData_CB__org(filename)
    else:
        return False, " "

def cleanData_CB__org(filename: str) -> (bool, str):
    # Open file
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

            iCol = 18
            if len(rowData[iCol]) > 5 and rowData[iCol][4] == '-':
                rowData[iCol] = cleanData_CB__Date2Date(rowData[iCol])

            iCol = 19
            if len(rowData[iCol]) > 5 and rowData[iCol][4] == '-':
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



def cleanData_CB__openFiles(filename: str) -> (_io.TextIOWrapper, _io.TextIOWrapper, str):
    return open(folderPath_data_cb + filename + ".csv", encoding="utf8"), \
           open(folderPath_data_cb + filename + "_cleaned.csv", 'w', encoding="utf8"), \
           filename + "_cleaned"

def cleanData_CB__closeFiles(hFileRead: _io.TextIOWrapper, hFileWrite: _io.TextIOWrapper):
    hFileRead.close()
    hFileWrite.close()
    return


def cleanData_CB__Date2Date(strIn: str) -> str:
    if not strIn[-1].isdigit():
        return ""
    else:
        # Get day and month
        d = strIn[2:4]
        m = strIn[5:7]

        # Year is trickier as we are given the last 2 digits
        #   Get current year with time.strftime("%Y")
        #           = '2017'
        #   Take last two digits, covert to integer, compare to what we have
        if int(strIn[8:10]) <= int(time.strftime("%Y")[2:4]):
            y = "20" + strIn[8:10]
        else:
            y = "19" + strIn[8:10]
        return d + "/" + m + "/" + y

def cleanData_CB__Date2Range(strIn: str) -> str:
    # Given either 'Nov-50' or '10-Jan'
    if strIn[0] == 'J' or strIn[-1] == 'n' or strIn[-1] == 'y':
        return "1-10"
    elif strIn[0] == 'N' or strIn[-1] == 'v' or strIn[-1] == 'r':
        return "11-50"
    else:
        return "invalid"



def orgData_findBy_employees(datColumn: pandas.core.frame.DataFrame, thres: int) -> (int, list):
    idxList = [None]
    iIdx = 0
    iRow = 0
    for rowData in datColumn:
        if len(rowData) > 1 and rowData[0].isdigit():
            if int( rowData.split('-',1)[0] ) > thres:
                if iIdx == 0:
                    idxList[iIdx]=iRow
                else:
                    idxList=idxList+[iRow]
                iIdx=iIdx+1
        iRow=iRow+1
    return iIdx, idxList



def orgData_findBy_homepage(datColumn: pandas.core.frame.DataFrame) -> (int, list):
    idxList = [None]
    iIdx = 0
    iRow = 0
    for rowData in datColumn:
        if rowData is not None and len(rowData) > 1:
            if iIdx==0:
                idxList[iIdx]=iRow
            else:
                idxList=idxList+[iRow]
            iIdx=iIdx+1
        iRow=iRow+1
    return iIdx, idxList



def sw_getKey() -> str:
    hFile = open(folderPath_root_cb + "sw.cnf")
    hReader = csv.reader(hFile,delimiter=',')
    apiKey_sw = None
    for rowData in hReader:
        if rowData[0]=="api_key":
            apiKey_sw = rowData[1]
    hFile.close()
    return apiKey_sw

def sw_buildURL(website: str, sw_getKey: str) -> str:
    # The URL is composed of the following parts:
    #   base / domain = api.similarweb.com
    #   version and type
    #       For version 1, we use /v1/website/
    #       For version 2, we use /v2/website/
    #       For mobile, we use /Mobile/0/
    #   endpoint
    #   api_key
    #   additional parameters
    #       start date
    #       end date
    #       granularity
    #           daily, weekly, monthly
    #       main domain only?
    #       output format, JSON (default) or XML

    url_base = "https://api.similarweb.com/"

    sw_category = "TotalTraffic"
    sw_endpoint = "visits"

    apiKey_sw = sw_getKey

    start_date = "2015-06"
    end_date = "2015-09"

    granularity = "monthly"

    main_domain_only = False

    format = "json"


    # Check
    I_validOpts = True
    if sw_category == "TotalTraffic":
        version_and_type = "v1/website/"
        url_category = "total-traffic-and-engagement/"
        if sw_endpoint == "visits":
            url_endpoint = "visits"
        elif sw_endpoint == "pages_per_visit":
            url_endpoint = "pages-per-visit"
        elif sw_endpoint == "average_visit_duration":
            url_endpoint = "average-visit-duration"
        elif sw_endpoint == "bounce_rate":
            url_endpoint = "bounce-rate"
        elif sw_endpoint == "visits_split":
            url_endpoint = "visits-split"
        else:
            I_validOpts = False
            url_endpoint = None
            print("Unknown endpoint for category " + sw_category)
            print("\t" + sw_endpoint)

    elif sw_category == "DesktopTraffic":
        version_and_type = "v1/website/"
        url_category = "traffic-and-engagement/"
        if sw_endpoint == "visits":
            url_endpoint = "visits"
        elif sw_endpoint == "pages_per_visit":
            url_endpoint = "pages-per-visit"
        elif sw_endpoint == "average_visit_duration":
            url_endpoint = "average-visit-duration"
        elif sw_endpoint == "bounce_rate":
            url_endpoint = "bounce-rate"
        elif sw_endpoint == "global_rank":
            url_category = "global-rank"
            url_endpoint = "global-rank"
        elif sw_endpoint == "geography_distribution":
            url_category = "Geo"
            url_endpoint = "traffic-by-country"
        elif sw_endpoint == "unique_visitors":
            url_category = "unique-visitors"
            url_endpoint = "desktop_mau"
        else:
            I_validOpts = False
            url_endpoint = None
            print("Unknown endpoint for category " + sw_category)
            print("\t" + sw_endpoint)

    elif sw_category == "WebTrafficSources":
        I_validOpts = False
        print("Not prepared to accept category " + sw_category)

    elif sw_category == "DesktopOther":
        I_validOpts = False
        print("Not prepared to accept category " + sw_category)

    elif sw_category == "MobileApp_and_MobileWeb":
        I_validOpts = False
        print("Not prepared to accept category " + sw_category)

    else:
        # Not ready
        I_validOpts = False
        print("Unknown category " + sw_category)



    if not I_validOpts:
        print("Cannot submit URL request")
    else:
        reqURL =


    return



def main():
    # filename = "organizations_sample"
    filename = "organizations"

    print("Loading data: " + filename)
    print("\tDoes clean version exist?")
    print("\t\tAssuming no")
    # I_flag, filename = cleanData_CB(filename)
    I_flag = True
    filename = filename + "_cleaned"

    print("\tDone cleaning")
    print(I_flag)
    print(filename)


    print("Loading with pandas")
    dat_cb_org = pandas.read_csv(folderPath_data_cb + filename + ".csv")
    print("\tDone")

    # print(list(dat_cb_org))
    # print(dat_cb_org['employee_count'])

    # Get companies with employee count > 500
    thres_employee = 500
    print("Finding companies with more than " + str(thres_employee) + " employees")
    idxList_n, idxList = orgData_findBy_employees(dat_cb_org['employee_count'], thres_employee)

    if idxList_n == 0:
        print("\tNo companies found")
    else:
        print("\tFound " + str(idxList_n) + " companies")
    # print(idxList)

    print("Paring down list for testing purposes")
    idxList = idxList[0:10]

    # Remove companies with website information
    idxList2_n, idxList2 = orgData_findBy_homepage(dat_cb_org['homepage_url'][idxList])

    # Update idxList and idxList_n
    idxList = numpy.asarray(idxList)[idxList2].tolist()
    idxList_n = idxList_n

    # Clear
    idxList2 = None
    idxList2_n = None





    return

if __name__ == '__main__':
    main()


# # # Load organizations.csv data
# # dat_cb_org = pandas.read_csv(folderPath_data_cb + "organizations.csv")
# #   Results in error
# #       Need to clean the data
# #           Col 18 = "first_funding_on"
# #               Meant to be DATE data
# #               Some entries don't make sense
# #                   e.g. 0001-01-01 BC
# #               Some needs to be reformated
# #                   e.g. 0023-06-16 is probably 23/06/2016
# #           Col 19 = "last_funding_on"
# #               Meant to be DATE data
# #               Some entries don't make sense
# #                   e.g. 0001-01-01 BC
# #           Col 21 = employee_count
# #               Meant to be STRING data representing integer ranges
# #                       1-10
# #                       11-50
# #                       51-100
# #                       101-500
# #               Jan-10 is actually 1-10
# #               Nov-50 is actually 11-50
# #
# #
# # Scan through and clean data
# hFileRead = open(folderPath_data_cb + "organizations.csv", encoding="utf8")
# hFileWrite = open(folderPath_data_cb + "organizations_cleaned.csv", 'w')
#
# datRead = csv.reader(hFileRead,delimiter=',')
# hWriter = csv.writer(hFileWrite, delimiter=',', lineterminator='\n')
#
# idx = 0
# while True:
#     try:
#         row = next(datRead)
#
#         # Increment counter / row number
#         idx = idx + 1
#
#         # Clean line
#         if idx != 0:
#             iCol = 18
#             if len(row[iCol]) > 5 and row[iCol][4] == '-':
#                 if not row[iCol][-1].isdigit():
#                     row[iCol] = "invalid"
#                 else:
#                     # Get day and month
#                     d = row[iCol][2:4]
#                     m = row[iCol][5:7]
#
#                     # Year is trickier as we are given the last 2 digits
#                     #   Get current year with time.strftime("%Y")
#                     #           = '2017'
#                     #   Take last two digits, covert to integer, compare to what we have
#                     if int(row[iCol][8:10]) <= int(time.strftime("%Y")[2:4]):
#                         y = "20" + row[iCol][8:10]
#                     else:
#                         y = "19" + row[iCol][8:10]
#
#                     row[iCol] = d + "/" + m + "/" + y
#
#             iCol = 19
#             if len(row[iCol]) > 5 and row[iCol][4] == '-':
#                 if not row[iCol][-1].isdigit():
#                     row[iCol] = ""
#                 else:
#                     # Get day and month
#                     d = row[iCol][2:4]
#                     m = row[iCol][5:7]
#
#                     # Year is trickier as we are given the last 2 digits
#                     #   Get current year with time.strftime("%Y")
#                     #           = '2017'
#                     #   Take last two digits, covert to integer, compare to what we have
#                     if int(row[iCol][8:10]) <= int(time.strftime("%Y")[2:4]):
#                         y = "20" + row[iCol][8:10]
#                     else:
#                         y = "19" + row[iCol][8:10]
#
#                     row[iCol] = d + "/" + m + "/" + y
#
#             iCol = 21
#             if len(row[iCol]) > 2:
#                 if not row[iCol][0].isdigit() or not row[iCol][-1].isdigit():
#
#                     # Given either 'Nov-50' or '10-Jan'
#                     if row[iCol][0] == 'J' or row[iCol][-1] == 'n' or row[iCol][-1] == 'y':
#                         row[iCol] = "1-10"
#                     elif row[iCol][0] == 'N' or row[iCol][-1] == 'v' or row[iCol][-1] == 'r':
#                         row[iCol] = "11-50"
#
#         # Save
#         try:
#             hWriter.writerow(row)
#         except:
#             "Error writing at " + str(idx)
#
#
#     except csv.Error:
#         print("Error at " + str(idx))
#     except StopIteration:
#         break
#
#
# # for row in datRead:
# #     # Display row
# #     # print(row)
# #
# #     # Clean line
# #     if idx != 0:
# #         iCol = 18
# #         if len(row[iCol]) > 5 and row[iCol][4] == '-':
# #             if not row[iCol][-1].isdigit():
# #                 row[iCol] = "invalid"
# #             else:
# #                 # Get day and month
# #                 d = row[iCol][2:4]
# #                 m = row[iCol][5:7]
# #
# #                 # Year is trickier as we are given the last 2 digits
# #                 #   Get current year with time.strftime("%Y")
# #                 #           = '2017'
# #                 #   Take last two digits, covert to integer, compare to what we have
# #                 if int(row[iCol][8:10]) <= int(time.strftime("%Y")[2:4]):
# #                     y = "20" + row[iCol][8:10]
# #                 else:
# #                     y = "19" + row[iCol][8:10]
# #
# #                 row[iCol] = d + "/" + m + "/" + y
# #
# #         iCol = 19
# #         if len(row[iCol]) > 5 and row[iCol][4] == '-':
# #             if not row[iCol][-1].isdigit():
# #                 row[iCol] = ""
# #             else:
# #                 # Get day and month
# #                 d = row[iCol][2:4]
# #                 m = row[iCol][5:7]
# #
# #                 # Year is trickier as we are given the last 2 digits
# #                 #   Get current year with time.strftime("%Y")
# #                 #           = '2017'
# #                 #   Take last two digits, covert to integer, compare to what we have
# #                 if int(row[iCol][8:10]) <= int(time.strftime("%Y")[2:4]):
# #                     y = "20" + row[iCol][8:10]
# #                 else:
# #                     y = "19" + row[iCol][8:10]
# #
# #                 row[iCol] = d + "/" + m + "/" + y
# #
# #         iCol = 21
# #         if len(row[iCol]) > 2:
# #             if not row[iCol][0].isdigit() or not row[iCol][-1].isdigit():
# #
# #                 # Given either 'Nov-50' or '10-Jan'
# #                 if row[iCol][0] == 'J' or row[iCol][-1] == 'n' or row[iCol][-1] == 'y':
# #                     row[iCol] = "1-10"
# #                 elif row[iCol][0] == 'N' or row[iCol][-1] == 'v' or row[iCol][-1] == 'r':
# #                     row[iCol] = "11-50"
# #
# #     # Save
# #     hWriter.writerow(row)
# #
# #     # Increment counter / row number
# #     idx = idx+1
#
# hFileRead.close()
# hFileWrite.close()