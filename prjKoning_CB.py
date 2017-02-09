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
import pandas # R-like read of CrunchBase CSV files
import csv # read CrunchBase CSV files, for cleaning
import time
import _io # file opening and writing
import numpy # more intuitive indexing operations
import json # receive and store JSON data
import urllib # send URL request - API interaction

# Prepare constants
#   Project Storage
folderPath_root_cb = "C:\\Users\\pjonak\\Documents\\Projects\\" + \
                     "Koning\\Crunchbase_SimilarWeb_BuiltWith\\"
folderPath_data_cb = folderPath_root_cb + "dataset\\crunchbase_2017_02_06\\"
folderPath_swResponse_cb = folderPath_root_cb + "SW_responses\\"
folderPath_bwResponse_cb = folderPath_root_cb + "BW_responses\\"



def cleanData_CB(filename: str) -> (bool, str):
    # Generic function to receive CrunchBase CSV files for cleaning
    #   Identify which CSV file was given, then go to specific cleaning operations
    if len(filename)>=3 and filename[0:3]=='org':
        # Organizations.csv or Organizations_Descriptions.csv
        #   Assume only Organizations.csv for now
        return cleanData_CB__org(filename)
    else:
        return False, " "

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
    # Generic function to open CrunchBase CSV files along with new CSV files to receive cleaned data
    return open(folderPath_data_cb + filename + ".csv", encoding="utf8"), \
           open(folderPath_data_cb + filename + "_cleaned.csv", 'w', encoding="utf8"), \
           filename + "_cleaned"

def cleanData_CB__closeFiles(hFileRead: _io.TextIOWrapper, hFileWrite: _io.TextIOWrapper):
    # Generic function to close files associated with CrunchBase cleaning routine
    hFileRead.close()
    hFileWrite.close()
    return


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
        return "invalid"



def orgData_findBy_employees(datColumn: pandas.core.frame.DataFrame, thres: int, aboveThres: bool) -> (int, list):
    # Specific to CrunchBase data - ORGANIZATIONS.CSV
    #   Get list of companies which have employee range ABOVE the threshold
    #       List is returned as indices
    idxList = [None]
    iIdx = 0
    iRow = 0
    if aboveThres:
        for rowData in datColumn:
            if len(rowData) > 1 and rowData[0].isdigit():
                if int( rowData.split('-',1)[0] ) > thres:
                    if iIdx == 0:
                        idxList[iIdx]=iRow
                    else:
                        idxList=idxList+[iRow]
                    iIdx=iIdx+1
            iRow=iRow+1
    else:
        for rowData in datColumn:
            if len(rowData) > 1 and rowData[0].isdigit():
                if int( rowData.split('-',1)[0] ) < thres:
                    if iIdx == 0:
                        idxList[iIdx]=iRow
                    else:
                        idxList=idxList+[iRow]
                    iIdx=iIdx+1
            iRow=iRow+1
    return iIdx, idxList



def orgData_findBy_homepage(datColumn: pandas.core.frame.DataFrame) -> (int, list):
    # Specific to CrunchBase data - ORGANIZATIONS.CSV
    #   Get list of companies which have an entry in the homepage_url column
    #       List is returned as indices
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
    # Loads API key for SimilarWeb
    #   File must be named "sw.cnf"
    #   File must reside in the root of the project folder
    hFile = open(folderPath_root_cb + "sw.cnf")
    hReader = csv.reader(hFile,delimiter=',')
    apiKey_sw = None
    for rowData in hReader:
        if rowData[0]=="api_key":
            apiKey_sw = rowData[1]
    hFile.close()
    return apiKey_sw



def sw_cleanWebsiteURL(website:str) -> str:
    # SimilarWeb does not accept company URLs with "/" nor "www."

    # Clean up website URL
    #   Remove "/"
    website = website.lower()
    if len(website) > 5 and website[0:4] == "http":
        website = website.split("/")
        if len(website[1]) == 0:
            website = website[2]
        else:
            website = website[1]
    else:
        website = website.split("/")
        for w in website:
            if len(w) != 0:
                website = w
                break
    #   Remove "www."
    if len(website) > 4 and website[0:4] == "www.":
        website = website[4::]
    return website



def sw_buildURL(website: str, apiKey_sw: str) -> (str, str):
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

    urlBase = "https://api.similarweb.com/"

    sw_category = "TotalTraffic"
    sw_endpoint = "visits"

    start_date = "2015-06"
    end_date = "2015-09"

    granularity = "monthly"

    main_domain_only = False

    urlFormat = "json"


    # Check
    I_validOpts = True
    if sw_category == "TotalTraffic":
        version_and_type = "v1/website/"
        urlCategory = "total-traffic-and-engagement/"
        if sw_endpoint == "visits":
            urlEndpoint = "visits"
        elif sw_endpoint == "pages_per_visit":
            urlEndpoint = "pages-per-visit"
        elif sw_endpoint == "average_visit_duration":
            urlEndpoint = "average-visit-duration"
        elif sw_endpoint == "bounce_rate":
            urlEndpoint = "bounce-rate"
        elif sw_endpoint == "visits_split":
            urlEndpoint = "visits-split"
        else:
            I_validOpts = False
            urlEndpoint = None
            print("Unknown endpoint for category " + sw_category)
            print("\t" + sw_endpoint)

    elif sw_category == "DesktopTraffic":
        version_and_type = "v1/website/"
        urlCategory = "traffic-and-engagement/"
        if sw_endpoint == "visits":
            urlEndpoint = "visits"
        elif sw_endpoint == "pages_per_visit":
            urlEndpoint = "pages-per-visit"
        elif sw_endpoint == "average_visit_duration":
            urlEndpoint = "average-visit-duration"
        elif sw_endpoint == "bounce_rate":
            urlEndpoint = "bounce-rate"
        elif sw_endpoint == "global_rank":
            urlCategory = "global-rank"
            urlEndpoint = "global-rank"
        elif sw_endpoint == "geography_distribution":
            urlCategory = "Geo"
            urlEndpoint = "traffic-by-country"
        elif sw_endpoint == "unique_visitors":
            urlCategory = "unique-visitors"
            urlEndpoint = "desktop_mau"
        else:
            I_validOpts = False
            urlEndpoint = None
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
        print("Cannot build URL request")
        return None, None
    else:
        urlRequest = urlBase + version_and_type + website + "/" + urlCategory + urlEndpoint + "?api_key=" + apiKey_sw

        if start_date is not None:
            urlRequest = urlRequest + "&start_date=" + start_date
        if end_date is not None:
            urlRequest = urlRequest + "&end_date=" + end_date
        if main_domain_only is not None:
            urlRequest = urlRequest + "&main_domain_only=" + str(main_domain_only).lower()
        if granularity is not None:
            urlRequest = urlRequest + "&granularity=" + granularity

        urlRequest = urlRequest + "&format=" + urlFormat


        # Create filename for storing SimilarWeb response
        filenameResponse = "sw__" + website + "__" + urlCategory[0:-1] + "__" + urlEndpoint
        if start_date is not None:
            filenameResponse = filenameResponse + "__start_" + start_date
        if end_date is not None:
            filenameResponse = filenameResponse + "__end=" + end_date
        if granularity is not None:
            filenameResponse = filenameResponse + "__" + granularity

        filenameResponse = filenameResponse + "." + urlFormat


        return urlRequest, filenameResponse



def bw_getKey() -> str:
    # Loads API key for SimilarWeb
    #   File must be named "sw.cnf"
    #   File must reside in the root of the project folder
    hFile = open(folderPath_root_cb + "bw.cnf")
    hReader = csv.reader(hFile,delimiter=',')
    apiKey_bw = None
    for rowData in hReader:
        if rowData[0]=="api_key":
            apiKey_bw = rowData[1]
    hFile.close()
    return apiKey_bw



def bw_buildURL(website: str, apiKey_bw: str) -> (str, str):
    # The URL is composed of the following parts:
    #   base / domain = api.builtwith.com/
    #

    urlBase = "https://api.builtwith.com/"

    # Technology Categories index = True
    # All else = False
    bw_category = False

    # All potential vertical values?
    bw_verticals = False

    # Last database update dates?
    bw_update = False

    if bw_category:
        urlCategory = "categoriesV4"
    else:
        urlCategory = "v11/api"

    if bw_verticals:
        urlVert = "VERTICALS=1"
    else:
        urlVert = None

    if bw_update:
        urlUpdate = "UPDATE=1"
    else:
        urlUpdate = None


    urlRequest = urlBase + urlCategory + ".json" + "?KEY=" + apiKey_bw + "&LOOKUP=" + website

    if urlVert is not None:
        urlRequest = urlRequest + "&" + urlVert
    if urlUpdate is not None:
        urlRequest = urlRequest + "&" + urlUpdate


    filenameResponse = "bw__" + website + urlCategory.replace("/","_")
    if urlVert is not None:
        filenameResponse = filenameResponse + "__VERTICALS"
    if urlUpdate is not None:
        filenameResponse = filenameResponse + "__UPDATE"
    filenameResponse = filenameResponse + ".json"


    return urlRequest, filenameResponse



def main():
    # filename = "organizations_sample"
    filename = "organizations"

    print("Loading data: " + filename)
    print("\tDoes clean version exist?")
    print("\t\tAssuming no")
    I_flag, filename = cleanData_CB(filename)
    # I_flag = True
    # filename = filename + "_cleaned"
    print("\tDone cleaning")

    print("Loading with pandas")
    dat_cb_org = pandas.read_csv(folderPath_data_cb + filename + ".csv")
    print("\tDone")

    # Get companies with employee count > 500
    thres_employee = 500
    print("Finding companies with more than " + str(thres_employee) + " employees")
    idxList_n, idxList = orgData_findBy_employees(dat_cb_org['employee_count'], thres_employee, True)

    if idxList_n == 0:
        print("\tNo companies found")
    else:
        print("\tFound " + str(idxList_n) + " companies")
    # print(idxList)

    print("Paring down list for testing purposes")
    idxList = idxList[0:10]
    idxList_n = len(idxList)

    # Remove companies without website information
    idxList2_n, idxList2 = orgData_findBy_homepage(dat_cb_org['homepage_url'][idxList])

    # Update idxList and idxList_n
    idxList = numpy.asarray(idxList)[idxList2].tolist()
    idxList_n = idxList2_n
    #   Clear idxList2 and idxList2_n
    idxList2 = None
    idxList2_n = None

    # Request SimilarWeb and BuiltWith data
    print("Requesting SimilarWeb and BuiltWith data...")
    #   Get SimilarWeb API key
    sw_apiKey = sw_getKey()
    #   Get BuiltWith API key
    bw_apiKey = bw_getKey()

    print("Paring down list AGAIN for testing purposes")
    idxList = idxList[1:2]
    idxList_n = len(idxList)

    for idx in idxList:
        print("Company: " + dat_cb_org['company_name'][idx])

        #   Get company homepage URL
        #       SW doesn't allow for
        #           "http:"
        #           "www."
        #           "[website]/subdomain"
        #       assume same for BW
        website = sw_cleanWebsiteURL( dat_cb_org['homepage_url'][idx] )

        #   Build SW request URL for current company
        #       and filename where we will store the response
        urlReq, filenameResponse = sw_buildURL(website, sw_apiKey)

        if urlReq is None:
            print("Unable to build SW request for: " + website)
        else:
            print("SW request for: " + website)
            try:
                hJSON = json.loads( urllib.request.urlopen( urlReq ).read().decode('utf-8') )
                I_json = True
            except:
                print("\tUnable to request data on:\n\t" + urlReq + "\n\tPlease check website URL")
                I_json = False

            #   Save SW data to file
            hFile = _io.open(folderPath_swResponse_cb + filenameResponse, "w")
            if I_json:
                print("\tSaving SW data to file")
                json.dump(hJSON,hFile)
                # print(hJSON)

            hFile.close()

        # # Load past results
        # filename = "sw__intel.com__total-traffic-and-engagement__visits__start_2015-06__end=2015-09__monthly.json"
        # with open(folderPath_swResponse_cb + filename) as json_data:
        #     dat = json.load(json_data)
        #     print(dat)
        #
        # website = dat['meta']['request']['domain']
        # print(website)


        #   Build BW request URL for current company
        urlReq, filenameResponse = bw_buildURL(website, bw_apiKey)

        if urlReq is None:
            print("Unable to build BW request for: " + website)
        else:
            print("BW request for: " + website)
            try:
                hJSON = json.loads( urllib.request.urlopen( urlReq ).read().decode('utf-8') )
                I_json = True
            except:
                print("\tUnable to request data on:\n\t" + urlReq + "\n\tPlease check website URL")
                I_json = False

            #   Save BW data to file
            hFile = _io.open(folderPath_bwResponse_cb + filenameResponse, "w")
            if I_json:
                print("\tSaving BW data to file")
                json.dump(hJSON,hFile)
                # print(hJSON)

            hFile.close()

    return

if __name__ == '__main__':
    main()