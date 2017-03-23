import os
import csv # read CrunchBase CSV files, for cleaning
import _io # file opening and writing
import pandas
import numpy
import json
import time
import random
import copy




dataset_I_sample = True
dataset_nSample = 20000

query_nSample = 5 # how many companies are in our query sample?
query_I_founded = True # require entries to have "founded_on" date
query_colName_founded = 'founded_on'
query_I_funding = True # require entries to have "first_funding_on" date
query_colName_funding = 'first_funding_on'
query_I_employee = True # require entries to have "employee_count"
query_colName_employee = 'employee_count'





# ------------------------------------------
# This assumes you've already run
#   RKoning_CB_LoadAndTrim_Orgs
#
#
#
# ------------------------------------------
# Variables used in above .py files
#
#
# Get current directory of where this code file is
# Get project root directory by going to the parent directory
folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

# Identify where data is held
folderPath_data = "dataset\\crunchbase_2017_02_06\\"

# Which file are we looking to load?
fileName = "organizations"

# What are the suffixes we will be using?
suffix_load = "load"
suffix_enum = "part2"

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
colList_enum = ['employee_count', 'founded_on']

#   Columns for exploratory analysis
colList_eda = ['employee_count', 'first_funding_on',
               'founded_on', 'facebook_url']





def CB_buildURL_SW(website: str, company: str, apiKey_sw: str) -> (str, str):
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

    granularity = "daily"

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
        filenameResponse = "sw__" + company.replace(" ", "_") + "__" + urlCategory[0:-1] + "__" + urlEndpoint
        if start_date is not None:
            filenameResponse = filenameResponse + "__start_" + start_date
        if end_date is not None:
            filenameResponse = filenameResponse + "__end=" + end_date
        if granularity is not None:
            filenameResponse = filenameResponse + "__" + granularity

        filenameResponse = filenameResponse + "." + urlFormat

        return urlRequest, filenameResponse



def CB_buildURL_BW(website: str, company: str, apiKey_bw: str) -> (str, str):
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


    filenameResponse = "bw__" + company.replace(" ", "_") + urlCategory.replace("/","_")
    if urlVert is not None:
        filenameResponse = filenameResponse + "__VERTICALS"
    if urlUpdate is not None:
        filenameResponse = filenameResponse + "__UPDATE"
    filenameResponse = filenameResponse + ".json"


    return urlRequest, filenameResponse



def main():
    # Verify file exists
    fullPath = folderPath_root + folderPath_data + fileName + "_" + suffix_load + "_" + suffix_enum + ".csv"
    if os.path.exists(fullPath):
        print("File found")
        I_flag = True
    else:
        print("File not found\n\tSearch path: " + fullPath)
        I_flag = False

    if I_flag:
        # Load data
        print("Loading data: " + fileName)
        dat = pandas.read_csv(fullPath, sep=',', encoding='utf-8', dtype=str)
        print("\tDone")

        # Take a sample?
        if dataset_I_sample:
            print("Taking a sample of " + str(dataset_nSample) + " companies")
            print("\tSampling check\n\t\tOriginal # rows = " + str(len(dat)))
            dat = dat.iloc[sorted(random.sample(range(0, len(dat)), dataset_nSample)), :]
            print("\t\tSample # rows = " + str(len(dat)))

        # Remove entries that do not have homepage specified
        print("Removing entries that do not have a homepage specified")
        len1 = len(dat['homepage_url'])
        dat = dat.iloc[numpy.arange(len(dat['homepage_url']))[numpy.array(dat['homepage_url'].notnull())]]
        print("\tDone\n\t\tRemoved " + str(len1 - len(dat['homepage_url'])) + " entries")



        # Ready to get our sample of companies to query
        datQ = copy.deepcopy(dat)
        if query_I_founded:
            colName = query_colName_founded
            datQ = datQ.iloc[numpy.arange(len(datQ[colName]))[numpy.array(datQ[colName].notnull())]]

        if query_I_funding:
            colName = query_colName_funding
            datQ = datQ.iloc[numpy.arange(len(datQ[colName]))[numpy.array(datQ[colName].notnull())]]

        if query_I_employee:
            colName = query_colName_employee
            datQ = datQ.iloc[numpy.arange(len(datQ[colName]))[numpy.array(datQ[colName].notnull())]]

        print("Taking a sample of " + str(query_nSample) + " companies")
        datS = copy.deepcopy(datQ)
        print("\tSampling check\n\t\tOriginal # rows = " + str(len(datS)))
        datS = datS.iloc[sorted(random.sample(range(0, len(datS)), query_nSample)), :]
        print("\t\tSample # rows = " + str(len(datS)))



        # Ready to build up queries
        #   Do not include the API key in the query, but instead put a placeholder
        tempStr_API = "API_Key_Placeholder"
        #   Then store the query
        #   Finally add the API key before sending request

        # Prepare new columns
        datS['sw_url'] = [None]*len(datS['homepage_url'])
        datS['sw_filename'] = [None]*len(datS['homepage_url'])
        # Ready
        print("Building API requests: SimilarWeb - daily visits")
        for iRow in range(0,len(datS['homepage_url'])):
            urlReq, filenameResponse = CB_buildURL_SW(datS['homepage_url'].iloc[iRow],
                                                      datS['company_name'].iloc[iRow],
                                                      tempStr_API)

            if urlReq is not None:
                datS['sw_url'].iloc[iRow] = urlReq
                datS['sw_filename'].iloc[iRow] = filenameResponse

        # Prepare new columns
        datS['bw_url'] = [None] * len(datS['homepage_url'])
        datS['bw_filename'] = [None] * len(datS['homepage_url'])
        # Ready
        print("Building API requests: BuiltWith")
        for iRow in range(0, len(datS['homepage_url'])):
            urlReq, filenameResponse = CB_buildURL_BW(datS['homepage_url'].iloc[iRow],
                                                      datS['company_name'].iloc[iRow],
                                                      tempStr_API)

            if urlReq is not None:
                datS['bw_url'].iloc[iRow] = urlReq
                datS['bw_filename'].iloc[iRow] = filenameResponse




        print(datS)




    return
if __name__ == '__main__':
    main()