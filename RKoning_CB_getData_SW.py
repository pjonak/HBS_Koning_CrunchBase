import sys
import os
import csv # read CrunchBase CSV files, for cleaning
import _io # file opening and writing
import pandas
import numpy
import json
import time
import random
import copy
import urllib # send URL request - API interaction


SW_gran = "weekly" # "daily", "weekly"
SW_date_start = "2015-03"
SW_date_end = "2017-02"

I_grid = False
I_print = False
I_log = True

dataset_I_sample_rand = False
dataset_nSample_rand = 10

dataset_I_sample = True
dataset_nSample = 1
# dataset_I_intel = True


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
    folderPath_response = "SW_responses/"
    folderPath_keys = "keys/"
else:
    folderPath_data = "dataset\\crunchbase_2017_02_06\\"
    folderPath_response = "SW_responses\\"
    folderPath_keys = "keys\\"

# Which file are we looking to load?
if I_grid:
    fileName = "organizations"
    fileName = fileName + "_trim.csv"

else:
    fileName = "organizations"
    fileName = fileName + "_load_v3_part2_v3__trim.csv"

def cleanWebsiteURL(website:str) -> str:
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

def sw_getKey() -> str:
    # Loads API key for SimilarWeb
    #   File must be named "sw.cnf"
    #   File must reside in the root of the project folder
    hFile = open(folderPath_root + folderPath_keys + "sw.cnf")
    hReader = csv.reader(hFile,delimiter=',')
    apiKey_sw = None
    for rowData in hReader:
        if rowData[0]=="api_key":
            apiKey_sw = rowData[1]
    hFile.close()
    return apiKey_sw

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

    # if dataset_I_sample and dataset_I_intel and dataset_nSample == 1:
    #     website = "intel.com"
    #     print("!!")
    #     print("using intel.com")
    #     print("!!")


    urlBase = "https://api.similarweb.com/"

    sw_category = "TotalTraffic"
    sw_endpoint = "visits"

    start_date = SW_date_start
    end_date = SW_date_end

    granularity = SW_gran

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
        if isinstance(company, float):
            # Empty
            company = website

        filenameResponse = "sw__" + company.replace(" ","_").replace(",","_").replace(".","_").replace("/","_").replace("\\","_")
        # filenameResponse = filenameResponse + urlEndpoint
        # filenameResponse = "sw__" + website + "__" + urlCategory[0:-1] + "__" + urlEndpoint
        # if start_date is not None:
        #     filenameResponse = filenameResponse + "__start_" + start_date
        # if end_date is not None:
        #     filenameResponse = filenameResponse + "__end=" + end_date
        # if granularity is not None:
        #     filenameResponse = filenameResponse + "__" + granularity

        # filenameResponse = filenameResponse + "." + urlFormat
        filenameResponse = filenameResponse + ".json"

        return urlRequest, filenameResponse

def main():

    if I_log:
        # Prepare log file
        #   Assume ending is ".py"
        logPath = folderPath_root + os.path.basename( os.path.realpath(__file__) )[0:-3] + ".log"
        hFileLog = open(logPath, 'w', encoding="ascii")
        hFileLog.write("Log file for:\n\t" + os.path.realpath(__file__) +"\n")

    # Verify file exists
    fullPath = folderPath_root + folderPath_data + fileName
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
            tempMsg = "Loading data: " + fileName
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        dat = pandas.read_csv(fullPath, sep=',', encoding='utf-8', index_col=0)
        if I_print or I_log:
            tempMsg = "\tDone"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")

        # Take a sample?
        if dataset_I_sample_rand:
            if I_print or I_log:
                tempMsg = "Taking a random sample of " + str(dataset_nSample_rand) + " companies" + \
                          "\n\tSampling check\n\t\tOriginal # rows = " + str(len(dat))
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            dat = dat.iloc[sorted(random.sample(range(0, len(dat)), dataset_nSample_rand)), :]
            if I_print or I_log:
                tempMsg = "\t\tSample # rows = " + str(len(dat))
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")

        # Make sure we're organized by date
        #   Sorting by date
        #       column = 'founded_on'
        dat = dat.ix[dat['founded_on'].sort_values(ascending=False).index]


        if not dataset_I_sample_rand and dataset_I_sample:
            if I_print or I_log:
                tempMsg = "Taking a sample of first " + str(dataset_nSample) + " companies" + \
                          "\n\tSampling check\n\t\tOriginal # rows = " + str(len(dat))
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")
            dat = dat.iloc[numpy.arange(dataset_nSample)]
            if I_print or I_log:
                tempMsg = "\t\tSample # rows = " + str(len(dat))
                if I_print:
                    print(tempMsg)
                if I_log:
                    hFileLog.write(tempMsg + "\n")


        # Add 3 columns
        #   sw_url - what is the request we will send?
        #   sw_filename - where will we store the response?
        #   sw_I_request - a flag to determine if we sent the request
        #   sw_I_response - a flag to determine if we received a response
        dat['sw_url'] = [None]*dat.shape[0]
        dat['sw_filename'] = [None]*dat.shape[0]
        dat['sw_I_request'] = [0] * dat.shape[0]
        dat['sw_I_response'] = [0] * dat.shape[0]

        # Ready to build up queries
        #   Get API key
        sw_apiKey = sw_getKey()
        #   Then store the query
        #   Finally add the API key before sending request
        pandas.options.mode.chained_assignment = None  # default='warn'
        if I_print or I_log:
            tempMsg = "Building API requests: SimilarWeb"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        for iRow in range(0, dat.shape[0]):
            urlReq, filenameResponse = CB_buildURL_SW( cleanWebsiteURL(dat['homepage_url'].iloc[iRow]),
                                                      dat['company_name'].iloc[iRow],
                                                      sw_apiKey)

            if urlReq is not None:
                dat['sw_url'].iloc[iRow] = urlReq
                dat['sw_filename'].iloc[iRow] = filenameResponse
        pandas.options.mode.chained_assignment = 'warn'

        # Check to see if we already have a response for any of these
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iRow in range(0, dat.shape[0]):
            if os.path.exists(folderPath_root + folderPath_response + dat['sw_filename'].iloc[iRow]):
                # File exists!
                dat['sw_I_response'].iloc[iRow] = 1
        pandas.options.mode.chained_assignment = 'warn'

        # Ready
        if I_print or I_log:
            tempMsg = "Sending SW requests:"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iRow in range(0, dat.shape[0]):
            if dat['sw_I_response'].iloc[iRow] == 1:
                if I_print or I_log:
                    tempMsg = "\tHave response for " + cleanWebsiteURL(dat['homepage_url'].iloc[iRow])
                    if I_print:
                        print(tempMsg)
                    if I_log:
                        hFileLog.write(tempMsg + "\n")
            elif dat['sw_I_response'].iloc[iRow] == 0:
                if I_print or I_log:
                    tempMsg = "\t" + dat['sw_url'].iloc[iRow]
                    if I_print:
                        print(tempMsg)
                    if I_log:
                        hFileLog.write(tempMsg + "\n")

                try:
                    dat['sw_I_request'].iloc[iRow] = 1
                    hJSON = json.loads(urllib.request.urlopen( dat['sw_url'].iloc[iRow] ).read().decode('utf-8'))
                    dat['sw_I_response'].iloc[iRow] = 1
                    I_json = True
                except Exception as e:
                    if I_print or I_log:
                        tempMsg = "\t\tError: " + str(e)
                        if I_print:
                            print(tempMsg)
                        if I_log:
                            hFileLog.write(tempMsg + "\n")
                    I_json = False

                # Save sW data to file
                #   Leave empty if no response received
                hFile = _io.open(folderPath_root + folderPath_response + dat['sw_filename'].iloc[iRow], "w")
                if I_json:
                    if I_print or I_log:
                        tempMsg = "\t\tSaving SW data to file"
                        if I_print:
                            print(tempMsg)
                        if I_log:
                            hFileLog.write(tempMsg + "\n")
                    json.dump(hJSON, hFile)
                hFile.close()

                # Add time delay
                time.sleep(0.5)

        if I_print or I_log:
            tempMsg = "\tEnd of requests"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        pandas.options.mode.chained_assignment = 'warn'

        if I_print or I_log:
            tempMsg = "Saving status matrix to file"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")
        dat.to_csv(folderPath_root + folderPath_response + "dat_" + time.strftime("%Y_%m_%d") + ".csv", sep=',', encoding='utf-8')
        if I_print or I_log:
            tempMsg = "\tDone"
            if I_print:
                print(tempMsg)
            if I_log:
                hFileLog.write(tempMsg + "\n")

    if I_print:
        print("End")
    if I_log:
        hFileLog.write("End")
        hFileLog.close()
    return

if I_grid:
    if __name__ == '__main__':
        main()
else:
    main()
    exit()