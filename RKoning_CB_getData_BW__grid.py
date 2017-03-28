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

dataset_I_sample_rand = False
dataset_nSample_rand = 1

dataset_I_sample = True
dataset_nSample = 100




# Get current directory of where this code file is
# Get project root directory by going to the parent directory
folderPath_root = "/export/home/dor/pjonak/Projects/RKoning_CB/"
# folderPath_root = os.path.dirname( os.path.realpath(__file__) )
# folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

# Identify where data is held
folderPath_data = "dataset/"
folderPath_response = "BW_responses/"
folderPath_keys = "keys/"

# Which file are we looking to load?
fileName = "organizations"
fileName = fileName + "_trim.csv"

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

def bw_getKey() -> str:
    # Loads API key for SimilarWeb
    #   File must be named "sw.cnf"
    #   File must reside in the root of the project folder
    hFile = open(folderPath_root + folderPath_keys + "bw.cnf")
    hReader = csv.reader(hFile,delimiter=',')
    apiKey_bw = None
    for rowData in hReader:
        if rowData[0]=="api_key":
            apiKey_bw = rowData[1]
    hFile.close()
    return apiKey_bw

def CB_buildURL_BW(website: str, company: str, apiKey_bw: str) -> (str, str):
    # The URL is composed of the following parts:
    #   base / domain = api.builtwith.com/
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

    if isinstance(company, float):
        # Empty
        company = website

    # filenameResponse = "bw__" + company.replace(" ", "_") + urlCategory.replace("/","_")
    filenameResponse = "bw__" + company.replace(" ", "_").replace(",", "_").replace(".", "_").replace("/","_").replace("\\","_")
    if urlVert is not None:
        filenameResponse = filenameResponse + "__VERTICALS"
    if urlUpdate is not None:
        filenameResponse = filenameResponse + "__UPDATE"
    filenameResponse = filenameResponse + ".json"

    return urlRequest, filenameResponse

def main():
    # Verify file exists
    fullPath = folderPath_root + folderPath_data + fileName
    if os.path.exists(fullPath):
        print("File found")
        I_flag = True
    else:
        print("File not found\n\tSearch path: " + fullPath)
        I_flag = False

    if I_flag:
        # Load data
        print("Loading data: " + fileName)
        dat = pandas.read_csv(fullPath, sep=',', encoding='utf-8', index_col=0)
        print("\tDone")

        # Take a sample?
        if dataset_I_sample_rand:
            print("Taking a random sample of " + str(dataset_nSample_rand) + " companies")
            print("\tSampling check\n\t\tOriginal # rows = " + str(len(dat)))
            dat = dat.iloc[sorted(random.sample(range(0, len(dat)), dataset_nSample_rand)), :]
            print("\t\tSample # rows = " + str(len(dat)))

        # Make sure we're organized by date
        #   Sorting by date
        #       column = 'founded_on'
        dat = dat.ix[dat['founded_on'].sort_values(ascending=False).index]


        if not dataset_I_sample_rand and dataset_I_sample:
            print("Taking a sample of first " + str(dataset_nSample) + " companies")
            print("\tSampling check\n\t\tOriginal # rows = " + str(len(dat)))
            dat = dat.iloc[numpy.arange(dataset_nSample)]
            print("\t\tSample # rows = " + str(len(dat)))


        # Add 3 columns
        #   bw_url - what is the request we will send?
        #   bw_filename - where will we store the response?
        #   bw_I_request - a flag to determine if we sent the request
        #   bw_I_response - a flag to determine if we received a response
        dat['bw_url'] = [None]*dat.shape[0]
        dat['bw_filename'] = [None]*dat.shape[0]
        dat['bw_I_request'] = [0] * dat.shape[0]
        dat['bw_I_response'] = [0] * dat.shape[0]

        # Ready to build up queries
        #   Get API key
        bw_apiKey = bw_getKey()
        #   Then store the query
        #   Finally add the API key before sending request
        pandas.options.mode.chained_assignment = None  # default='warn'
        print("Building API requests: BuiltWith")
        for iRow in range(0, dat.shape[0]):
            urlReq, filenameResponse = CB_buildURL_BW( cleanWebsiteURL(dat['homepage_url'].iloc[iRow]),
                                                      dat['company_name'].iloc[iRow],
                                                      bw_apiKey)

            if urlReq is not None:
                dat['bw_url'].iloc[iRow] = urlReq
                dat['bw_filename'].iloc[iRow] = filenameResponse
        pandas.options.mode.chained_assignment = 'warn'

        # Check to see if we already have a response for any of these
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iRow in range(0, dat.shape[0]):
            if os.path.exists(folderPath_root + folderPath_response + dat['bw_filename'].iloc[iRow]):
                # File exists!
                # dat['bw_I_request'].iloc[iRow] = 1
                dat['bw_I_response'].iloc[iRow] = 1
        pandas.options.mode.chained_assignment = 'warn'

        # Ready
        print("Sending BW requests:")
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iRow in range(0, dat.shape[0]):
            if dat['bw_I_response'].iloc[iRow] == 0:
                print("\t" + dat['bw_url'].iloc[iRow])
                try:
                    dat['bw_I_request'].iloc[iRow] = 1
                    hJSON = json.loads(urllib.request.urlopen( dat['bw_url'].iloc[iRow] ).read().decode('utf-8'))
                    dat['bw_I_response'].iloc[iRow] = 1
                    I_json = True
                except:
                    print("\t\tError")
                    I_json = False
        
                # Save BW data to file
                #   Leave empty if no response received
                hFile = _io.open(folderPath_root + folderPath_response + dat['bw_filename'].iloc[iRow], "w")
                if I_json:
                    print("\t\tSaving BW data to file")
                    json.dump(hJSON, hFile)
                hFile.close()
        
                # Add time delay
                time.sleep(0.5)
        
        print("\tEnd of requests")
        pandas.options.mode.chained_assignment = 'warn'

        print("Saving status matrix to file")
        dat.to_csv(folderPath_root + folderPath_response + "dat_" + time.strftime("%Y_%m_%d") + ".csv", sep=',', encoding='utf-8')
        print("Done")

    return

main()
exit()
