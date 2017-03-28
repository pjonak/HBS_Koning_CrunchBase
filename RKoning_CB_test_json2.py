import json
import _io
import os
import pandas
import numpy
import copy
import random


folderPath_root = os.path.dirname( os.path.realpath(__file__) )
folderPath_root = folderPath_root[::-1].split("\\",1)[1][::-1] + "\\"

folderPath_SW = "SW_responses\\"
fileName_SW = "sw__intel.com__total-traffic-and-engagement__visits__start_2015-06__end=2015-09__monthly"

folderPath_BW = "BW_responses\\"
fileName_BW = "bw__intel.com.json"

I_BW = True



# Load file
if I_BW:
    hFile = _io.open(folderPath_root + folderPath_BW + fileName_BW)
else:
    hFile = _io.open(folderPath_root + folderPath_BW + fileName_SW)
dat = json.loads(hFile.read())
hFile.close()



# Validate that the response has data
if I_BW:
    I_error = not not dat['Errors']
else:
    I_error = True



if I_error:
    print("Error detected, unable to proceed")
else:

    homepage_url = [None]
    last_update = [None]

    if I_BW:
        # How many Lookups do we have?
        nLookup = len(list(dat['Results']))
        # How many Paths per Lookup?
        nPaths_perLookup = [None]*nLookup
        # How many Technologies entries are there in each lookup?
        nTech_perLookup = [None]*nLookup
        for iLU in range(0, nLookup):
            # How many Paths in this lookup?
            nPaths_perLookup[iLU] = len(list(dat['Results'][iLU]['Result']['Paths']))
            # How many Technologies entries in each Paths?
            nTech = [None]*nPaths_perLookup[iLU]
            for iPaths in range(0, nPaths_perLookup[iLU]):
                nTech[iPaths] = len(list(dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies']))
            # Sum to get total number of Technologies entries for this Lookup
            nTech_perLookup[iLU] = sum(nTech)
        # Total number of Technologies entries would be sum of nTech_perLookup
        nTech_total = sum(nTech_perLookup)
        nPaths_total = sum(nPaths_perLookup)
        # Grab data
        #   Initialize
        #       Table 1: Lookup, FirstIndexed, LastIndexed
        datLookup = pandas.DataFrame( {
            'LU_id': numpy.arange(1,nLookup+1, dtype=int),
            'LU_website': numpy.empty((nLookup), dtype=str),
            'LU_indexed_first': numpy.empty((nLookup), dtype=str),
            'LU_indexed_last': numpy.empty((nLookup), dtype=str)
            } )

        pandas.options.mode.chained_assignment = None  # default='warn'
        for iLU in range(0,nLookup):
            datLookup['LU_website'].iloc[iLU] = copy.deepcopy(
                dat['Results'][iLU]['Lookup'] )
            datLookup['LU_indexed_first'].iloc[iLU] = copy.deepcopy(
                dat['Results'][iLU]['FirstIndexed'] )
            datLookup['LU_indexed_last'].iloc[iLU] = copy.deepcopy(
                dat['Results'][iLU]['LastIndexed'] )
        pandas.options.mode.chained_assignment = 'warn'

        #       Table 2: Domain, FirstIndexed, LastIndexed
        datPaths = pandas.DataFrame({
            'paths_id': numpy.arange(1, nPaths_total + 1, dtype=int),
            'paths_domain': numpy.empty((nPaths_total), dtype=str),
            'paths_indexed_first': numpy.empty((nPaths_total), dtype=str),
            'paths_indexed_last': numpy.empty((nPaths_total), dtype=str),
            'LU_id': numpy.empty((nPaths_total), dtype=int)
        })

        pandas.options.mode.chained_assignment = None  # default='warn'
        nPaths_prev = 0
        for iLU in range(0, nLookup):
            for iPaths in range(0,nPaths_perLookup[iLU]):
                datPaths['paths_domain'].iloc[nPaths_prev+iPaths] = copy.deepcopy(
                    dat['Results'][iLU]['Result']['Paths'][iPaths]['Domain'] )
                datPaths['paths_indexed_first'].iloc[nPaths_prev + iPaths] = copy.deepcopy(
                    dat['Results'][iLU]['Result']['Paths'][iPaths]['FirstIndexed'] )
                datPaths['paths_indexed_last'].iloc[nPaths_prev + iPaths] = copy.deepcopy(
                    dat['Results'][iLU]['Result']['Paths'][iPaths]['LastIndexed'] )

                datPaths['LU_id'].iloc[nPaths_prev + iPaths] = int( datLookup['LU_id'].iloc[iLU] )

            nPaths_prev += nPaths_perLookup[iLU]
        pandas.options.mode.chained_assignment = 'warn'

        #       Table 3: Technologies, FirstDetected, LastDetected
        datTech = pandas.DataFrame({
            'tech_id': numpy.arange(1, nTech_total + 1, dtype=int),
            'tech_name': numpy.empty((nTech_total), dtype=str),
            'tech_tag': numpy.empty((nTech_total), dtype=str),
            'tech_categories': numpy.empty((nTech_total), dtype=str),
            'tech_detected_first': numpy.empty((nTech_total), dtype=str),
            'tech_detected_last': numpy.empty((nTech_total), dtype=str),
            'paths_id': numpy.empty((nTech_total), dtype=int)
        })

        pandas.options.mode.chained_assignment = None  # default='warn'
        nTech_prev = 0
        for iLU in range(0, nLookup):
            for iPaths in range(0, nPaths_perLookup[iLU]):

                nTech_iPaths = len(list(dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies']))
                for iTech in range(0,nTech_iPaths):
                    datTech['tech_name'].iloc[nTech_prev + iTech] = copy.deepcopy(
                        dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Name'] )
                    datTech['tech_tag'].iloc[nTech_prev + iTech] = copy.deepcopy(
                        dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag'] )
                    datTech['tech_categories'].iloc[nTech_prev + iTech] = copy.deepcopy(
                        dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Categories'] )
                    datTech['tech_detected_first'].iloc[nTech_prev + iTech] = copy.deepcopy(
                        dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['FirstDetected'])
                    datTech['tech_detected_last'].iloc[nTech_prev + iTech] = copy.deepcopy(
                        dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['LastDetected'])

                    datTech['paths_id'].iloc[nTech_prev + iTech] = int( datPaths['paths_id'].iloc[iPaths] )

                nTech_prev += nTech_iPaths
        pandas.options.mode.chained_assignment = 'warn'



        # print(datLookup)
        # print(datPaths.iloc[0:5])
        # print(datTech.iloc[0:5])
        # print(datTech.iloc[-5:-1])


        # Join together
        #   datTech is the base
        datJoin = copy.deepcopy(datTech)
        datJoin = datJoin.iloc[sorted(random.sample(range(0, len(datJoin)), 10)), :]
        print(datJoin['paths_id'])


        datJoin['paths_indexed_first'] = [0]*datJoin.shape[0]
        pandas.options.mode.chained_assignment = None  # default='warn'
        for i in range(0,datJoin.shape[0]):
            datJoin['paths_indexed_first'].iloc[i] = copy.deepcopy(
                datPaths['paths_indexed_first'].iloc[ numpy.where( datPaths['paths_id'] == datJoin['paths_id'].iloc[i] ) ].iloc[0]
            )
        pandas.options.mode.chained_assignment = 'warn'


        print( datJoin['paths_indexed_first'] )

