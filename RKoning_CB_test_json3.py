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


    if I_BW:

        # Initialize tables
        tableLookup_init = pandas.DataFrame({
            'id_lookup': numpy.empty(1, dtype=int),
            'domain_lookup': numpy.empty(1, dtype=str),
            'name_lookup': numpy.empty(1, dtype=str),
            'vertical': numpy.empty(1, dtype=str),
            'ARank': numpy.empty(1, dtype=str),
            'QRank': numpy.empty(1, dtype=str),
            'IndexFirst': numpy.empty(1, dtype=int),
            'IndexLast': numpy.empty(1, dtype=int)
        })
        tableLookup = copy.deepcopy(tableLookup_init)
        I_any_lookup = False

        tableTech_init = pandas.DataFrame({
            'id_tech': numpy.empty(1, dtype=int),
            'name_tech': numpy.empty(1, dtype=str),
            'id_techTag': numpy.empty(1, dtype=int),
            'id_techCat': numpy.empty(1, dtype=int)
        })
        tableTech = copy.deepcopy(tableTech_init)
        I_any_tech = False

        tableTechTag_init = pandas.DataFrame({
            'id_techTag': numpy.empty(1, dtype=int),
            'name_techTag': numpy.empty(1, dtype=str)
        })
        tableTechTag = copy.deepcopy(tableTechTag_init)
        I_any_techTag = False

        tableTechCat_init = pandas.DataFrame({
            'id_techCat': numpy.empty(1, dtype=int),
            'name_techCat': numpy.empty(1, dtype=str)
        })
        tableTechCat = copy.deepcopy(tableTechCat_init)
        I_any_techCat = False



        # Fill the Lookup table
        #   How many Lookups do we have?
        nLookup = len(list(dat['Results']))

        pandas.options.mode.chained_assignment = None  # default='warn'
        I_add = False
        for iLU in range(0, nLookup):
            # Get domain
            domain = dat['Results'][iLU]['Lookup'].lower()

            if not I_any_lookup:
                I_add = True
            # else:
                # Search

            if I_add:
                if not I_any_lookup:
                    I_any_lookup = True
                    tableLookup["id_lookup"].iloc[0] = 1
                    tableLookup["domain_lookup"].iloc[0] = domain
                    # tableLookup["IndexFirst"].iloc[0] = int(dat['Results'][iLU]['FirstIndexed'] )/1000
                else:
                    tableLookup_init["id_lookup"].iloc[0] = 1+max(tableLookup["id_lookup"])
                    tableLookup_init["domain_lookup"].iloc[0] = domain
                    # tableLookup_init["IndexFirst"].iloc[0] = int(dat['Results'][iLU]['FirstIndexed']) / 1000

                    tableLookup = tableLookup.append(tableLookup_init)

                # Reset I_add
                I_add = False
        pandas.options.mode.chained_assignment = 'warn'



        # File the Tech, Tag, and Cat tables
        pandas.options.mode.chained_assignment = None  # default='warn'
        I_add = False
        for iLU in range(0, nLookup):
            for iPaths in range(0, len(list(dat['Results'][iLU]['Result']['Paths'])) ):
                for iTech in range(0, len(list(dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'])) ):
                    I_add = False

                    # Get tech, tag and category
                    techName = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Name'].lower()
                    techTag = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag'].lower()

                    techCatList = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Categories']

                    # Initialize IDs for tag and category
                    id_tag = None
                    if isinstance(techCatList, list):
                        idList_cat = [None]*len(techCatList)
                    else:
                        idList_cat = [None]

                    # Check to see if we have the Tag
                    if not I_any_techTag:
                        I_add = True
                    else:
                        # Search for match
                        if not any(tableTechTag["name_techTag"] == techTag):
                            I_add = True
                        else:
                            id_tag = tableTechTag["id_techTag"].iloc[
                                numpy.where(tableTechTag["name_techTag"] == techTag)[0]]

                    if I_add:
                        if not I_any_techTag:
                            I_any_techTag = True
                            id_tag = 1
                            tableTechTag["id_techTag"].iloc[0] = id_tag
                            tableTechTag["name_techTag"].iloc[0] = techTag
                        else:
                            id_tag = 1 + max(tableTechTag["id_techTag"])
                            tableTechTag_init["id_techTag"].iloc[0] = id_tag
                            tableTechTag_init["name_techTag"].iloc[0] = techTag

                            tableTech = tableTech.append(tableTechTag_init)
                        # Reset I_add
                        I_add = False

                    # Check to see if we have the Category
                    if isinstance(techCatList, list):
                        for iCat in range(0, len(techCatList)):

                    else:
                        if not I_any_techCat:
                            I_add = True
                        else:
                            # Search for match
                            if not any(tableTechCat["name_techCat"] == techCatList):
                                I_add = True
                            else:
                                idList_cat[0] = tableTechCat["id_techCat"].iloc[
                                    numpy.where(tableTechCat["name_techCat"] == techCatList)[0]]

                        if I_add:
                            if not I_any_techCat:
                                I_any_techCat = True
                                idList_cat[0] = 1
                                tableTechTag["id_techTag"].iloc[0] = idList_cat[0]
                                tableTechTag["name_techTag"].iloc[0] = techCatList
                            else:
                                idList_cat[0] = 1 + max(tableTechTag["id_techTag"])
                                tableTechTag_init["id_techTag"].iloc[0] = idList_cat[0]
                                tableTechTag_init["name_techTag"].iloc[0] = techCatList

                                tableTechCat = tableTech.append(tableTechTag_init)
                            # Reset I_add
                            I_add = False


                    if not I_any_techCat:
                        I_add = True
                    else:
                        if isinstance(techCatList, list):
                            for iCat in range(0,len(techCatList)):

                        else:
                            # Search for match
                            if not any(tableTechCat["name_techCat"] == techCatList):
                                I_add = True
                            else:
                                id_tag = tableTechCat["id_techCat"].iloc[
                                    numpy.where(tableTechCat["name_techCat"] == techCatList)[0]]






                    if not I_any_tech:
                        I_add = True
                    else:
                        # Search for match
                        if not any(tableTech["name_tech"]==techName):
                            I_add = True

                    if I_add:
                        # Reset I_add
                        I_add = False

                        # Do we need to update tableTechTag or tableTechCat?
                        id_tag = None
                        id_cat = None

                        techTag = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag'].lower()

                        if not I_any_techTag:
                            I_add = True
                        else:
                            # Search for match
                            if not any(tableTechTag["name_techTag"] == techTag):
                                I_add = True

                        if I_add:
                            if not I_any_techTag:
                                I_any_techTag = True
                                tableTechTag["id_techTag"].iloc[0] = 1
                                tableTechTag["name_techTag"].iloc[0] = techTag
                            else:
                                tableTechTag_init["id_techTag"].iloc[0] = 1 + max(tableTechTag["id_techTag"])
                                tableTechTag_init["name_techTag"].iloc[0] = techTag

                                tableTech = tableTech.append(tableTechTag_init)



                        # Done updating tableTechTag and/or tableTechCat
                        # Add tech to tableTech
                        if not I_any_tech:
                            I_any_tech = True
                            tableTech["id_tech"].iloc[0] = 1
                            tableTech["name_tech"].iloc[0] = techName
                        else:
                            tableTech_init["id_tech"].iloc[0] = 1 + max(tableTech["id_tech"])
                            tableTech_init["name_tech"].iloc[0] = techName

                            tableTech = tableTech.append(tableTech_init)




        pandas.options.mode.chained_assignment = 'warn'

        print(tableLookup)
        print(tableTech)

        tableTech.to_csv(folderPath_root + "intel_Tech.csv", sep=',', encoding='utf-8')


    #     pandas.options.mode.chained_assignment = None  # default='warn'
    #     nTech_prev = 0
    #     for iLU in range(0, nLookup):
    #         for iPaths in range(0, nPaths_perLookup[iLU]):
    #
    #             nTech_iPaths = len(list(dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies']))
    #             for iTech in range(0,nTech_iPaths):
    #                 datTech['tech_name'].iloc[nTech_prev + iTech] = copy.deepcopy(
    #                     dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Name'] )
    #                 datTech['tech_tag'].iloc[nTech_prev + iTech] = copy.deepcopy(
    #                     dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag'] )
    #                 datTech['tech_categories'].iloc[nTech_prev + iTech] = copy.deepcopy(
    #                     dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Categories'] )
    #                 datTech['tech_detected_first'].iloc[nTech_prev + iTech] = copy.deepcopy(
    #                     dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['FirstDetected'])
    #                 datTech['tech_detected_last'].iloc[nTech_prev + iTech] = copy.deepcopy(
    #                     dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['LastDetected'])
    #
    #                 datTech['paths_id'].iloc[nTech_prev + iTech] = int( datPaths['paths_id'].iloc[iPaths] )
    #
    #             nTech_prev += nTech_iPaths
    #     pandas.options.mode.chained_assignment = 'warn'
