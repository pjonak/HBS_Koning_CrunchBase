import json
import _io
import os
import pandas
import numpy
import copy
import random
import datetime


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
        pandas.options.mode.chained_assignment = None  # default='warn'
        # Initialize tables
        tableLookup_init = pandas.DataFrame({
            'id_lookup': numpy.empty(1, dtype=int),
            'domain_lookup': numpy.empty(1, dtype=str),
            'name_lookup': numpy.empty(1, dtype=str),
            'vertical': numpy.empty(1, dtype=str),
            'ARank': numpy.empty(1, dtype=str),
            'QRank': numpy.empty(1, dtype=str),
            'IndexFirst': numpy.empty(1, dtype=int),
            'IndexLast': numpy.empty(1, dtype=int),
            'IndexFirst_date': numpy.empty(1, dtype=str),
            'IndexLast_date': numpy.empty(1, dtype=str)
        })
        tableLookup_init['IndexFirst_date'] = pandas.to_datetime(tableLookup_init['IndexFirst_date'])
        tableLookup_init['IndexLast_date'] = pandas.to_datetime(tableLookup_init['IndexLast_date'])
        tableLookup = copy.deepcopy(tableLookup_init)
        I_any_lookup = False

        tableTech_init = pandas.DataFrame({
            'id_tech': numpy.empty(1, dtype=int),
            'name_tech': numpy.empty(1, dtype=str),
            'id_techTag': numpy.empty(1, dtype=int),
            'id_techCat': numpy.empty(1, dtype=int),
            'DetectedFirst': numpy.empty(1, dtype=int),
            'DetectedLast': numpy.empty(1, dtype=int),
            'DetectedFirst_date': numpy.empty(1, dtype=str),
            'DetectedLast_date': numpy.empty(1, dtype=str)
        })
        tableTech_init['DetectedFirst_date'] = pandas.to_datetime(tableTech_init['DetectedFirst_date'])
        tableTech_init['DetectedLast_date'] = pandas.to_datetime(tableTech_init['DetectedLast_date'])
        tableTech = copy.deepcopy(tableTech_init)
        tableTech['id_tech'].iloc[0] = 0
        tableTech['name_tech'].iloc[0] = "none"
        tableTech['id_techTag'].iloc[0] = 0
        tableTech['id_techCat'].iloc[0] = 0
        tableTech['DetectedFirst'].iloc[0] = None # was seeing -1 otherwise
        tableTech['DetectedLast'].iloc[0] = None
        I_any_tech = True

        tableTechTag_init = pandas.DataFrame({
            'id_techTag': numpy.empty(1, dtype=int),
            'name_techTag': numpy.empty(1, dtype=str)
        })
        tableTechTag = copy.deepcopy(tableTechTag_init)
        tableTechTag['id_techTag'].iloc[0] = 0
        tableTechTag['name_techTag'].iloc[0] = "none"
        I_any_techTag = True

        tableTechCat_init = pandas.DataFrame({
            'id_techCat': numpy.empty(1, dtype=int),
            'name_techCat': numpy.empty(1, dtype=str)
        })
        tableTechCat = copy.deepcopy(tableTechCat_init)
        tableTechCat['id_techCat'].iloc[0] = 0
        tableTechCat['name_techCat'].iloc[0] = "none"
        I_any_techCat = True

        pandas.options.mode.chained_assignment = 'warn'


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
                    tableLookup["name_lookup"].iloc[0] = dat['Results'][iLU]['Meta']['CompanyName']
                    tableLookup["vertical"].iloc[0] = dat['Results'][iLU]['Meta']['Vertical']
                    tableLookup["ARank"].iloc[0] = dat['Results'][iLU]['Meta']['ARank']
                    tableLookup["QRank"].iloc[0] = dat['Results'][iLU]['Meta']['QRank']
                    tableLookup["IndexFirst"].iloc[0] = int(dat['Results'][iLU]['FirstIndexed'] )/1000
                    tableLookup["IndexLast"].iloc[0] = int(dat['Results'][iLU]['LastIndexed']) / 1000
                    tableLookup["IndexFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                        tableLookup["IndexFirst"].iloc[0] ).date()
                    tableLookup["IndexLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                        tableLookup["IndexLast"].iloc[0] ).date()
                else:
                    tableLookup_init["id_lookup"].iloc[0] = 1+max(tableLookup["id_lookup"])
                    tableLookup_init["domain_lookup"].iloc[0] = domain
                    tableLookup_init["name_lookup"].iloc[0] = dat['Results'][iLU]['Meta']['CompanyName']
                    tableLookup_init["vertical"].iloc[0] = dat['Results'][iLU]['Meta']['Vertical']
                    tableLookup_init["ARank"].iloc[0] = dat['Results'][iLU]['Meta']['ARank']
                    tableLookup_init["QRank"].iloc[0] = dat['Results'][iLU]['Meta']['QRank']
                    tableLookup_init["IndexFirst"].iloc[0] = int(dat['Results'][iLU]['FirstIndexed']) / 1000
                    tableLookup_init["IndexLast"].iloc[0] = int(dat['Results'][iLU]['LastIndexed']) / 1000
                    tableLookup_init["IndexFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                        tableLookup_init["IndexFirst"].iloc[0] ).date()
                    tableLookup_init["IndexLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                        tableLookup_init["IndexLast"].iloc[0] ).date()

                    tableLookup = tableLookup.append(tableLookup_init)

                # Reset I_add
                I_add = False
        pandas.options.mode.chained_assignment = 'warn'



        # File the Tech, Tag, and Cat tables
        pandas.options.mode.chained_assignment = None  # default='warn'
        for iLU in range(0, nLookup):
            for iPaths in range(0, len(list(dat['Results'][iLU]['Result']['Paths'])) ):
                for iTech in range(0, len(list(dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'])) ):
                    I_addName = False
                    I_addTag = False
                    # I_addCat = False

                    # Get tech, tag and category
                    #   Clean up
                    techName = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Name'].encode(
                        'utf8').decode('ascii', 'ignore').replace(" ","_").lower()
                    detectedFirst = int( dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                             'FirstDetected'] )/1000
                    detectedLast = int( dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                            'LastDetected'] )/1000

                    techTag = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag']
                    if techTag is None:
                        techTag = "none"
                    else:
                        techTag = techTag.encode('utf8').decode('ascii', 'ignore').replace(" ","_").lower()

                    techCatList = dat['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Categories']
                    if techCatList is None:
                        techCatList = "None"
                    if isinstance(techCatList, list):
                        for iCat in range(0, len(techCatList)):
                            techCatList[iCat] = techCatList[iCat].encode('utf8').decode('ascii', 'ignore').replace(" ","_").lower()
                    else:
                        techCatList = [techCatList.encode('utf8').decode('ascii', 'ignore').replace(" ","_").lower()]


                    # Want to convert name, tag and cat to IDs
                    #   Initialize IDs for tag and category
                    id_tech = None
                    id_tag = None
                    idList_cat = [None]*len(techCatList)


                    # Check to see if we have the Tag
                    if not I_any_techTag:
                        I_addTag = True
                    else:
                        # Search for match
                        if not any(tableTechTag["name_techTag"] == techTag):
                            I_addTag = True
                        else:
                            id_tag = int( tableTechTag["id_techTag"].iloc[
                                numpy.where(tableTechTag["name_techTag"] == techTag)[0]] )


                    if I_addTag:
                        if not I_any_techTag:
                            I_any_techTag = True
                            id_tag = 1
                            tableTechTag["id_techTag"].iloc[0] = id_tag
                            tableTechTag["name_techTag"].iloc[0] = techTag
                        else:
                            id_tag = 1 + max(tableTechTag["id_techTag"])
                            tableTechTag_init["id_techTag"].iloc[0] = id_tag
                            tableTechTag_init["name_techTag"].iloc[0] = techTag

                            tableTechTag = tableTechTag.append(tableTechTag_init)


                    # Check to see if we have the Category
                    for iCat in range(0, len(techCatList)):
                        I_addCat = False

                        if not I_any_techCat:
                            I_addCat = True
                        else:
                            # Search for match
                            I_match = tableTechCat["name_techCat"] == techCatList[iCat]
                            if not any(I_match):
                                I_addCat = True
                            else:
                                idList_cat[iCat] = int( tableTechCat["id_techCat"].iloc[numpy.where(I_match)[0]] )

                        if I_addCat:
                            if not I_any_techCat:
                                I_any_techCat = True
                                idList_cat[iCat] = 1
                                tableTechCat["id_techCat"].iloc[0] = idList_cat[iCat]
                                tableTechCat["name_techCat"].iloc[0] = techCatList[iCat]
                            else:
                                idList_cat[iCat] = 1 + max(tableTechCat["id_techCat"])
                                tableTechCat_init["id_techCat"].iloc[0] = idList_cat[iCat]
                                tableTechCat_init["name_techCat"].iloc[0] = techCatList[iCat]

                                tableTechCat = tableTechCat.append(tableTechCat_init)

                    # Check to see if we have the Tech name
                    if not I_any_tech:
                        I_addName = True
                    else:
                        # Search for match
                        I_match = tableTech["name_tech"]==techName
                        if not any(I_match):
                            I_addName = True
                        else:
                            # Only take the first one as multiple categories = multiple rows of same tech name
                            idxListTech = numpy.where(I_match)[0]
                            idxTech = idxListTech[0]
                            id_tech = tableTech["id_tech"].iloc[idxTech]

                    if I_addName:
                        # Add tech to tableTech
                        if not I_any_tech:
                            I_any_tech = True
                            id_tech = 1
                            tableTech["id_tech"].iloc[0] = id_tech
                            tableTech["name_tech"].iloc[0] = techName
                            tableTech["id_techTag"].iloc[0] = id_tag
                            tableTech["id_techCat"].iloc[0] = idList_cat[0]
                            tableTech["DetectedFirst"].iloc[0] = detectedFirst
                            tableTech["DetectedLast"].iloc[0] = detectedLast
                            tableTech["DetectedFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                detectedFirst).date()
                            tableTech["DetectedLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                detectedLast).date()

                            if len(idList_cat) > 1:
                                for iCat in range(1,len(idList_cat)):
                                    tableTech_init["id_tech"].iloc[0] = id_tech
                                    tableTech_init["name_tech"].iloc[0] = techName
                                    tableTech_init["id_techTag"].iloc[0] = id_tag
                                    tableTech_init["id_techCat"].iloc[0] = idList_cat[iCat]
                                    tableTech_init["DetectedFirst"].iloc[0] = detectedFirst
                                    tableTech_init["DetectedLast"].iloc[0] = detectedLast
                                    tableTech_init["DetectedFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                        detectedFirst ).date()
                                    tableTech_init["DetectedLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                        detectedLast ).date()

                                    tableTech = tableTech.append(tableTech_init)
                        else:
                            id_tech = 1 + max(tableTech["id_tech"])
                            for iCat in range(0, len(idList_cat)):
                                tableTech_init["id_tech"].iloc[0] = id_tech
                                tableTech_init["name_tech"].iloc[0] = techName
                                tableTech_init["id_techTag"].iloc[0] = id_tag
                                tableTech_init["id_techCat"].iloc[0] = idList_cat[iCat]
                                tableTech_init["DetectedFirst"].iloc[0] = detectedFirst
                                tableTech_init["DetectedLast"].iloc[0] = detectedLast
                                tableTech_init["DetectedFirst_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                    detectedFirst).date()
                                tableTech_init["DetectedLast_date"].iloc[0] = datetime.datetime.fromtimestamp(
                                    detectedLast).date()

                                tableTech = tableTech.append(tableTech_init)

                    elif id_tech > 0:
                        if detectedFirst < tableTech["DetectedFirst"].iloc[idxTech]:
                            for idx in idxListTech:
                                tableTech["DetectedFirst"].iloc[idx] = detectedFirst
                                tableTech["DetectedFirst_date"].iloc[idx] = datetime.datetime.fromtimestamp(
                                    detectedFirst).date()
                        if detectedLast > tableTech["DetectedLast"].iloc[idxTech]:
                            for idx in idxListTech:
                                tableTech["DetectedLast"].iloc[idx] = detectedLast
                                tableTech["DetectedLast_date"].iloc[idx] = datetime.datetime.fromtimestamp(
                                    detectedLast).date()



                    if not I_addName and (I_addTag or I_addCat):
                        print("Already had name but new Tag or Cat!")
                        print(techName)



        pandas.options.mode.chained_assignment = 'warn'

        print(tableLookup)
        # print(tableTech)
        # print(tableTechTag)
        # print(tableTechCat)

        tableLookup.to_csv(folderPath_root + "intel_Lookup.csv", sep=',', encoding='utf-8')
        tableTech.to_csv(folderPath_root + "intel_Tech.csv", sep=',', encoding='utf-8')
        tableTechTag.to_csv(folderPath_root + "intel_TechTag.csv", sep=',', encoding='utf-8')
        tableTechCat.to_csv(folderPath_root + "intel_TechCat.csv", sep=',', encoding='utf-8')