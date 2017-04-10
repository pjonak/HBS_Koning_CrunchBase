import sys
import os
import pandas
import MySQLdb
from MySQLdb.connections import Connection as mysqlConnection
import math
import numpy
import json



folderPath_root = os.path.abspath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) , os.pardir) )
folderPath_CB = os.path.join(folderPath_root,"dataset","crunchbase_2017_02_06")
folderPath_BW = os.path.join(folderPath_root,"BW_responses")
folderPath_SW = os.path.join(folderPath_root,"SW_responses")
folderPath_tables = os.path.join(folderPath_root,"tables")
folderPath_cfg = os.path.join(folderPath_root,"keys")

cfg_db_filepath = "dbConfig.txt"



I_grid = False


# Which file are we looking to load?
if I_grid:
    fileName_CB = "organizations"
    fileName_CB = fileName_CB + "_trim.csv"

else:
    fileName_CB = "organizations"
    fileName_CB = "organizations_sample"
    fileName_CB = fileName_CB + "_intel.csv"



colTypeList = '{"founded_on": "date", "first_funding_on": "date", ' + \
              '"employee_count": "range"}'



def colName_2_colIdx__pType(colList: list, matchJSON: dict) -> list:
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

def Orgs_cleanup_byType(datRow: pandas.core.frame.DataFrame, datHeader: list,
                      colType_json: dict, colIdxList: list) -> pandas.core.frame.DataFrame:

    for idx in colIdxList:
        if datRow.iloc[idx] is not None and (
                    (isinstance(datRow.iloc[idx],str) and len(datRow.iloc[idx])>0) or
                    (isinstance(datRow.iloc[idx],float) and not numpy.isnan(datRow.iloc[idx]) ) ):
            colType = colType_json[datHeader[idx]]
            if colType == "date":
                datRow.iloc[idx] = Orgs_cleanup_byType_date(datRow.iloc[idx])
            elif colType == "range":
                datRow.iloc[idx] = Orgs_cleanup_byType_range(datRow.iloc[idx])
    return datRow

def Orgs_cleanup_byType_date(dat: str) -> str:
    # The format we want is YYYY-MM-DD
    #   Sometimes we get
    #       YYYY-MM-DD
    #       00DD-MM-YY
    I_valid = False

    if dat.find(":") != -1:
        dat = dat[0:len(dat)-dat[::-1].find(" ")-1]

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
                # Assume MM / DD / YYYY
                try:
                    temp = dat.split('/', 2)
                    dat = str(int(temp[2])) + "-" + str(int(temp[1])) + "-" + str(int(temp[0]))
                    I_valid = True
                except:
                    print("error converting to date (assume MM/DD/YYYY): " + dat)

            elif dat[4] == "/" and dat[0] != 0:
                # Assume YYYY / MM / DD
                try:
                    temp = dat.split('/', 2)
                    dat = str(int(temp[0])) + "-" + str(int(temp[1])) + "-" + str(int(temp[2]))
                    I_valid = True
                except:
                    print("error converting to date (assume YYYY/MM/DD): " + dat)

        elif dat.find("-") != -1:
            # Either
            #   00??-??-??
            #       or
            #   YYYY-MM-DD
            if dat[0] != "0":
                # Assume YYYY - MM - DD
                try:
                    temp = dat.split('-', 2)
                    dat = str(int(temp[0])) + "-" + str(int(temp[1])) + "-" + str(int(temp[2]))
                    I_valid = True
                except:
                    print("error converting to date: " + dat)

    if I_valid:
        return dat
    else:
        return ""

def Orgs_cleanup_byType_range(dat: str) -> str:
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



def dispSQL(cmdStr: str) -> str:
    # Every X characters, insert "\n\t\t" to make the code more readable
    #   \n causes a new line
    #   \t causes one indent
    #       Take X characters, add a new line and 2 indents, then take another X characters, etc
    # Characters per line?
    perLine = 100
    # Total number of characters?
    lenStr = len(cmdStr)

    if lenStr <= perLine:
        # No need to add a new line
        dispStr = "\t\t" + cmdStr
    else:
        # Need to add at least 1 line
        #   How many lines do we need in total?
        nLine = math.ceil(lenStr/perLine)

        # Format very first line
        dispStr = "\t\t" + cmdStr[0:perLine]
        # Move on to next lines
        if nLine == 2:
            # Add only 1 new line
            dispStr = dispStr + "\n\t\t" + cmdStr[perLine:lenStr]
        else:
            # Have more than 1 new line
            for iLine in range(1,nLine-1):
                dispStr = dispStr + "\n\t\t" + cmdStr[perLine*iLine:perLine*(iLine+1)]
            dispStr = dispStr + "\n\t\t" + cmdStr[perLine*(nLine-1):lenStr]
            print(cmdStr[perLine*nLine:lenStr])
    return dispStr

def buildSQL_existDB(dbName: str) -> str:
    cmdStr = "SELECT schema_name FROM information_schema.schemata" + \
             " WHERE schema_name='" + dbName + "';"
    return cmdStr

def buildSQL_existTable(dbName: str, tableName: str) -> str:
    cmdStr = "SELECT table_name FROM information_schema.tables WHERE table_schema='" + \
             dbName + "' AND table_name='" + tableName + "';"
    return cmdStr

def buildSQL_createTable(
        dbName: str, tableName: str, tableInfo: pandas.core.frame.DataFrame) -> str:
    # Get column headers
    colList = tableInfo.index.values

    # Initialize SQL command
    cmdStr = "CREATE TABLE " + dbName + "." + \
             tableName + " ("

    # Add columns
    for colName in colList:
        cmdStr += colName + " " + tableInfo['colType'].ix[colName] + ","

    if tableName == "Organizations":
        cmdStr += "PRIMARY KEY (uuid),"
    elif tableName == "Visit":
        cmdStr += "PRIMARY KEY (id_lookup,visit_granularity,visit_date),"

    cmdStr = cmdStr[0:-1] + ") ENGINE=MyISAM;"
    return cmdStr


def buildSQL_matchEntry(dbName: str, tableName: str, datMatch: pandas.core.frame.DataFrame) -> str:
    if isinstance(datMatch.columns.values[0],str) and not isinstance(datMatch.index.values[0],str):
        I_col = True
    else:
        I_col = False
    # Get column headers
    if I_col:
        colList = datMatch.columns.values
    else:
        colList = datMatch.index.values
    # Build initial SQL command
    cmdStr = "SELECT " + colList[0] + " FROM " + dbName + "." + tableName + " WHERE "
    # Add WHERE conditions
    for colName in colList:
        if I_col:
            if isinstance(datMatch[colName].iloc[0], str):
                # Note: dates may be read as strings
                cmdStr += colName + "='" + datMatch[colName].iloc[0] + "' AND "
            else:
                cmdStr += colName + "=" + str(datMatch[colName].iloc[0]) + " AND "
        else:
            if isinstance(datMatch.ix[colName], str):
                # Note: dates may be read as strings
                cmdStr += colName + "='" + datMatch.ix[colName] + "' AND "
            else:
                cmdStr += colName + "=" + str(datMatch.ix[colName]) + " AND "
    cmdStr = cmdStr[0:-5] + " LIMIT 1;"
    return cmdStr

def buildSQL_getEntry(dbName: str, tableName: str, datMatch: pandas.core.frame.DataFrame) -> str:
    if isinstance(datMatch.columns.values[0],str) and not isinstance(datMatch.index.values[0],str):
        I_col = True
    else:
        I_col = False
    # Get column headers
    if I_col:
        colList = datMatch.columns.values
    else:
        colList = datMatch.index.values
    # Build initial SQL command
    cmdStr = "SELECT * FROM " + dbName + "." + tableName + " WHERE "
    # Add WHERE conditions
    for colName in colList:
        if I_col:
            if isinstance(datMatch[colName].iloc[0], str):
                # Note: dates may be read as strings
                cmdStr += colName + "='" + datMatch[colName].iloc[0] + "' AND "
            else:
                cmdStr += colName + "=" + str(datMatch[colName].iloc[0]) + " AND "
        else:
            if isinstance(datMatch.ix[colName], str):
                # Note: dates may be read as strings
                cmdStr += colName + "='" + datMatch.ix[colName] + "' AND "
            else:
                cmdStr += colName + "=" + str(datMatch.ix[colName]) + " AND "
    cmdStr = cmdStr[0:-5] + ";"
    return cmdStr

def buildSQL_insertEntry(dbName: str, tableName: str, dat: pandas.core.frame.DataFrame) -> str:
    # Format is
    #       INSERT INTO [table] ([col1] , [col2] , ... ) VALUES ([val1] , [val2] , ... )

    # Get column headers
    colList = dat.index.values
    # Build initial SQL command
    cmdStr = "INSERT INTO " + dbName + "." + tableName + " ("
    # Add columns
    for colName in colList:
        cmdStr += colName + ","
    # Add values
    cmdStr = cmdStr[0:-1] + ") VALUES ("
    for colName in colList:
        if isinstance(dat.ix[colName], str):
            # Note: dates may be read as strings
            cmdStr += "'" + dat.ix[colName] + "',"
        else:
            cmdStr += str(dat.ix[colName]) + ","
    cmdStr = cmdStr[0:-1] + ");"

    return cmdStr




def validateDB_CB_Orgs(
        hConn: MySQLdb.connections.Connection,dbName: str, tableName: str,
        folderPath: str, fileName: str) -> (bool, str):
    fullPath = os.path.join(folderPath,fileName)
    fullPath_dbSetup = fullPath[0:-4] + "__dbSetup.txt"
    if not os.path.exists(fullPath):
        I_flag = True
        errMsg = "Unable to locate CB Organizations data at " + fullPath
    elif not os.path.exists(fullPath_dbSetup):
        I_flag = True
        errMsg = "Unable to locate DB setup file for CB Organizations: " + fullPath_dbSetup
    else:
        I_flag = False
        errMsg = ""

        # Load DB setup file
        info_table = pandas.read_csv(fullPath_dbSetup)
        #   Extract columns of interest
        colList_db = info_table.index.values

        # We want to associate a unique id_lookup with each CB Organizations entry
        #   Verify that database exists
        #       If it doesn't, create
        #           id_lookup starts at 10,001
        #       If it does exist,
        #           get max id_lookup
        #           go through each entry in CB_Orgs file and check if already exists in database
        #               if it doesn't, add plus update max id_lookup
        cmdStr = buildSQL_existTable(dbName, tableName)
        resSQL = pandas.read_sql(cmdStr, hConn)
        if resSQL.empty:
            # Need to create table
            I_createTable = True
            # Build SQL command
            cmdStr = buildSQL_createTable(dbName, tableName, info_table)
            # Get results (cannot use pandas_read_sql here, results in error)
            hCursor = hConn.cursor()
            hCursor.execute(cmdStr)
            resSQL = hCursor.fetchall()
            if len(resSQL) != 0:
                I_flag = True
                errMsg = "Error creating CB Organizations table"
            else:
                id_lookup_max = 10001
        else:
            I_createTable = False
            # Get max id_lookup
            #   Query
            cmdStr = "SELECT MAX(id_lookup) FROM " + dbName + "." + tableName + ";"
            resSQL = pandas.read_sql(cmdStr, hConn)

            if resSQL.empty:
                I_flag = True
                errMsg = "Error querying CB Organizations table with: " + cmdStr
            else:
                id_lookup_max = resSQL['MAX(id_lookup)'].iloc[0]
                if id_lookup_max is None:
                    id_lookup_max = 10001

        if not I_flag:
            # Load CB Orgs data
            datOrg = pandas.read_csv(fullPath, sep=',', encoding='utf-8', index_col=0)
            # Ensure each entry has an id_lookup
            #   If not, add id_lookup and then re-save the file
            colName = "id_lookup"
            if not colName in list(datOrg):
                datOrg[colName] = numpy.arange(id_lookup_max, id_lookup_max+datOrg.shape[0])
                I_saveOrg = True
            else:
                I_haveID = numpy.asarray( datOrg[colName] > 0 )
                if not all(I_haveID):
                    idxList = numpy.where(numpy.invert(I_haveID))[0]

                    pandas.options.mode.chained_assignment = None  # default='warn'
                    datOrg[colName].iloc[idxList] = numpy.arange(id_lookup_max+1, id_lookup_max+1+len(idxList))
                    pandas.options.mode.chained_assignment = 'warn'

                    I_saveOrg = True
                else:
                    I_saveOrg = False


            # Clean up
            #   Dates
            #   employee_count
            datHeader = list(datOrg)
            colList_clean = json.loads(colTypeList)
            colIdxList_clean = colName_2_colIdx__pType(datHeader, colList_clean)

            pandas.options.mode.chained_assignment = None  # default='warn'
            for iRow in range(0, datOrg.shape[0]):
                datOrg.iloc[iRow] = Orgs_cleanup_byType(datOrg.iloc[iRow], datHeader, colList_clean, colIdxList_clean)
            pandas.options.mode.chained_assignment = 'warn'

            if I_saveOrg:
                datOrg.to_csv(fullPath, sep=',', encoding='utf-8')

            # Update database according to CB Orgs file
            #   First find out which entries are to be added and which are to be updated
            if I_createTable:
                I_addList = numpy.asarray([True] * datOrg.shape[0])
                I_updateList = numpy.asarray([False] * datOrg.shape[0])

            else:
                I_addList = numpy.asarray([False] * datOrg.shape[0])
                I_updateList = numpy.asarray([False] * datOrg.shape[0])

                for iOrg in range(0, datOrg.shape[0]):
                    dat_match = pandas.DataFrame({
                        'uuid': numpy.asarray([datOrg['uuid'].iloc[iOrg]], dtype=str),
                    })

                    cmdStr = buildSQL_getEntry(dbName, tableName, dat_match)
                    resSQL = pandas.read_sql(cmdStr, hConn)
                    if resSQL.empty:
                        # Entry doesn't exist
                        I_addList[iOrg] = True

                    # ELSE
                        # Entry does exist, validate


            if any(I_addList):
                for i in range(0, datOrg.shape[0]):
                    if I_addList[i]:
                        # Add entry to database
                        cmdStr = buildSQL_insertEntry(dbName, tableName, datOrg[colList_db].iloc[i])
                        hCursor = hConn.cursor()

                        try:
                            hCursor.execute(cmdStr)
                            resSQL = hCursor.fetchall()
                        except Exception as e:
                            print("Error adding " + datOrg['domain'].iloc[i] + "\n" + str(e))

            # if I_update_any:



    return I_flag, errMsg

def CB_Orgs_responseFiles(
        hConn: MySQLdb.connections.Connection,dbName: str, tableName: str,
        fp_BW: str, fp_SW: str) -> (bool, str):
    I_flag = False
    errMsg = ""

    

    return I_flag, errMsg


def SW_Visit(
        hConn: MySQLdb.connections.Connection, dbName: str, tableName: list,
        folderPath: str) -> (bool, str):
    I_flag = False
    errMsg = ""

    I_table = [False] * len(tableName)

    iTable = 0

    if tableName[iTable] == "Visit":
        colList_match = ['id_lookup', 'visit_granularity', 'visit_date']

    fn_info = os.path.join(folderPath_tables, "table_" + tableName[iTable] + "__dbSetup.txt")
    fn_dat = os.path.join(folderPath_tables, "table_" + tableName[iTable] + ".csv")

    # Validate we have our table within the database
    #   Create otherwise
    cmdStr = buildSQL_existTable(dbName, tableName[iTable])
    resSQL = pandas.read_sql(cmdStr, hConn)
    if resSQL.empty:
        # Need to create table
        #   Confirm we have table settings
        if os.path.exists(fn_info):
            # Get table info
            info_table = pandas.read_csv(fn_info)
            # Build SQL command
            cmdStr = buildSQL_createTable(dbName, tableName[iTable], info_table)
            # Get results (cannot use pandas_read_sql here, results in error)
            hCursor = hConn.cursor()
            hCursor.execute(cmdStr)
            resSQL = hCursor.fetchall()
            if len(resSQL) == 0:
                I_table[iTable] = True
            else:
                I_flag = True
                errMsg = "Error creating table " + tableName[iTable]
    else:
        I_table[iTable] = True

    if I_table[iTable]:
        # Insert data
        if not os.path.exists(fn_dat):
            errMsg = "No data found for " + tableName[iTable] + "\n" + fn_dat
        else:
            # Load data
            dat = pandas.read_csv(fn_dat,
                                  sep=',', index_col=0, header=0, encoding='utf-8')

            I_addList = numpy.asarray([False] * dat.shape[0])

            for id in numpy.unique(dat['id_lookup']):
                # Check to see if this ID is already present in the database
                #   format of dat_match
                #           [col1]      [col2]
                #   idx     [val1.1]    [val2.1]
                dat_match = pandas.DataFrame({'id_lookup': numpy.asarray([id], dtype=int)})

                cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], dat_match)
                resSQL = pandas.read_sql(cmdStr, hConn)
                if resSQL.empty:
                    # id_lookup not present, add all
                    I_addList[numpy.where(dat['id_lookup'] == id)[0]] = True
                else:
                    # id_look is present, check to see if granularity is there
                    for gran in numpy.unique(dat['visit_granularity'].iloc[
                                                 numpy.where(dat['id_lookup'] == id)[0]]):
                        dat_match = pandas.DataFrame({
                            'id_lookup': numpy.asarray([id], dtype=int),
                            'visit_granularity': numpy.asarray([gran], dtype=str)
                        })
                        cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], dat_match)
                        resSQL = pandas.read_sql(cmdStr, hConn)

                        idxList = numpy.where(
                            dat['visit_granularity'].iloc[
                                numpy.where(dat['id_lookup'] == id)[0]] == gran )[0]

                        if resSQL.empty:
                            # No entry for this id_lookup and granularity
                            I_addList[idxList] = True
                        else:
                            # Have entry for this id_lookup and granularity
                            #   Search by date
                            for idx in idxList:
                                dat_match = pandas.DataFrame({
                                    'id_lookup': numpy.asarray([id], dtype=int),
                                    'visit_granularity': numpy.asarray([gran], dtype=str),
                                    'visit_date': numpy.asarray([ dat['visit_date'].iloc[idx] ], dtype=str)
                                })
                                cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], dat_match)
                                resSQL = pandas.read_sql(cmdStr, hConn)

                                if resSQL.empty:
                                    # No entry for this id_lookup and granularity and date
                                    I_addList[idx] = True

            if any(I_addList):
                for i in range(0, len(I_addList)):
                    if I_addList[i]:
                        # Insert row

                        cmdStr = buildSQL_insertEntry(dbName, tableName[iTable], dat.iloc[i])
                        hCursor = hConn.cursor()
                        try:
                            hCursor.execute(cmdStr)
                            resSQL = hCursor.fetchall()
                        except Exception as e:
                            print("Error adding " + dat['id_lookup'].iloc[i] + " at " +
                                  dat['visit_date'].iloc[i] + "\n" + str(e))


    return I_flag, errMsg


def main():

    # Connect to database
    fullPath = os.path.join(folderPath_cfg, cfg_db_filepath)
    if not os.path.exists(fullPath):
        cfg_db = None
        errMsg = "Unable to locate database connection settings at " + fullPath
    else:
        cfg_db = pandas.read_csv(fullPath)

    # with SSHTunnelForwarder(
    #         (cfg_grid["value"]["host"], int(cfg_grid["value"]["port"])),
    #         ssh_password=cfg_grid["value"]["password"],
    #         ssh_username=cfg_grid["value"]["username"],
    #         remote_bind_address=(
    #                 cfg_db["value"]["host"],
    #                 int(cfg_db["value"]["port"]))) as server:
    if True:
        try:
            hConn = MySQLdb.connect(host=cfg_db['value']['host'],
                                    database=cfg_db['value']['database'],
                                    user=cfg_db['value']['username'],
                                    password=cfg_db['value']['password'])
            I_conn = True
        except Exception as e:
            I_conn = False
            print("Error connecting to database")
            print("\t" + str(e))

        dbName = cfg_db['value']['database']

        if I_conn:
            # Pretty sure we should be in the database but just in case verify it is in the information schema
            cmdStr = buildSQL_existDB(dbName)
            resSQL = pandas.read_sql(cmdStr, hConn)
            if resSQL.empty:
                print("Error: Connected to server but unable to access database")

                # Close connection
                hConn.close()
                I_conn = False

        if I_conn:
            # Make sure CrunchBase Organizations data is loaded
            tableName = "Organizations"
            I_flag, errMsg = validateDB_CB_Orgs(
                hConn, dbName, tableName,
                folderPath_CB, fileName_CB)

            if I_flag:
                print(I_flag)
                print(errMsg)
            else:
                # Check if CrunchBase Organizations data has recorded BW and SW filenames + I_response values
                I_flag, errMsg = CB_Orgs_responseFiles(
                    hConn, dbName, tableName,
                    folderPath_BW, folderPath_SW)




                # # Ready to add SimilarWeb and BuiltWith data
                # #   SimilarWeb
                # tableName = ["Visit"]
                #
                # I_flag, errMsg = SW_Visit(
                #     hConn, dbName, tableName,
                #     folderPath_tables)
                #
                # print(I_flag)
                # print(errMsg)










            # folderPath = "C:\\Users\\pjonak\\Documents\\Projects\\RKoning\\Crunchbase_SimilarWeb_BuiltWith\\tables\\"
            # tableName = ["Visit"]
            #
            # I_table = [False]*len(tableName)
            #
            #
            # colList_match = ['id_lookup','visit_granularity','visit_date']
            #
            # iTable = 0
            # fn_info = folderPath + "table_" + tableName[iTable] + "_info.txt"
            # fn_dat = folderPath + "table_" + tableName[iTable] + ".csv"
            #
            # # Validate we have our table within the database
            # #   Create otherwise
            # cmdStr = buildSQL_existTable(dbName, tableName[iTable])
            # resSQL = pandas.read_sql(cmdStr, hConn)
            # if resSQL.empty:
            #     # Need to create table
            #     #   Confirm we have table settings
            #     if os.path.exists(fn_info):
            #         # Get table info
            #         info_table = pandas.read_csv(fn_info)
            #         # Build SQL command
            #         cmdStr = buildSQL_createTable(dbName, tableName[iTable], info_table)
            #         # Get results (cannot use pandas_read_sql here, results in error)
            #         hCursor = hConn.cursor()
            #         hCursor.execute(cmdStr)
            #         resSQL = hCursor.fetchall()
            #         if len(resSQL) == 0:
            #             I_table[iTable] = True
            #         else:
            #             print("Error creating table")
            # else:
            #     I_table[iTable] = True
            #
            #
            # if I_table[iTable]:
            #     # Insert data
            #     #   Load data
            #     if os.path.exists(fn_dat):
            #         dat = pandas.read_csv(fn_dat,
            #                                 sep=',', index_col=0, header=0, encoding='utf-8')
            #
            #         I_add_any = False
            #         I_addList = numpy.asarray( [False]*dat.shape[0] )
            #
            #         for id in numpy.unique(dat['id_lookup']):
            #             # Check to see if this ID is already present
            #             #   format of dat_match
            #             #           [col1]      [col2]
            #             #   idx     [val1.1]    [val2.1]
            #             dat_match = pandas.DataFrame({'id_lookup': numpy.asarray([id], dtype=int)})
            #
            #             cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], dat_match)
            #             resSQL = pandas.read_sql(cmdStr, hConn)
            #             if resSQL.empty:
            #                 # id_lookup not present, add all
            #                 if not I_add_any:
            #                     I_add_any = True
            #                 I_addList[numpy.where( dat['id_lookup'] == id )[0]] = True
            #             else:
            #                 # id_look is present, check to see if granularity is there
            #                 for gran in numpy.unique(dat['visit_granularity'].iloc[
            #                         numpy.where(dat['id_lookup'] == id)[0] ]):
            #                     dat_match = pandas.DataFrame({
            #                         'id_lookup': numpy.asarray([id], dtype=int),
            #                         'visit_granularity': numpy.asarray([gran], dtype=str)
            #                     })
            #                     cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], dat_match)
            #                     resSQL = pandas.read_sql(cmdStr, hConn)
            #
            #                     print(gran)
            #                     print(resSQL)
            #
            #             break
            #
            #         if I_add_any:
            #             for i in range(0, len(I_addList)):
            #                 if I_addList[i]:
            #                     # Insert row
            #
            #                     cmdStr = buildSQL_insertEntry(dbName, tableName[iTable], dat.iloc[i])
            #                     hCursor = hConn.cursor()
            #                     hCursor.execute(cmdStr)
            #                     resSQL = hCursor.fetchall()
            #                     break



        if I_conn:
            hConn.close()

    return
if __name__ == '__main__':
    sys.exit( main() )