import sys
import os
import pandas
import MySQLdb
from MySQLdb.connections import Connection as mysqlConnection
from sshtunnel import SSHTunnelForwarder
import math
import numpy
import _io
import json
import datetime
import copy


I_sample = False
nSample = 50

I_intelTest = False
I_grid = True
I_log = 2 # 0 = none, 1 = print, 2 = file, 3 = print+file


folderPath_root = os.path.abspath( os.path.join( os.path.dirname( os.path.realpath(__file__) ) , os.pardir) )
if I_grid:
    folderPath_CB = os.path.join(folderPath_root, "dataset")
else:
    folderPath_CB = os.path.join(folderPath_root,"dataset","crunchbase_2017_02_06")
if I_grid:
    folderPath_BW = os.path.join(folderPath_root, "BW_responses")
    folderPath_SW = os.path.join(folderPath_root, "SW_responses")
else:
    if I_intelTest:
        folderPath_BW = os.path.join(folderPath_root, "BW_responses")
        folderPath_SW = os.path.join(folderPath_root, "SW_responses")
    else:
        folderPath_BW = os.path.join(folderPath_root,"BW_responses","run_0","response")
        folderPath_SW = os.path.join(folderPath_root,"SW_responses","run_0","response")
folderPath_tables = os.path.join(folderPath_root,"tables")
folderPath_cfg = os.path.join(folderPath_root,"keys")

cfg_db_filepath = "dbConfig.txt"
cfg_ssh_filepath = "sshConfig.txt"


# For Grid
path_ssl_ca = "/etc/mysql-ssl/ca-cert.pem"
path_ssl_cert = "/etc/mysql-ssl/client-cert.pem"
path_ssl_key = "/etc/mysql-ssl/client-key.pem"



# Which file are we looking to load?
if I_grid:
    fileName_CB = "organizations"
    fileName_CB = fileName_CB + "_trim.csv"
else:
    if I_intelTest:
        fileName_CB = "organizations_sample_intel.csv"
    else:
        fileName_CB = "organizations_trim.csv"



colTypeList = '{"founded_on": "date", "first_funding_on": "date", ' + \
              '"employee_count": "range"}'




def pjLog_open(logSettings: int = 3) -> _io.TextIOWrapper:
    if logSettings == 2 or logSettings == 3:
        # Start logging
        #   Assume current code ends in .py
        logPath = os.path.join(folderPath_root,
                               os.path.basename(os.path.realpath(__file__))[0:-3] +\
                               datetime.datetime.now().strftime("%Y-%m-%d") + \
                               ".log" )
        if os.path.exists(logPath):
            iRun = 1
            logPath = os.path.join(folderPath_root,
                                   os.path.basename(os.path.realpath(__file__))[0:-3] + \
                                   datetime.datetime.now().strftime("%Y-%m-%d") + \
                                   "__run" + str(iRun) + ".log")
            while os.path.exists(logPath):
                iRun += 1
                logPath = os.path.join(folderPath_root,
                                       os.path.basename(os.path.realpath(__file__))[0:-3] + \
                                       datetime.datetime.now().strftime("%Y-%m-%d") + \
                                       "__run" + str(iRun) + ".log")
        hFile = open(logPath, 'w', encoding="utf8")
        logMsg = "Log file for:\n\t" + os.path.realpath(__file__)
        pjLog_write(logMsg, I_log, hFile)
    else:
        hFile = None
    return hFile

def pjLog_close(logSetting: int = 3, hFile: _io.TextIOWrapper = None):
    logMsg = "Closing Logger"
    if logSetting == 1 or logSetting == 3:
        print(logMsg)
    if (logSetting == 2 or logSetting == 3) and hFile is not None:
        hFile.write(logMsg + "\n")
        hFile.close()
    return

def pjLog_write(logMsg: str, logSetting: int = 3, hFile: _io.TextIOWrapper = None):
    if logSetting > 0:
        if logSetting == 1 or logSetting == 3:
            print(logMsg)
        if (logSetting == 2 or logSetting == 3) and hFile is not None:
            hFile.write(logMsg + "\n")
    return

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
def Orgs_cleanup_byType__v2(datRow: pandas.core.frame.DataFrame,
                      colType_json: dict) -> pandas.core.frame.DataFrame:

    for colName in colType_json:
        if colName in datRow:
            if colType_json[colName] == "date":
                datRow[colName] = Orgs_cleanup_byType_date(datRow[colName])
            elif colType_json[colName] == "range":
                datRow[colName] = Orgs_cleanup_byType_range(datRow[colName])
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


    # Sometimes get nan
    if isinstance(dat, str):
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
        nLine = int(math.ceil(lenStr/perLine))

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
        dbName: str, tableName: str, tableInfo: pandas.core.frame.DataFrame, colList_pk: list = None) -> str:
    # Get column headers
    colList = tableInfo.index.values

    # Initialize SQL command
    cmdStr = "CREATE TABLE " + dbName + "." + \
             tableName + " ("

    # Add columns
    for colName in colList:
        cmdStr += colName + " " + tableInfo['colType'].ix[colName] + ","

    if colList_pk is not None:
        if len(colList_pk) == 1:
            cmdStr += "PRIMARY KEY (" + colList_pk[0] + "),"
        else:
            cmdStr += "PRIMARY KEY ("
            for colName in colList_pk:
                cmdStr += colName + ","
            cmdStr = cmdStr[0:-1] + "),"
    # if tableName == "Organizations":
    #     cmdStr += "PRIMARY KEY (uuid),"
    # elif tableName == "Visit":
    #     cmdStr += "PRIMARY KEY (id_lookup,visit_granularity,visit_date),"

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
                cmdStr += colName + "='" + buildSQL_cleanStr( datMatch[colName].iloc[0] ) + "' AND "
            else:
                cmdStr += colName + "=" + buildSQL_cleanStr( str(datMatch[colName].iloc[0]) ) + " AND "
        else:
            if isinstance(datMatch.ix[colName], str):
                # Note: dates may be read as strings
                cmdStr += colName + "='" + buildSQL_cleanStr( datMatch.ix[colName] ) + "' AND "
            else:
                cmdStr += colName + "=" + buildSQL_cleanStr( str(datMatch.ix[colName]) ) + " AND "
    cmdStr = cmdStr[0:-5] + ";"
    return cmdStr

def buildSQL_insertEntry(dbName: str, tableName: str, dat: pandas.core.frame.DataFrame) -> str:
    # Format is
    #       INSERT INTO [table] ([col1] , [col2] , ... ) VALUES ([val1] , [val2] , ... )

    # Build initial SQL command
    cmdStr = "INSERT INTO " + dbName + "." + tableName + " ("

    # Get column headers
    if dat.shape[0] == 1 and dat.shape[1] > 1:
        I_col = True
    else:
        I_col = False

    if I_col:
        colList = dat.columns.values
    else:
        colList = dat.index.values

    # Remove columns if value is nan
    delList = None
    iCol = 0
    for colName in colList:
        if (I_col and ( isinstance(dat[colName].iloc[0], float) and numpy.isnan(dat[colName].iloc[0]) ) ) or \
                (not I_col and ( isinstance(dat.ix[colName], float) and numpy.isnan(dat.ix[colName]) ) ):
            if delList is None:
                delList = [iCol]
            else:
                delList = delList + [iCol]
        iCol += 1

    if delList is not None:
        for idx in delList[::-1]:
            if I_col:
                dat = dat.drop(colList[idx], 1)
            else:
                dat = dat.drop(colList[idx], 0)
            colList = numpy.delete(colList,idx)

    # Add columns
    for colName in colList:
        cmdStr += colName + ","
    # Add values
    cmdStr = cmdStr[0:-1] + ") VALUES ("
    for colName in colList:
        if I_col:
            if isinstance(dat[colName].iloc[0], str):
                # Note: dates may be read as strings
                cmdStr += "'" + buildSQL_cleanStr( dat[colName].iloc[0] ) + "',"
            else:
                cmdStr += buildSQL_cleanStr( str(dat[colName].iloc[0]) ) + ","
        else:
            if isinstance(dat.ix[colName], str):
                # Note: dates may be read as strings
                cmdStr += "'" + buildSQL_cleanStr( dat.ix[colName] ) + "',"
            else:
                cmdStr += buildSQL_cleanStr( str(dat.ix[colName]) ) + ","
    cmdStr = cmdStr[0:-1] + ");"

    return cmdStr

def buildSQL_getNumberOfRows(dbName: str, tableName: str) -> str:
    cmdStr = 'SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA="' + \
             dbName + '" AND TABLE_NAME="' + tableName + '";'
    return cmdStr

def buildSQL_getTableColumns(dbName: str, tableName: str) -> str:
    cmdStr = 'SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA="' + \
             dbName + '" AND TABLE_NAME="' + tableName + '";'
    return cmdStr

def buildSQL_cleanStr(cmdStr: str) -> str:
    return cmdStr.encode('utf8').decode('ascii', 'ignore').replace(';','_').replace('"','_').replace("'",'_')

def strCleanup1(strIn: str) -> str:
    return strIn.encode('utf8').decode('ascii', 'ignore').replace(" ","_").replace("/","_").replace("\\","_").replace(":","_").lower()

def validateDB_CB_Orgs(
        hConn: MySQLdb.connections.Connection,dbName: str, tableName: str,
        folderPath: str, fileName: str,
        logSetting: int = 3, hFileLog: _io.TextIOWrapper = None) -> (bool, str):

    I_flag = False
    errMsg = ""

    datOrg = None # Initialize

    logMsg = "function: validateDB_CB_Orgs"
    logMsg += "\n\n\t!! Not validating or updating previous entries !!\n"
    pjLog_write(logMsg, I_log, hFileLog)

    fullPath = os.path.join(folderPath,fileName)
    fullPath_dbSetup = fullPath[0:-4] + "__dbSetup.txt"
    if not os.path.exists(fullPath):
        I_flag = True
        logMsg = "\tUnable to locate CB Organizations data\n\t\t" + fullPath
        pjLog_write(logMsg, I_log, hFileLog)
    elif not os.path.exists(fullPath_dbSetup):
        I_flag = True
        logMsg = "\tUnable to locate DB setup file for CB Organizations\n\t\t" + fullPath_dbSetup
        pjLog_write(logMsg, I_log, hFileLog)
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
            cmdStr = buildSQL_createTable(dbName, tableName, info_table, ["uuid"])
            # Get results (cannot use pandas_read_sql here, results in error)
            hCursor = hConn.cursor()
            hCursor.execute(cmdStr)
            resSQL = hCursor.fetchall()
            if len(resSQL) != 0:
                I_flag = True
                logMsg = "\tError creating CB Organizations table\n\t\t" + cmdStr
                pjLog_write(logMsg, I_log, hFileLog)
            else:
                id_lookup_max = 10001
        else:
            I_createTable = False

            # Get current number of rows and current columns
            cmdStr = buildSQL_getNumberOfRows(dbName,tableName)
            resSQL = pandas.read_sql(cmdStr, hConn)
            db_nRow = resSQL['TABLE_ROWS'].iloc[0]

            cmdStr = buildSQL_getTableColumns(dbName,tableName)
            resSQL = pandas.read_sql(cmdStr, hConn)
            db_colList = list(resSQL['COLUMN_NAME'])

            # Get max id_lookup
            #   Query
            cmdStr = "SELECT MAX(id_lookup) FROM " + dbName + "." + tableName + ";"
            resSQL = pandas.read_sql(cmdStr, hConn)

            if resSQL.empty:
                I_flag = True
                logMsg = "\tError querying CB Organizations table with:\n\t\t" + cmdStr
                pjLog_write(logMsg, I_log, hFileLog)
            else:
                id_lookup_max = resSQL['MAX(id_lookup)'].iloc[0]
                if id_lookup_max is None:
                    id_lookup_max = 10001

        if not I_flag:
            # Load CB Orgs data
            datOrg = pandas.read_csv(fullPath, sep=',', encoding='utf-8', index_col=0)

            if I_sample:
                print("Sample")
                print(datOrg.shape)
                datOrg = datOrg.iloc[0:nSample]
                print(datOrg.shape)

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

            if I_saveOrg:
                logMsg = "\tSaving Organizations data"
                pjLog_write(logMsg, I_log, hFileLog)

                datOrg.to_csv(fullPath, sep=',', encoding='utf-8')

                logMsg = "\t\tDone"
                pjLog_write(logMsg, I_log, hFileLog)


            # Add info on BW and SW response files
            datOrg, I_flag2, errMsg2 = CB_Orgs_responseFiles(folderPath_BW, folderPath_SW, datOrg, I_log, hFileLog)


            # Update database according to CB Orgs file
            #   First find out which entries are to be added and which are to be updated
            if I_createTable:
                I_addList = numpy.asarray([True] * datOrg.shape[0])
                I_updateList = numpy.asarray([False] * datOrg.shape[0])

            else:
                I_addList = numpy.asarray([False] * datOrg.shape[0])
                I_updateList = numpy.asarray([False] * datOrg.shape[0])

                for iOrg in range(0, datOrg.shape[0]):
                    cmdStr = buildSQL_getEntry(dbName, tableName, pandas.DataFrame({
                        'uuid': numpy.asarray([datOrg['uuid'].iloc[iOrg]], dtype=str) }) )
                    resSQL = pandas.read_sql(cmdStr, hConn)
                    if resSQL.empty:
                        # Entry doesn't exist
                        I_addList[iOrg] = True

                    # ELSE
                        # Entry does exist, validate


            # Will need to do some clean-up if adding or updating
            #   Dates
            #   employee_count
            datHeader = list(datOrg)
            colList_clean = json.loads(colTypeList)
            colIdxList_clean = colName_2_colIdx__pType(datHeader, colList_clean)

            if any(I_addList):
                logMsg = "\tAdd Organization entries"
                pjLog_write(logMsg, I_log, hFileLog)
                pandas.options.mode.chained_assignment = None  # default='warn'
                for iRow in range(0, datOrg.shape[0]):
                    if I_addList[iRow]:
                        # Want to add this entry
                        #   Clean up
                        #   Add entry to database
                        cmdStr = buildSQL_insertEntry(dbName, tableName,
                                                      Orgs_cleanup_byType__v2(datOrg[colList_db].iloc[iRow],
                                                                              colList_clean) )
                        hCursor = hConn.cursor()
                        try:
                            hCursor.execute(cmdStr)
                            resSQL = hCursor.fetchall()
                        except Exception as e:
                            logMsg = "\tError adding " + datOrg['domain'].iloc[iRow] + "\n\t\t" + cmdStr + "\n\t\t" + str(e)
                            pjLog_write(logMsg, I_log, hFileLog)
                logMsg = "\t\tDone adding entries"
                pjLog_write(logMsg, I_log, hFileLog)
                pandas.options.mode.chained_assignment = 'warn'

            # if I_update_any:



    return I_flag, errMsg

def CB_Orgs_responseFiles(
        fp_BW: str, fp_SW: str, datOrg: pandas.core.frame.DataFrame,
        logSetting: int = 3, hFileLog: _io.TextIOWrapper = None) -> (pandas.core.frame.DataFrame, bool, str):
    I_flag = False
    errMsg = ""

    logMsg = "function: CB_Orgs_responseFiles"
    pjLog_write(logMsg, I_log, hFileLog)

    if I_intelTest:
        fileName_dat_BW = "dat__intel.csv"
        fileName_dat_SW = "dat__intel.csv"
    else:
        fileName_dat_BW = "dat_2017_03_30.csv"
        fileName_dat_SW = "dat_2017_04_01.csv"

    # Ready to add BuiltWith data to Org data
    #   Prepare CB Org data columns
    colList = ['bw_filename', 'sw_filename', 'bw_I_response', 'sw_I_response']

    # Get list of current column names
    #   If one of the new columns isn't present, add it
    colList_orig = numpy.asarray(list(datOrg))
    for colName in colList:
        if not any(colList_orig == colName):
            datOrg[colName] = numpy.nan

    # Get filenames and I_response values
    #   Load response dat file first, then SW dat file
    #       Match each row to Organizations data
    #       Record filename and I_response
    #       Verify I_response

    folderList_response = [fp_BW, fp_SW]
    fileList_response = [fileName_dat_BW, fileName_dat_SW]
    colList_fn_response = ['bw_filename', 'sw_filename']
    colList_fn_Org = ['bw_filename', 'sw_filename']
    colList_I_response = ['bw_I_response', 'sw_I_response']
    colList_I_Org = ['bw_I_response', 'sw_I_response']

    pandas.options.mode.chained_assignment = None
    for i in range(0, 2):
        fullPath = os.path.join(folderList_response[i],fileList_response[i])
        if not os.path.exists(fullPath):
            logMsg = "\tUnable to find dat file\n\t\t" + fullPath
            pjLog_write(logMsg, I_log, hFileLog)
        else:
            datResponse = pandas.read_csv(fullPath, sep=',', index_col=0, header=0, encoding='utf-8')

            # Now we need to match each row to Organizations data
            #   We preserved the index column
            #   There is less data in the response file so start with that
            for iRow in range(0, datResponse.shape[0]):
                # Get index
                idx = int(datResponse.iloc[iRow].name)
                # Does this index occur in the Organizations data?
                if idx in datOrg.index:
                    # Record filename and I_response
                    datOrg[colList_fn_Org[i]].ix[idx] = datResponse[colList_fn_response[i]].iloc[iRow]
                    datOrg[colList_I_Org[i]].ix[idx] = datResponse[colList_I_response[i]].iloc[iRow]
                    # Verify I_response
                    if datOrg[colList_I_Org[i]].ix[idx] == 1:
                        fullPath = os.path.join(folderList_response[i], datOrg[colList_fn_Org[i]].ix[idx])
                        if not os.path.exists(fullPath):
                            datOrg[colList_I_Org[i]].ix[idx] = 0
    pandas.options.mode.chained_assignment = 'warn'

    return datOrg, I_flag, errMsg


def SW_Visit(
        hConn: MySQLdb.connections.Connection, dbName: str, tableName: list, tableNameOrg: str,
        folderPath: str,
        logSetting: int = 3, hFileLog: _io.TextIOWrapper = None) -> (bool, str):
    I_flag = False
    errMsg = ""

    logMsg = "function: Sw_Visit"
    pjLog_write(logMsg, I_log, hFileLog)

    colName_response = "sw_I_response"
    colName_filename = "sw_filename"

    I_table = [False] * len(tableName)

    iTable = 0

    if tableName[iTable] == "Visit":
        colList_pk = ['id_lookup', 'visit_granularity', 'visit_date']

    fn_info = os.path.join(folderPath_tables, "table_" + tableName[iTable] + "__dbSetup.txt")

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
            cmdStr = buildSQL_createTable(dbName, tableName[iTable], info_table, colList_pk)
            # Get results (cannot use pandas_read_sql here, results in error)
            hCursor = hConn.cursor()
            hCursor.execute(cmdStr)
            resSQL = hCursor.fetchall()
            if len(resSQL) == 0:
                I_table[iTable] = True
            else:
                I_flag = True
                logMsg = "\tError creating table " + tableName[iTable]
                pjLog_write(logMsg, I_log, hFileLog)
    else:
        I_table[iTable] = True

    if I_table[iTable]:
        # Get min and max id_lookups from Organizations table
        cmdStr = "SELECT MAX(id_lookup), MIN(id_lookup) FROM " + dbName + "." + tableNameOrg + ";"
        resSQL = pandas.read_sql(cmdStr, hConn)

        id_lookup_min = resSQL['MIN(id_lookup)'].iloc[0]
        id_lookup_max = resSQL['MAX(id_lookup)'].iloc[0]

        # Go through all IDs
        for id in range(id_lookup_min, 1+id_lookup_max):
            cmdStr = buildSQL_getEntry(dbName, tableNameOrg,
                                       pandas.DataFrame({'id_lookup': numpy.asarray([id], dtype=int)}) )
            try:
                resSQL = pandas.read_sql(cmdStr, hConn)
                I_validID = True
            except:
                I_validID = False

            if I_validID and resSQL[colName_response].iloc[0]==1:
                # Load file
                filename = resSQL[colName_filename].iloc[0]

                # Sometimes the filename is broken?
                if isinstance(filename,str) and len(filename)>30 and filename[-5::] != ".json":
                    logMsg = "\tWarning: incomplete response filename for id_lookup=" + str(id) + "\n\t\t" + filename
                    pjLog_write(logMsg, I_log, hFileLog)

                    if filename[-4::] == ".jso":
                        filename += "n"
                        logMsg = "\t\tUpdated filename = " + filename
                        pjLog_write(logMsg, I_log, hFileLog)
                    elif filename[-3::] == ".js":
                        filename += "on"
                        logMsg = "\t\tUpdated filename = " + filename
                        pjLog_write(logMsg, I_log, hFileLog)
                    elif filename [-2::] == ".j":
                        filename += "son"
                        logMsg = "\t\tUpdated filename = " + filename
                        pjLog_write(logMsg, I_log, hFileLog)

                fullPath = os.path.join(folderPath_SW,filename)
                if not os.path.exists(fullPath):
                    logMsg = "\tUnable to find SW response file\n\t\t" + fullPath
                    pjLog_write(logMsg, I_log, hFileLog)
                else:
                    hFile = _io.open(fullPath)
                    try:
                        datJ = json.loads(hFile.read())
                        I_read = True
                    except Exception as e:
                        I_read = False
                        logMsg = "\tUnable to read SW response file\n\t\t" + fullPath
                        pjLog_write(logMsg, I_log, hFileLog)
                    hFile.close()

                    if I_read:
                        # Get granularity
                        granularity = datJ['meta']['request']['granularity']

                        # Get visit data
                        iVisit = 0
                        visitDate = datetime.datetime.strptime(datJ['visits'][iVisit]['date'], "%Y-%m-%d").date()
                        visitDate_min = visitDate
                        visitDate_max = visitDate
                        if len(datJ['visits']) > 1:
                            for iVisit in range(1, len(datJ['visits'])):
                                visitDate = datetime.datetime.strptime(datJ['visits'][iVisit]['date'], "%Y-%m-%d").date()

                                if visitDate < visitDate_min:
                                    visitDate_min = visitDate
                                elif visitDate > visitDate_max:
                                    visitDate_max = visitDate

                        # Check to see if database has entries for both min and max dates already at this granularity
                        cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], pandas.DataFrame({
                                    'id_lookup': numpy.asarray([id], dtype=int),
                                    'visit_granularity': numpy.asarray([granularity], dtype=str),
                                    'visit_date': numpy.asarray([visitDate_min], dtype=str) }) )
                        resSQL = pandas.read_sql(cmdStr, hConn)
                        if resSQL.empty:
                            # Combination not present, add all
                            I_add = True
                        else:
                            cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], pandas.DataFrame({
                                'id_lookup': numpy.asarray([id], dtype=int),
                                'visit_granularity': numpy.asarray([granularity], dtype=str),
                                'visit_date': numpy.asarray([visitDate_max], dtype=str)}))
                            resSQL = pandas.read_sql(cmdStr, hConn)
                            if resSQL.empty:
                                # Combination not present, add all
                                I_add = True
                            else:
                                I_add = False

                        if I_add:
                            for iVisit in range(1, len(datJ['visits'])):
                                visitCt = datJ['visits'][iVisit]['visits']
                                visitDate = datetime.datetime.strptime(datJ['visits'][iVisit]['date'], "%Y-%m-%d").date()

                                # Try to insert
                                cmdStr = buildSQL_insertEntry(dbName, tableName[iTable], pandas.DataFrame({
                                        'id_lookup': numpy.asarray([id], dtype=int),
                                        'visit_granularity': numpy.asarray([granularity], dtype=str),
                                        'visit_date': numpy.asarray([visitDate], dtype=str),
                                        'visit_count': numpy.asarray([visitCt], dtype=str) }) )
                                hCursor = hConn.cursor()
                                try:
                                    hCursor.execute(cmdStr)
                                    resSQL = hCursor.fetchall()
                                except Exception as e:
                                    logMsg = "\tUnable to add SW data for id_lookup=" + str(id) + \
                                             ", date=" + str(visitDate) + "\n\t\t" + cmdStr + "\n\t\t" + str(e)
                                    pjLog_write(logMsg, I_log, hFileLog)
    return I_flag, errMsg


def BW_Tech(
        hConn: MySQLdb.connections.Connection, dbName: str, tableName: list, tableNameOrg: str,
        folderPath: str,
        logSetting: int = 3, hFileLog: _io.TextIOWrapper = None) -> (bool, str):
    I_flag = False
    errMsg = ""

    logMsg = "function: BW_Tech"
    pjLog_write(logMsg, I_log, hFileLog)


    colName_response = "bw_I_response"
    colName_filename = "bw_filename"

    tableName = numpy.asarray(tableName) # will be easier to work with later than a list

    I_table = [False] * len(tableName)

    for iTable in range(0, len(tableName)):

        if tableName[iTable] == "Tech":
            colList_pk = ['id_tech', 'id_techTag', 'id_techCat']
        elif tableName[iTable] == "TechTag":
            colList_pk = ['id_techTag']
        elif tableName[iTable] == "TechCat":
            colList_pk = ['id_techCat']
        elif tableName[iTable] == "DomainTech":
            colList_pk = ['id_lookup']
        else:
            colList_pk = None

        fn_info = os.path.join(folderPath_tables, "table_" + tableName[iTable] + "__dbSetup.txt")

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
                cmdStr = buildSQL_createTable(dbName, tableName[iTable], info_table, colList_pk)
                # Get results (cannot use pandas_read_sql here, results in error)
                hCursor = hConn.cursor()
                hCursor.execute(cmdStr)
                resSQL = hCursor.fetchall()
                if len(resSQL) == 0:
                    I_table[iTable] = True
                else:
                    I_flag = True
                    logMsg = "\tError creating table " + tableName[iTable] + "\n\t\t" + cmdStr
                    pjLog_write(logMsg, I_log, hFileLog)
        else:
            I_table[iTable] = True

    if not any(I_table):
        I_flag = True
        errMsg = "No BW tables"
    else:
        # Get min and max id_lookups from Organizations table
        cmdStr = "SELECT MAX(id_lookup), MIN(id_lookup) FROM " + dbName + "." + tableNameOrg + ";"
        resSQL = pandas.read_sql(cmdStr, hConn)

        id_lookup_min = resSQL['MIN(id_lookup)'].iloc[0]
        id_lookup_max = resSQL['MAX(id_lookup)'].iloc[0]

        logMsg = "Loading and acting on BuiltWith response files"
        pjLog_write(logMsg, I_log, hFileLog)

        if not any(tableName=="metaBW") or not I_table[tableName=="metaBW"]:
            logMsg = "\t!! Will not be storing BW meta data !!\n\t\tTable unavailable or not requested\n"
            pjLog_write(logMsg, I_log, hFileLog)

        if not all(I_table[tableName!="metaBW"]):
            logMsg = "\t!! Will not be storing BW data related to technologies !!\n\t\tAll tables must be available and requested together\n"
            pjLog_write(logMsg, I_log, hFileLog)
        else:
            # Get max
            #   id_tech
            #   id_techTag
            #   id_techCat
            for tname in tableName[tableName!="metaBW"]:
                if tname.lower() == "tech":
                    cmdStr = "SELECT MAX(id_tech) FROM " + dbName + "." + tname + ";"
                    resSQL = pandas.read_sql(cmdStr, hConn)
                    id_tech_max = resSQL['MAX(id_tech)'].iloc[0]
                    if id_tech_max is None:
                        id_tech_max = 2000
                elif tname.lower() == "techtag":
                    cmdStr = "SELECT MAX(id_techTag) FROM " + dbName + "." + tname + ";"
                    resSQL = pandas.read_sql(cmdStr, hConn)
                    id_techTag_max = resSQL['MAX(id_techTag)'].iloc[0]
                    if id_techTag_max is None:
                        id_techTag_max = 1000
                elif tname.lower() == "techcat":
                    cmdStr = "SELECT MAX(id_techCat) FROM " + dbName + "." + tname + ";"
                    resSQL = pandas.read_sql(cmdStr, hConn)
                    id_techCat_max = resSQL['MAX(id_techCat)'].iloc[0]
                    if id_techCat_max is None:
                        id_techCat_max = 0


        # Go through all IDs
        for id in range(id_lookup_min, 1 + id_lookup_max):
            pjLog_write(logMsg, I_log, hFileLog)

            cmdStr = buildSQL_getEntry(dbName, tableNameOrg,
                                       pandas.DataFrame({'id_lookup': numpy.asarray([id], dtype=int)}))
            try:
                resSQL = pandas.read_sql(cmdStr, hConn)
                I_validID = True
            except:
                I_validID = False

            if I_validID and resSQL[colName_response].iloc[0] == 1:
                resSQL_id = copy.deepcopy(resSQL)

                # Load file
                filename = resSQL[colName_filename].iloc[0]
                fullPath = os.path.join(folderPath_BW, filename)
                if not os.path.exists(fullPath):
                    logMsg = "\tUnable to find BW response file for id_lookup=" + str(id) + "\n\t\t" + fullPath
                    pjLog_write(logMsg, I_log, hFileLog)
                else:
                    hFile = _io.open(fullPath)
                    try:
                        datJ = json.loads(hFile.read())
                        I_read = True
                    except Exception as e:
                        I_read = False
                        logMsg = "\tUnable to read SW response file\n\t\t" + fullPath
                        pjLog_write(logMsg, I_log, hFileLog)
                    hFile.close()

                    if I_read:
                        # Validate that the response has data
                        #   Gotta love those double negatives!
                        if not not datJ['Errors']:
                            logMsg = "\tError detected within BW response file for id_lookup=" + str(id)
                            pjLog_write(logMsg, I_log, hFileLog)
                        else:
                            if len(list(datJ['Results'])) == 1:
                                iLU = 0
                            else:
                                iLU = -1

                                logMsg = "\tMultiple results within BW response file for id_lookup=" + str(id)
                                pjLog_write(logMsg, I_log, hFileLog)

                                for i in range(0, len(list(datJ['Results']))):
                                    if datJ['Results'][i]['Lookup'].lower() == resSQL_id['domain'].iloc[0]:
                                        iLU = i

                                        logMsg = "\t\tUsing iLU=" + str(iLU)
                                        pjLog_write(logMsg, I_log, hFileLog)

                                        break

                            if iLU == -1:
                                logMsg = "\t\t!! No valid domain matchup found for id_lookup=" + str(id)
                                pjLog_write(logMsg, I_log, hFileLog)
                            else:
                                # We know which ['Results'] we are adding
                                #   Update each table
                                #       Do metaBW separately, the rest together
                                if any(tableName=="metaBW") and I_table[tableName=="metaBW"]:
                                    iTable = int( numpy.arange(len(tableName))[tableName=="metaBW"] )
                                    # Do we already have this entry?
                                    cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], pandas.DataFrame({
                                        'id_lookup': numpy.asarray([id], dtype=int) }) )
                                    resSQL = pandas.read_sql(cmdStr, hConn)
                                    if resSQL.empty:
                                        # Entry not present, try to insert

                                        indexFirst = int(datJ['Results'][iLU]['FirstIndexed']) / 1000
                                        indexLast = int(datJ['Results'][iLU]['LastIndexed']) / 1000

                                        cmdStr = buildSQL_insertEntry(dbName, tableName[iTable], pandas.DataFrame({
                                            'id_lookup': numpy.asarray([id], dtype=int),
                                            'companyname': numpy.asarray([ datJ['Results'][iLU]['Meta']['CompanyName'] ], dtype=str),
                                            'vertical': numpy.asarray([ datJ['Results'][iLU]['Meta']['Vertical'] ], dtype=str),
                                            'arank': numpy.asarray([ datJ['Results'][iLU]['Meta']['ARank'] ], dtype=int),
                                            'qrank': numpy.asarray([ datJ['Results'][iLU]['Meta']['QRank'] ], dtype=int),
                                            'IndexedFirst': numpy.asarray([indexFirst], dtype=int),
                                            'IndexedFirst_date': numpy.asarray([ datetime.datetime.fromtimestamp(indexFirst).date() ], dtype=str),
                                            'IndexedLast': numpy.asarray([indexLast], dtype=int),
                                            'IndexedLast_date': numpy.asarray([ datetime.datetime.fromtimestamp(indexLast).date() ], dtype=str) }) )
                                        hCursor = hConn.cursor()
                                        try:
                                            hCursor.execute(cmdStr)
                                            resSQL = hCursor.fetchall()
                                        except Exception as e:
                                            logMsg = "\tUnable to add BW meta data for id_lookup=" + str(id) + \
                                                     "\n\t\t" + cmdStr + "\n\t\t" + str(e)
                                            pjLog_write(logMsg, I_log, hFileLog)
                                        # try:
                                        #     resSQL = pandas.read_sql(cmdStr, hConn)
                                        # except Exception as e:
                                        #     logMsg = "\tUnable to add BW meta data for id_lookup=" + str(id) + \
                                        #              "\n\t\t" + str(e)
                                        #     pjLog_write(logMsg, I_log, hFileLog)

                                # Can now do the others if they are available
                                if all(I_table[tableName != "metaBW"]):
                                    # Get tech, tag and category
                                    for iPaths in range(0, len(list(datJ['Results'][iLU]['Result']['Paths']))):

                                        for iTech in range(0, len(
                                                list(datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'])
                                                ) ):

                                            I_addName = False
                                            techName = strCleanup1(
                                                datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                    'Name'] )
                                            detectedFirst = int(
                                                datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                    'FirstDetected'] ) / 1000
                                            detectedLast = int(
                                                datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                    'LastDetected'] ) / 1000

                                            I_addTag = False
                                            techTag = datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech]['Tag']
                                            if techTag is None:
                                                techTag = "none"
                                            else:
                                                techTag = strCleanup1(techTag)

                                            techCatList = datJ['Results'][iLU]['Result']['Paths'][iPaths]['Technologies'][iTech][
                                                'Categories']
                                            if techCatList is None:
                                                techCatList = "None"
                                            if isinstance(techCatList, list):
                                                for iCat in range(0, len(techCatList)):
                                                    techCatList[iCat] = strCleanup1(techCatList[iCat])
                                            else:
                                                techCatList = [strCleanup1(techCatList)]

                                            # Want to convert name, tag and cat to IDs
                                            #   Initialize IDs for tag and category
                                            id_tech = None
                                            id_tag = None
                                            idList_cat = [None] * len(techCatList)

                                            # Check to see if we have the Tag
                                            cmdStr = buildSQL_matchEntry(dbName, tableName[iTable], pandas.DataFrame({
                                                'name_techTag': numpy.asarray([techTag], dtype=str)}))
                                            resSQL = pandas.read_sql(cmdStr, hConn)
                                            if resSQL.empty:
                                                # Entry not present, try to insert
                                                id_tag = id_techTag_max+1



                                            else:
                                                # Get
                                                id_tag = int( resSQL['id_techTag'].iloc[0] )



    return I_flag, errMsg


def main():
    # Start logging
    hFileLog = pjLog_open(I_log)

    # Connect to database
    fullPath = os.path.join(folderPath_cfg, cfg_db_filepath)
    if not os.path.exists(fullPath):
        cfg_db = None
        logMsg = "Unable to locate database connection settings at " + fullPath
        pjLog_write(logMsg, I_log, hFileLog)
    else:
        cfg_db = pandas.read_csv(fullPath)

    # fullPath = os.path.join(folderPath_cfg, cfg_ssh_filepath)
    # if not os.path.exists(fullPath):
    #     cfg_grid = None
    #     logMsg = "Unable to locate SSH connection settings at " + fullPath
    #     pjLog_write(logMsg, I_log, hFileLog)
    # else:
    #     cfg_grid = pandas.read_csv(fullPath)
    #
    # print(cfg_grid["value"]["host"])

    # # Use with local machine to remote DB
    # #     No longer working???
    # with SSHTunnelForwarder(
    #         (cfg_grid["value"]["host"], int(cfg_grid["value"]["port"])),
    #         ssh_password=cfg_grid["value"]["password"],
    #         ssh_username=cfg_grid["value"]["username"],
    #         remote_bind_address=(
    #                 cfg_db["value"]["host"],
    #                 int(cfg_db["value"]["port"]))) as server:
    if True:
        try:
            if I_grid:
                hConn = MySQLdb.connect(host=cfg_db['value']['host'],
                                        database=cfg_db['value']['database'],
                                        user=cfg_db['value']['username'],
                                        password=cfg_db['value']['password'],
                                        ssl={'ca': path_ssl_ca,
                                             'cert': path_ssl_cert,
                                             'key': path_ssl_key} )
            else:
                hConn = MySQLdb.connect(host=cfg_db['value']['host'],
                                        database=cfg_db['value']['database'],
                                        user=cfg_db['value']['username'],
                                        password=cfg_db['value']['password'] )

            I_conn = True
            dbName = cfg_db['value']['database']
        except Exception as e:
            I_conn = False
            logMsg = "Error connecting to database: " + str(e)
            pjLog_write(logMsg, I_log, hFileLog)

        if I_conn:
            # Pretty sure we should be in the database but just in case verify it is in the information schema
            cmdStr = buildSQL_existDB(dbName)
            resSQL = pandas.read_sql(cmdStr, hConn)
            if resSQL.empty:
                logMsg = "Error: Connected to server but unable to access database"
                pjLog_write(logMsg, I_log, hFileLog)

                # Close connection
                hConn.close()
                I_conn = False

        if I_conn:
            # Make sure CrunchBase Organizations data is loaded
            tableNameOrg = "Organizations"
            I_flag, errMsg = validateDB_CB_Orgs(
                hConn, dbName, tableNameOrg,
                folderPath_CB, fileName_CB,
                I_log, hFileLog)

            if I_flag:
                logMsg = "end of function: validateDB_CB_Orgs\n\tFlagged\n\tMessage: " + errMsg
                pjLog_write(logMsg, I_log, hFileLog)

            else:
                # Ready to add SimilarWeb and BuiltWith data
                #   SimilarWeb
                tableName = ["Visit"]

                I_flag, errMsg = SW_Visit(
                    hConn, dbName, tableName, tableNameOrg,
                    folderPath_tables,
                    I_log, hFileLog)

                if I_flag:
                    logMsg = "end of function: SW_Visits\n\tFlagged\n\tMessage: " + errMsg
                    pjLog_write(logMsg, I_log, hFileLog)


                # tableName = ["metaBW","Tech", "TechTag", "TechCat", "DomainTech"]
                # tableName = ["metaBW"]
                #
                # I_flag, errMsg = BW_Tech(
                #     hConn, dbName, tableName, tableNameOrg,
                #     folderPath_tables,
                #     I_log, hFileLog)
                #
                # if I_flag:
                #     logMsg = "end of function: BW_Tech\n\tFlagged\n\tMessage: " + errMsg
                #     pjLog_write(logMsg, I_log, hFileLog)

        if I_conn:
            hConn.close()

    pjLog_close(I_log, hFileLog)

    return
if __name__ == '__main__':
    sys.exit( main() )