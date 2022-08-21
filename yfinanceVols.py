# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 11:08:24 2020

@author: ZCS0349
"""

import pandas as pd
import yfinance as yf
import numpy as np
import VaRStats as vs
from datetime import timedelta, datetime


def executeSQL(DSN, sql):
    
    results = None
    pyodbc.pooling = False
    cnxn = pyodbc.connect("DSN=" + DSN, autocommit = True)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    try:
        results = cursor.fetchall()
        cnxn.close()
    except:
        None
    return results 


def dataFrameToSQL(DSN, df, tablename, csvpath, convert, encodingformat = None):

    #Dumps dataframe (df) to sql server (DSN) table (tablename) using bulk insert from csv (csvpath)
    #Forces conversion to existing table's column datatypes if convert == True
    #If convert == False, insert will fail if datatypes do not align properly
    df.to_csv(csvpath, index = False, header = False, sep = '~', encoding = encodingformat)
    conversiondictionary = {'int64' : 'int', 'datetime64[ns]' : 'smalldatetime', 'datetime32[ns]' : 'smalldatetime', 'object' : 'text', 'float64' : 'float', 'float32' : 'float', '<M8[ns]' : 'smalldatetime', 'bool' : 'varchar(5)'}
    alltables = sqlToDataFrame(DSN, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = 'RISKPOC'")
    tableexists = False
    for table in alltables['TABLE_NAME']:
        if tablename.lower() == table.lower():
            tableexists = True
    if not tableexists:
        print ('Table does not already exist. Creating new table and inserting data.')
        sql = 'create table ' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)]                    
            else:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ','
        sql = sql + ');'
        executeSQL(DSN, sql)
    else:
        print ('Table already exists. Inserting data into existing table.')
    if convert == False:
        sql = 'create table #' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)]                
            else:
                sql = sql + '"' + str(df.columns[column]) + '" ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ','
        sql = sql + '); bulk insert #' + tablename + " from '" + csvpath + "' with (FIELDTERMINATOR = '~') insert into " + tablename + ' select '
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + conversiondictionary[str(df[df.columns[column]].dtype)] + ')'            
            else:
                sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + conversiondictionary[str(df[df.columns[column]].dtype)] + '),'
        sql = sql + ' from #' + tablename + '; drop table #' + tablename + ';'
        executeSQL(DSN, sql)       
    else:
        existingdtypes = sqlToDataFrame(DSN, "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "'")
        sql = 'create table #' +  tablename + '('
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column]
                else:
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + ')'
            else:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + ','
                else:
                    sql = sql + '"' + str(df.columns[column]) + '" ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + '),'        
        sql = sql + '); bulk insert #' + tablename + " from '" + csvpath + "' with (FIELDTERMINATOR = '~') insert into " + tablename + ' select '
        for column in range(0, len(df.columns)):
            if column == len(df.columns) - 1:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + ')'
                else:
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + '))'
            else:
                if pd.isnull(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column]) or existingdtypes['DATA_TYPE'][column] == 'text': #No maxmimum length for this datatype
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '),'
                else:
                    sql = sql + 'cast("' + str(df.columns[column]) + '" as ' + existingdtypes['DATA_TYPE'][column] + '(' + str(int(existingdtypes['CHARACTER_MAXIMUM_LENGTH'][column])) + ')),'
        sql = sql + ' from #' + tablename + '; drop table #' + tablename + ';'
        executeSQL(DSN, sql)
        #return sql

totaldaysback = 365


#voldays = [(datetime.now().date() - timedelta(days = daysback)) for daysback in range(0, totaldaysback)]
          
alldays = [(datetime.now().date() - timedelta(days = daysback)) for daysback in range(0, totaldaysback)]
alldays.sort()

maxdt = alldays[-1]
mindt = alldays[0]

tickers = open("//tedfil01/datadropdev/PythonPOC/Download_CSVs/neededtickers.csv","r",encoding="utf -8")
contents = tickers.readlines()
tickers.close()
contents.pop(0)

try:


    frame = pd.DataFrame()
    for i in contents:
     
        x = yf.download(i, start=mindt, end=maxdt)
 
        x = x.reset_index()
        z = x.copy()
        z = (z[['Date','Adj Close']])
        z['Ticker'] = i
        frame = frame.append(z)      

    frame2 = frame.pivot_table(index = 'Date', columns = 'Ticker', values = 'Adj Close')
    frame2 = frame2.sort_index()
    
    # calculate the natural log return
    
    nat_log = frame2.pct_change()
    
    
    numOfRows = len(nat_log.index)
    numOfRows = numOfRows -1
    #calculate the annual volatility
    annual_volatility = pd.DataFrame()
    annual_volatility = nat_log.rolling(window = numOfRows).std()*np.sqrt(numOfRows)
    
    # drop all the null rows 
    annual_volatility = annual_volatility.dropna(how = 'all')
    dtvar = annual_volatility.reset_index()
    dtvar = dtvar['Date']
    
    
    
    annual_volatlity2 = annual_volatility.transpose()
    annual_volatlity2['Dt'] = dtvar[0]
    
    annual_volatlity2 = annual_volatlity2.reset_index()
    
    annual_volatlity2.rename(columns={dtvar[0]:'Vol'}, inplace=True)
    
    cols = annual_volatlity2.columns.tolist()
    
    cols = cols[-1:] + cols[:-1]
    
    annual_volatlity2 = annual_volatlity2[cols]
    
    annual_volatlity2 = annual_volatlity2.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    executeSQL('RiskPOC', "delete from CompanyVols where dt = '" + dtvar[0].strftime("%Y-%m-%d") + "'")
    dataFrameToSQL('RiskPOC', annual_volatlity2, 'CompanyVols', "//tedfil01/DataDropDEV/PythonPOC/Upload_CSVs/companyvol.csv", True)
#

except Exception as e:

            print (str(e))
