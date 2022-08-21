# -*- coding: utf-8 -*-
"""
Created on Fri May  6 14:23:58 2022

@author: ZCS0349
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from datetime import datetime, date, timedelta
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=TEDSQL050;'
                      'Database=RiskPOC;'
                      'Trusted_Connection=yes;')


def sqlToDataFrame(DSN, sql):
    
    pyodbc.pooling = False
    cnxn = pyodbc.connect("DSN=" + DSN, autocommit = True)
    df = pd.read_sql(sql, cnxn)
    cnxn.close()
    return df

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

try:
    
    netshortlong2 = pd.DataFrame()

    for i in range(10):
    
        sqldata = db.sqlToDataFrame('RiskPOC',"""
                                      select * from newshortlong2
                                      order by actual_date
                                      """)
        sqldata = sqldata.rename(columns = {'actual_date':'Start Date', 'adj_net_long':'net long', 'adj_net_short':'net short', 'price':'Price'})
        sqldata['ctyr'] = sqldata['contract_com']+sqldata['yr']
        sqldata['Start Date'] = pd.to_datetime(sqldata['Start Date']).dt.date
        
        info = sqldata.copy()
        info2 = info['ctyr'].unique()
        
        lastDate = sqldata['Start Date'].max() 
        sqldata = sqldata.loc[sqldata['Start Date'] >= lastDate]
        maxdate = sqldata['Start Date'].max().strftime('%m-%d-%Y')
        
        cmeprelim = db.sqlToDataFrame('RiskPOC',"""
                                 select * 
                                 from cmePrelimData
                                 where Currdate >= '""" + maxdate + """'
                                 """)
            
        cmehistory =  db.sqlToDataFrame('RiskPOC',"""
                        select *
                        from cmeHistory
                        where marketdate >= '""" + maxdate + """'
                        order by contract_com, yr, marketdate
                    """)
        
        cmehistory['Commodity'] = cmehistory['contract_com'].str.replace(' ', '')
        tickerdict = {'C':'Corn', 'CL':'Crude Oil', 'NG':'Henry Hub Natural Gas', 'KW':'KC HRW Wheat', 'S':'Soybean', 'SM':'Soybean Meal', 'HO':'NY Harbor ULSD', 'FC':'Feeder Cattle', 'BO':'Soybean Oil', 'XB':'RBOB Gasoline', 'O':'Oats', 'LH':'Lean Hog', 'LC':'Live Cattle', 'RR':'Rough Rice', 'CUA':'Chicago Ethanol (Platts)', 'BZA':'Brent Last Day Financial', 'W':'Chicago SRW Wheat'} # add chicago wheat
        cmehistory = cmehistory.replace({"Commodity":tickerdict})
        cmehistory = cmehistory[['MarketDate', 'OpenInterest', 'Settle', 'contract_com', 'yr', 'Commodity']].reset_index().drop(columns = 'index')
        
        newdata = cmeprelim.merge(cmehistory, how = 'left', left_on =['Currdate', 'Posmonth', 'Commodity'], right_on = ['MarketDate', 'yr', 'Commodity'])
        newdata = newdata[['Currdate', 'Posmonth', 'OpenInterestClose', 'OpenInterestChange', 'Settle', 'Commodity']]
        newdata = newdata.rename(columns = {'Currdate':'MarketDate', 'OpenInterest':'OpenInterestClose', 'Posmonth':'yr', 'Commodity':'contract_com'}) #, 'Commodity':'contract_com'})
        
        newdata = newdata.dropna()
        newdata = newdata.drop_duplicates()
        
        # get difference in price between rows (/100 because it's in cents )
        newdata = newdata.sort_values(by = ['MarketDate', 'contract_com', 'yr'])
        newdata['price change']  = pd.DataFrame((newdata.groupby(['contract_com', 'yr'])['Settle'].diff())/100)
        
        #open interest change in dollars
        newdata['open int change'] = newdata['OpenInterestChange']
        newdata['dollars'] = newdata['open int change']  
        newdata= newdata.dropna()
        
        newdata.loc[(newdata['open int change']>0) & (newdata['price change']>0), 'increase longs'] = newdata['dollars']
        newdata.loc[(newdata['open int change']<0) & (newdata['price change']<0), 'decrease longs'] = newdata['dollars']
        newdata.loc[(newdata['open int change']>0) & (newdata['price change']<0), 'increase shorts'] = newdata['dollars']
        newdata.loc[(newdata['open int change']<0) & (newdata['price change']>0), 'decrease shorts'] = newdata['dollars']
        
        newdata = newdata.set_index(['MarketDate', 'contract_com', 'yr'])
        newdata = newdata.fillna(0).astype('float')
        newdata = newdata.reset_index()
        newdata['MarketDate'] = pd.to_datetime(newdata['MarketDate']).dt.date
        
        newdata['net long'] = newdata['decrease longs'] + newdata['increase longs']
        newdata ['net short'] = newdata['decrease shorts'] + newdata['increase shorts']
        newdata = newdata.dropna()
        
        newdata = newdata.rename(columns = {'MarketDate':'Start Date', 'OpenInterestClose':'open_interest', 'Settle':'Price'})
        newdata['new daily contracts'] = newdata['price change'] * newdata['open_interest']
        newdata['ctyr'] = newdata['contract_com'] + newdata['yr']
        
        #ls = 'net short'
        #q = 'Brent Last Day FinancialG23'
        
        def netPnL(ls):
            finaldf = pd.DataFrame()
        
            for q in info2:
                alldata2 = sqldata[['Start Date', 'date_window', ls, 'Price', 'contract_com', 'yr', 'ctyr']]
                alldata2 = alldata2.loc[alldata2['ctyr'] == q]
                alldata2 = alldata2.reset_index().drop(columns = 'index')
                alldata2 = alldata2.sort_values(by = 'date_window', axis = 0)
                alldata = alldata2.head(30)
                alldata = alldata.iloc[1:]
                
                lastrow = newdata.loc[newdata['ctyr'] == q].reset_index().drop(columns = 'index')
                if lastrow.empty:
                    pass
                
                else:
                    lastrow = lastrow.loc[:0]
                    lastrow['date_window'] = lastrow['Start Date']
                    lastrow = lastrow[['Start Date', 'date_window', ls, 'Price', 'contract_com', 'yr', 'ctyr']]
                                
                    alldata = alldata.append(lastrow)
                    alldata = alldata.reset_index().drop(columns = 'index')
                    
                    newseries = alldata.copy()
                    newdf = pd.DataFrame()
                    adjusted_vals = pd.DataFrame()
                    newseries_all = pd.DataFrame()
                    sumval = [newseries[ls].sum()]
                    
                    dates = lastrow['Start Date'].tolist()
                    df = pd.DataFrame()
                    df['startdate'] = dates
                    df['sumval'] = sumval
                    df['price'] = lastrow['Price']
                    df['adjusted price'] = df.sumval * df['price']
                    df['original contract #'] = lastrow[ls]
                    
                    for x in lastrow[ls]:
                        if x<0:
                            if abs(x) > abs(sumval[0]):
                                adjusted = pd.DataFrame(np.zeros((1,1)))
                                adjusted = adjusted.rename(columns = {0:ls})
                                adjusted = adjusted.reset_index()
                                adjusted_vals = adjusted_vals.append(adjusted)
                                newseries[ls] = adjusted
                                df['adjusted price'] = 0
                            else:
                                newindex = newseries.index[newseries[ls] == x].tolist()
                                proportion = x/sumval[0]
                                alldata.loc[newindex[0], ls] = 0
                                newseries.loc[newindex[0], ls] = 0
                                adjusted = (1+proportion) * newseries[ls]
                                newseries[ls] = adjusted
                                adjusted = adjusted.reset_index()
                                adjusted_vals = adjusted_vals.append(adjusted)
                        
                    newseries['adjusted price'] = newseries[ls] * newseries['Price']
                    realdate = lastrow['Start Date']
                    realdate = realdate[0]
                    newseries['actual date'] = realdate
                    adjpricesum = newseries['adjusted price'].sum()
                    newsumval = [newseries[ls].sum()]
                    newseries_all = newseries_all.append(newseries)
                    df['adjpricesum'] = adjpricesum
                    df['newsumval'] = newsumval
            #        df['weighted avg price'] = adjpricesum/newsumval 
                    cont_com = lastrow['contract_com'].reset_index().drop(columns = 'index')
                    year = lastrow['yr'].reset_index().drop(columns = 'index')
                    df['contract_com'] = cont_com['contract_com']
                    df['yr'] = year['yr']
                    newdf = newdf.append(df)
                                           
                    newseries_all = newseries_all.drop(columns = 'Start Date')        
                    newseries_all = newseries_all.rename(columns = {'actual date':'Start Date', 'adjusted price':'adjusted_price'})
                    newseries_all = newseries_all.fillna(0)
                    newseries_all = newseries_all[['Price', 'Start Date', 'date_window', ls, 'contract_com', 'yr']]
                
                    orig = newdata.copy()
                    orig = orig.loc[orig['ctyr'] == q]
                    orig = orig.rename(columns = {ls: 'orig_ct_num'})
                    orig = orig[['Start Date', 'yr', 'contract_com', 'Price', 'orig_ct_num']]
                    newseries_all2 = newseries_all.merge(orig, how = 'left', left_on = ['Start Date', 'Price', 'contract_com', 'yr'], right_on = ['Start Date', 'Price', 'contract_com', 'yr'])
                    
                    orig2 = sqldata.copy()
                    orig2 = orig2.loc[orig2['ctyr'] == q]
                    orig2 = orig2.reset_index().drop(columns = 'index')
                    orig2 = orig2.loc[orig2['Start Date'] == lastDate]
                    orig2 = orig2.rename(columns = {'orig_ct_num_short':'orig_ct_num_net short', 'orig_ct_num_long':'orig_ct_num_net long'})
                    orig2['orig_ct_num'] = orig2['orig_ct_num_'+ls]
                    orig2 = orig2[['date_window', 'yr', 'contract_com', 'Price', 'orig_ct_num']]
                    newseries_all3 = newseries_all.merge(orig2, how = 'left', left_on = ['date_window', 'Price', 'contract_com', 'yr'], right_on = ['date_window', 'Price', 'contract_com', 'yr'])
                    newseries_all3 = newseries_all3.iloc[:-1]
                    newseries_all3 = newseries_all3.append(newseries_all2.iloc[-1:])
                    
                    newseries_all3['reducedpositions'] = newseries_all3['orig_ct_num'] - newseries_all3[ls]
                    newseries_all3.loc[newseries_all3[ls] == 0, 'reducedpositions'] = 0
                    finaldf = finaldf.append(newseries_all3)
                        
            return finaldf
        
        def nsl():
            netshort = netPnL(ls = 'net short').fillna(0)
            netlong = netPnL(ls = 'net long').fillna(0)
            netshort = netshort.rename(columns = {'orig_ct_num':'orig_ct_num_short', 'net short':'adj_net_short', 'reducedpositions':'reduced_netshort'})
            netlong = netlong.rename(columns = {'orig_ct_num':'orig_ct_num_long', 'net long':'adj_net_long', 'reducedpositions':'reduced_netlong'})
            
            netshortlong = netshort.merge(netlong, how = 'left', left_on = ['Price', 'Start Date', 'date_window', 'contract_com', 'yr'], right_on = ['Price', 'Start Date', 'date_window', 'contract_com', 'yr'])
            netshortlong = netshortlong.fillna(0)
            netshortlong = netshortlong.rename(columns = {'Price':'price', 'Start Date':'actual_date'})
            netshortlong = netshortlong[['price', 'actual_date', 'date_window', 'orig_ct_num_long', 'reduced_netlong', 'adj_net_long', 'orig_ct_num_short', 'reduced_netshort', 'adj_net_short', 'contract_com', 'yr']]
            netshortlong = netshortlong.loc[netshortlong['price'] != 0]
            
            lastdate = pd.read_sql('SELECT max(actual_date) FROM RiskPOC.dbo.newshortlong2',conn)
            lastdate = lastdate['']
            lastdate = lastdate[0]
            lastdate = datetime.strptime(lastdate,'%Y-%m-%d')
            lastdate = datetime.date(lastdate)
            
            netshortlong2 = netshortlong.loc[(netshortlong['actual_date'] > lastdate)]
            
            return netshortlong2
        
        netshortlong = nsl()
        
        if not netshortlong.empty:
            netshortlong2 = netshortlong2.append(netshortlong)
            db.dataFrameToSQL('RiskPOC', netshortlong, 'newshortlong2', "//tedfil01/DataDropDEV/PythonPOC/Upload_CSVs/newshortlong2.csv", True)
        else:
            break

except Exception as e:
    print(str(e))
        