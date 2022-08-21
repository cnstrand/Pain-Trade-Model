# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 11:01:42 2022

@author: ZCS0349
"""

import pandas as pd
import pyodbc
import datetime as dt
from datetime import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
import time
from selenium.webdriver.common.keys import Keys
from inspect import currentframe, getframeinfo
frameinfo = getframeinfo(currentframe())
#frameinf = getframeinfo()


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


    productlibrary = [['http://www.cmegroup.com/trading/energy/ethanol/chicago-ethanol-platts-swap_quotes_volume_voi.html', 'Ethanol', 'Platts Swaps'],
                      ['http://www.cmegroup.com/trading/energy/crude-oil/light-sweet-crude_quotes_volume_voi.html', 'WTI Crude', 'Futures'],
                      ['http://www.cmegroup.com/trading/energy/crude-oil/brent-crude-oil-last-day_quotes_volume_voi.html', 'Brent Crude', 'Futures'],
                      ['http://www.cmegroup.com/trading/energy/natural-gas/natural-gas_quotes_volume_voi.html', 'Nat Gas', 'Futures'],
                      ['http://www.cmegroup.com/trading/energy/refined-products/rbob-gasoline_quotes_volume_voi.html', 'RBOB', 'Futures'],
                      ['http://www.cmegroup.com/trading/energy/refined-products/heating-oil_quotes_volume_voi.html', 'Heating Oil', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/livestock/live-cattle_quotes_volume_voi.html', 'Live Cattle', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/livestock/feeder-cattle_quotes_volume_voi.html', 'Feeder Cattle', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/livestock/lean-hogs_quotes_volume_voi.html', 'Lean Hog', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/corn_quotes_volume_voi.html', 'Corn', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/wheat_quotes_volume_voi.html', 'Chicago SRW Wheat', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/kc-wheat_quotes_volume_voi.html', 'KC HRW Wheat', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/soybean_quotes_volume_voi.html', 'Soybean', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/soybean-oil_quotes_volume_voi.html', 'Soybean Oil', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/soybean-meal_quotes_volume_voi.html', 'Soybean Meal', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/rough-rice_quotes_volume_voi.html', 'Rough Rice', 'Futures'],
                      ['http://www.cmegroup.com/trading/agricultural/grain-and-oilseed/oats_quotes_volume_voi.html', 'Oat', 'Futures']]


try:
    allproducts = pd.DataFrame()
    commodities2 = []
    product = productlibrary[0]
    z=0
  
    def loop(z):
        selectdate2 = datedropdown[2].click()
        time.sleep(1)
        dateselection = driver.find_elements_by_class_name('universal-dropdown.dropup.has-custom-scroll.dropdown.show')
        options = driver.find_elements_by_class_name('dropdown-item')
    
        SelectDate = []
        for o in options:
            if 'day' in o.text:
                print(o.text)
                SelectDate.append(o)
        
        options1 = SelectDate[z].click()
        time.sleep(3)
        selectdate = dateselection[0].text
        time.sleep(2)
        selectdate = dt.datetime.strptime(selectdate, '%A %d %b %Y')
        selectdate = dt.datetime.strftime(selectdate, '%Y-%m-%d')
        driver.execute_script("window.scrollTo(0, 1800)") 
    
        link = None
        while not link:
            try:
                driver.find_element_by_class_name('primary.load-all.btn.btn-').click()
                time.sleep(5)
                break
            except:
                pass
                break
    
        rows = driver.find_elements_by_class_name('row')
        time.sleep(3)
        rows8 = rows[8].text
        df = pd.DataFrame()
        text = rows8.rstrip().split(' ')
        for i in range(len(text)):
            df[i] = [text[i]]
        
        tas = df[38].str.contains("TAS")==True
        if not df[tas].empty:
            df = df.T
            df = df[38:]
        else:
            df = df.T
            df = df[19:]
           
        df = df.reset_index().drop(columns = 'index')        
        month = df[df.index % 13 == 0]  
        year = df[df.index % 13 == 1]
        month, year = month.reset_index(), year.reset_index()
        month, year = month.iloc[:-1], year.iloc[:-1]
        imonth = month['index'].tolist()
        iyear = year['index'].tolist()
        
        df = df[df[0].str.contains("to")==False]
        df = df.T
        
        for x, y in zip(imonth, iyear):
            df[y] = df[x].str[-3:] + ' ' + df[y]
            df[x] = df[x].str[:-3]
             
        df1 = df.T
        df1 = df1[1:-2]
        length = len(df1)
        df1 = df1.T
        df1[length] = df1[length].str[:-6]
        df1 = df1.T
        
        df1 = df1.reset_index().drop(columns = 'index')
        values = df1[df1.index %13 != 0]    
        
        monthyear = df1[df1.index %13 == 0]
        monthyear = monthyear.reset_index().drop(columns = 'index')
        monthyear = monthyear.dropna()
        
        col1 = monthyear[0]
        c1 = col1[:len(monthyear)]
        c1 = c1.drop(columns = 'index')
        
        values = values.rename(columns = {0:'col1'}).reset_index().drop(columns = 'index')
        values2 = values.copy()
        newdf = pd.DataFrame()
        newdf2 = pd.DataFrame()
        newdf3 = pd.DataFrame()
          
        for n in range(len(c1)):
             newseries = values2.head(12)
             n = str(n)
             newdf[n] = newseries['col1']
             newseries = newseries.reset_index()
             values2 = values2.drop(newseries['index'])
            # newdf2[n] = newdf[n]
             newseries = newseries.drop(columns = 'index')
             values2 = values2.reset_index()
             values2 = values2.drop(columns = 'index')
        
        newdf2 = newdf.T
        newdf2 = newdf2.rename(columns = {0:'Globex', 1:'Open Outcry', 2:'Pnt Clearport', 3:'Total Volume', 4:'Block Trades', 5:'EFP', 6:'EFR', 7:'EFS', 8:'TAS', 9:'Deliveries', 10:'Open Interest at Close', 11:'Open Interest Change'})
        trial = newdf2[newdf2['Open Interest Change'].str.contains("About")==True]
        
        if not trial.empty:
            val= newdf2.loc[len(newdf2)-1,'Open Interest Change'] 
            val = float(val[:-3])
            newdf2.loc[len(newdf2)-1, 'Open Interest Change'] = val
        else:
            pass
        
        monthyear = monthyear.reset_index()
        monthyear = monthyear.dropna()
        newdf2 = newdf2.reset_index().drop(columns = 'index').reset_index()
        newdf3 = newdf2.merge(monthyear, how = 'left', left_on = 'index', right_on = 'index').drop(columns = 'index')
        newdf3 = newdf3.rename(columns = {0:'Month'})
        newdf3['Date'] = selectdate
        
        newdf3 = newdf3[['Date', 'Month', 'Globex', 'Open Outcry', 'Pnt Clearport', 'Total Volume', 'Block Trades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'Open Interest at Close', 'Open Interest Change']]
        newdf3 = newdf3.rename(columns = {'Date':'Currdate', 'Month':'Posmonth', 'Open Outcry':'OpenOutcry', 'Pnt Clearport':'PntClearport', 'Total Volume':'TotalVolume', 'Block Trades':'BlockTrades', 'Open Interest at Close':'OpenInterestClose', 'Open Interest Change':'OpenInterestChange'})
        newdf3 = newdf3.replace(',','', regex= True)
        newdf3['OpenInterestChange'] = newdf3['OpenInterestChange'].str.strip()
        newdf3['Commodity'] = comm
        driver.execute_script("window.scrollTo(0, 800)") 

        return newdf3
 
except Exception as e:
    print(str(e))

try:   
    for product in productlibrary:
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.get(product[0])
        driver.fullscreen_window()
        acceptcookies = driver.find_element_by_id('onetrust-accept-btn-handler').click()
        
        datedropdown = driver.find_elements_by_class_name('button-text')
        comm = driver.find_elements_by_class_name('title')
        comm = comm[0].text
        commodities2.append(comm)
        driver.execute_script("window.scrollTo(0, 800)") 

        day1 = loop(z = 0)
        time.sleep(5)
        day2 = loop(z = 1)
        time.sleep(5)
        day3 = loop(z = 2)
        time.sleep(5)
        day4 = loop (z = 3)
        time.sleep(5)
        day5 = loop (z = 4)
        time.sleep(5)
        totalinfo = pd.concat([day1, day2, day3, day4, day5])
         
        allproducts = allproducts.append(totalinfo)
        
        driver.quit()
        
except Exception as e:
    print(str(e))
 
try:
    allproducts = allproducts.reset_index().drop(columns = 'index')
    commodities = allproducts['Commodity'].unique().tolist()
            
    allproducts['Currdate'] = pd.to_datetime(allproducts['Currdate']).dt.date
    allproducts['Posmonth'] = allproducts['Posmonth'].astype(str)

    allproducts['month'] = allproducts['Posmonth'].str[:3]
    allproducts['year'] = allproducts['Posmonth'].str[-2:]
    allproducts['month'] = allproducts['month'].str.replace(' ', '')
    datedict = {'JAN':'F', 'FEB':'G', 'MAR':'H', 'APR':'J', 'MAY':'K', 'JUN':'M', 'JUL':'N', 'AUG':'Q', 'SEP':'U', 'OCT':'V', 'NOV':'X', 'DEC':'Z'}
    allproducts = allproducts.replace({"month":datedict})
    allproducts['Posmonth'] = allproducts['month'] + allproducts['year']
    
    allproducts['OpenInterestChange'] = allproducts['OpenInterestChange'].replace('−', '-')
    
    allproducts.to_csv('//tedfil01/DataDropDEV/PythonPOC/Download_CSVs/allproductsdumpCS.csv')
    allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']] = allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']].astype(float)
    allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']] = allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']].fillna(0.0)
    allproducts.info()
    
    allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']] = allproducts[['Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange']].astype(int)
    allproducts = allproducts[['Currdate', 'Posmonth', 'Globex', 'OpenOutcry', 'PntClearport', 'TotalVolume', 'BlockTrades', 'EFP', 'EFR', 'EFS', 'TAS', 'Deliveries', 'OpenInterestClose', 'OpenInterestChange', 'Commodity']]

except Exception as e:
    print(str(e))
   

try: 

    for c in commodities2:
        minDate_frame = allproducts.loc[allproducts['Commodity'] == c]
        minDate = minDate_frame.Currdate.min().strftime('%Y-%m-%d')
        executeSQL('RiskPOC', "delete from cmePrelimData where Currdate >= '" + minDate + "' and Commodity = '" + c + "'")
        
    dataFrameToSQL('RiskPOC', allproducts, 'cmePrelimData', "//tedfil01/DataDropDEV/PythonPOC/Upload_CSVs/cmePrelimData.csv", True)
    
except Exception as e:
    print(str(e))
        

