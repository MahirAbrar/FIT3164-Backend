import boto3
import json
import pandas as pd
import ast
import numpy as np

from tqdm.auto import tqdm
tqdm.pandas()

import sys
sys.path.append('../')
from getData import getBase
from secret_manager import get_access_key, get_secret_key

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def addModelData(data, trueFalse: bool):
    
    data['priceDiff'] = data['basePrice'] - data['sell_price']
    data['priceDiscount'] = round(((data['basePrice'] - data['sell_price']) / data['basePrice']) * 100, 2)
    
    data['demandChange'] = data['demand'] - data['baseDemand'] 
    
    if trueFalse == True:
        data = data[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'eventBool', 'snapBool', 'basePrice', 'sell_price', 'priceDiff', 'priceDiscount', 
                     'baseDemand', 'demand', 'demandChange']]
    elif trueFalse == False:
        data = data[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'eventCount', 'snapCount', 'basePrice', 'sell_price', 'priceDiff', 'priceDiscount', 
                     'baseDemand', 'demand', 'demandChange']]
    
    return data

def addDemandPercent(row):
    if row['baseDemand'] == 0:
        if row['demandChange'] != 0:
            return 100
        else:
            return 0
    else:
        return round((row['demandChange'] / row['baseDemand']) * 100, 2)

def addEventSNAPCount(data, calendar, year: int):

    filteredCalendar = calendar[calendar['start_date'].dt.year == year].reset_index(drop=True)
    newData = pd.merge(data, filteredCalendar[['wm_yr_wk', 'start_d', 'end_d', 'event_num', 'event_type']], on='wm_yr_wk', how='left')
    newData.rename(columns={'event_num': 'eventCount', 'event_type': 'eventType'}, inplace=True)
    print(newData.shape)
    
    data_CA = newData[newData['state_id'] == 'CA']
    data_TX = newData[newData['state_id'] == 'TX']
    data_WI = newData[newData['state_id'] == 'WI']
    
    data_CA = pd.merge(data_CA, filteredCalendar[['wm_yr_wk', 'snap_CA']], on='wm_yr_wk', how='left')
    data_CA.rename(columns={'snap_CA':'snapCount'}, inplace=True)
    data_TX = pd.merge(data_TX, filteredCalendar[['wm_yr_wk', 'snap_TX']], on='wm_yr_wk', how='left')
    data_TX.rename(columns={'snap_TX':'snapCount'}, inplace=True)
    data_WI = pd.merge(data_WI, filteredCalendar[['wm_yr_wk', 'snap_WI']], on='wm_yr_wk', how='left')
    data_WI.rename(columns={'snap_WI':'snapCount'}, inplace=True)
    
    newData = pd.concat([data_CA, data_TX, data_WI])
    
    newData['eventBool'] = newData['eventCount'].apply(lambda x: 1 if x != 0 else 0)
    newData['snapBool'] = newData['snapCount'].apply(lambda x: 1 if x != 0 else 0)
    
    
    return newData

def generateDemand(row, sales_train):
    key = row['item_id'] + '_' + row['store_id']
    filtered_sales = sales_train[sales_train['id'] == key]
    
    start_d = row['start_d']
    end_d = row['end_d']
    
    totalSold = np.sum(filtered_sales.loc[:, start_d:end_d].values)
    return totalSold

def groupData(data, trueFalse: bool):
    if trueFalse == True:
        newData = data.groupby(['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'sell_price', 'eventBool', 'snapBool']).agg({'demand': 'mean', 'totalDay': 'sum'}).reset_index()
    elif trueFalse == False:
        newData = data.groupby(['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'sell_price', 'eventCount', 'snapCount']).agg({'demand': 'sum', 'totalDay': 'sum'}).reset_index()
        
    return newData

def addBase(row, demandDF, priceDF, year:int, trueFalse: bool):
    if trueFalse:
        eventCondition = row['eventBool'] == 1
        snapCondition = row['snapBool'] == 1
    else:
        eventCondition = row['eventCount'] != 0
        snapCondition = row['snapCount'] != 0
        
    # print(row['store_id'], row['item_id'], eventCondition, snapCondition)
    
    basePrice, baseDemand = getBase(demandDF, priceDF, year, row['store_id'], row['item_id'], eventCondition, snapCondition)
    
    return pd.Series({'basePrice': basePrice, 'baseDemand': baseDemand})

def getDataframe(calendar, demandDF, year: int):
    filteredCalendar = calendar[calendar['start_date'].dt.year == year].reset_index(drop=True)
    filteredDemand = demandDF[(demandDF['start_date'].dt.year <= year) & (demandDF['end_date'].dt.year >= year)].reset_index(drop=True)
    
    expandedRows = filteredDemand.progress_apply(expand_rows, args=(filteredCalendar, year,), axis=1).tolist()
    newDF = pd.concat(expandedRows, ignore_index=True)
    return newDF

def expand_rows(row, filteredCalendar, year):
    weeks = getWeeks(row, year)
    
    rows = []
    for week in weeks:
        new_row = row[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'sell_price']].copy()
        new_row['start_date'] = week
        if week.weekday() == 4:
            new_row['end_date'] = week.date()
        elif week == '2016-05-21':  
            new_row['end_date'] = (week + pd.DateOffset(days=1)).date()
        else:
            endWeek = week + pd.offsets.Week(weekday=4)
            if endWeek.year == year:
                new_row['end_date'] = endWeek.date()
            else:
                new_row['end_date'] = (pd.Timestamp(year + 1, 1, 1) - pd.Timedelta(days=1)).date()
        new_row['wm_yr_wk'] = filteredCalendar[filteredCalendar['start_date'] == week]['wm_yr_wk'].reset_index(drop=True)[0]
        new_row = new_row[['wm_yr_wk', 'start_date', 'end_date', 'state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'sell_price']]
        rows.append(new_row)
    return pd.DataFrame(rows)

def getWeeks(row, year):
    start_date = row['start_date'].date()
    end_date = row['end_date'].date()
    
    if start_date.year == end_date.year:
        weeks = pd.date_range(start=start_date, end=end_date, freq='W-SAT')
    elif start_date.year == year:
        weeks = pd.date_range(start=start_date, end=(pd.Timestamp(year + 1, 1, 1) - pd.Timedelta(days=1)).date(), freq='W-SAT')
    elif end_date.year == year:
        weeks = pd.date_range(start=pd.Timestamp(year, 1, 1).date(), end=end_date, freq='W-SAT')
        if len(weeks) == 0:
            weeks = weeks.insert(0, pd.Timestamp(year, 1, 1))
        elif weeks[0].month == 1 and weeks[0].day != 1:
            # If not, add it to the beginning of the list
            weeks = weeks.insert(0, pd.Timestamp(year, 1, 1))
    else:
        weeks = pd.date_range(start=pd.Timestamp(year, 1, 1).date(), end=(pd.Timestamp(year + 1, 1, 1) - pd.Timedelta(days=1)).date(), freq='W-SAT')
        if weeks[0].month == 1 and weeks[0].day != 1:
            # If not, add it to the beginning of the list
            weeks = weeks.insert(0, pd.Timestamp(year, 1, 1))
            
    return weeks
        
if __name__ == '__main__':
    # Get access key
    # access_key = get_access_key()
    # secret_key = get_secret_key()

    # s3 = boto3.resource(
    #     service_name='s3',
    #     region_name='ap-southeast-2',
    #     aws_access_key_id=access_key,
    #     aws_secret_access_key=secret_key
    # )
    
    # demandDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('demand.csv').get()['Body'])
    
    # demandDF['withoutBoth'] = demandDF['withoutBoth'].apply(ast.literal_eval)
    # demandDF['withBoth'] = demandDF['withBoth'].apply(ast.literal_eval)
    # demandDF['onlyEvent'] = demandDF['onlyEvent'].apply(ast.literal_eval)
    # demandDF['onlySNAP'] = demandDF['onlySNAP'].apply(ast.literal_eval)
    
    # demandDF['start_date'] = pd.to_datetime(demandDF['start_date'])
    # demandDF['end_date'] = pd.to_datetime(demandDF['end_date'])
    
    # priceDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('price.csv').get()['Body'])
    
    # priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
    # priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
    # priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
    # priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
    # priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    # calendar = pd.read_csv("newCalendar.csv")
    # calendar['start_date'] = pd.to_datetime(calendar['start_date'])
    # calendar['end_date'] = pd.to_datetime(calendar['end_date'])
    
    # calendar['event_type'] = calendar['event_type'].apply(ast.literal_eval)
    
    """
    Data Structure
    """
    # for year in range(2011, 2017):
    #     df_year = getDataframe(calendar, demandDF, year)
    #     df_year.to_csv(f"../dataStructure/data_{year}.csv", index=False)
    
    """
    Event/SNAP Count and Generate Demand
    """
    # sales_train = pd.read_csv("../dataset/sales_train_evaluation.csv")
    # sales_train['id'] = sales_train['id'].str.replace('_evaluation', '') # Remove evaluation in 'id' column
    
    # for year in range(2011, 2017):
    #     data = pd.read_csv(f"../dataStructure/data_{year}.csv")
    #     newData = addEventSNAPCount(data, calendar, year)
    #     newData['demand'] = newData.progress_apply(generateDemand, args=(sales_train,), axis=1)
    #     newData.to_csv(f"../rawModelData/data_{year}.csv", index=False)
    
    """
    Group Data
    """
    # for year in range(2011, 2017):
    #     data = pd.read_csv(f"../rawModelData/data_{year}.csv")
    #     data['start_date'] = pd.to_datetime(data['start_date'])
    #     data['end_date'] = pd.to_datetime(data['end_date'])
        
    #     data['totalDay'] = (data['end_date'] - data['start_date']).dt.days + 1

    #     newData = groupData(data, True)
    #     newData['demand'] = round(newData['demand']/(newData['totalDay']/7),3)
    #     newData.drop(columns='totalDay')
    #     newData.to_csv(f"../baseModelData/trueFalse/data_{year}.csv", index=False)
        
    #     newData2 = groupData(data, False)
    #     newData2['demand'] = round(newData2['demand']/(newData2['totalDay']/7),3)
    #     newData2.drop(columns='totalDay')
    #     newData2.to_csv(f"../baseModelData/data_{year}.csv", index=False)
    
    
    """
    Add Base Price and Base Demand
    """
    # for year in range(2011, 2017):
    #     print(year)
    #     data = pd.read_csv(f"../baseModelData/data_{year}.csv")
    #     data.loc[:, ['basePrice', 'baseDemand']] = data.progress_apply(addBase, args=(demandDF, priceDF, year, False,), axis=1)
    #     data.to_csv(f"../baseModelData/data_{year}.csv", index=False)
        
    # for year in range(2011, 2017):
    #     print(year)
    #     dataTrue = pd.read_csv(f"../baseModelData/trueFalse/data_{year}.csv")
    #     dataTrue.loc[:, ['basePrice', 'baseDemand']] = dataTrue.progress_apply(addBase, args=(demandDF, priceDF, year, True,), axis=1)
    #     dataTrue.to_csv(f"../baseModelData/trueFalse/data_{year}.csv", index=False)
    
    """
    Add Model Data and Demand Percent
    """
    # for year in range(2011, 2017):
    #     data = pd.read_csv(f"../baseModelData/data_{year}.csv")
    #     data = addModelData(data, False)
    #     print((data['priceDiscount'] >= 0).all())
        
    #     data['demandPercent'] = data.progress_apply(addDemandPercent, axis=1)
    #     data.to_csv(f"../modelData/data_{year}.csv", index=False)
        
    #     dataTrue = pd.read_csv(f"../baseModelData/trueFalse/data_{year}.csv")
    #     dataTrue = addModelData(dataTrue, True)
    #     print((dataTrue['priceDiscount'] >= 0).all())
        
    #     dataTrue['demandPercent'] = dataTrue.progress_apply(addDemandPercent, axis=1)
    #     dataTrue.to_csv(f"trueFalse/data_{year}.csv", index=False)