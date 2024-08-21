import boto3
import json
import pandas as pd
import ast

import numpy as np

from priceElasticityModel import createModel, predictDemand
from getData import getBase, getYearList
from secret_manager import get_access_key, get_secret_key

def getOptimizedPrice(revenueList, stockCost):
    if revenueList == []:
        return 'Empty List'
    
    profitList = [tuple_item for tuple_item in revenueList if tuple_item[-1] == 'PROFIT']
    lossList = [tuple_item for tuple_item in revenueList if tuple_item[-1] == 'LOSS']
    
    # print(profitList)
    # print(lossList)
    
    if profitList != []:
        maxRevTuple = max(profitList, key=lambda x: x[3])
    else:
        maxRevTuple = max(lossList, key=lambda x: x[3])
    
    discount = maxRevTuple[0]
    optimizedPrice = maxRevTuple[1]
    totalSold = maxRevTuple[2]
    totalRevenue = maxRevTuple[3]
    
    profitLoss = maxRevTuple[4]
    
    if profitLoss == 'LOSS':
        revenueNeeded = stockCost - totalRevenue
        
        revPerDay = totalRevenue/7
        
        dayNeeded = np.ceil(revenueNeeded/revPerDay)
        totalDay = 7 + dayNeeded
        # print(totalSold/7*totalDay)
    elif profitLoss == 'PROFIT':
        totalDay = 7
    
    return discount, optimizedPrice, totalSold, totalRevenue, profitLoss, totalDay

def calculateRevenue(demandDF, priceDF, year: int, store_id: str, item_id: str, event: bool, snap: bool, eventCount: int, snapCount: int):
    if (year-1) not in getYearList(priceDF, store_id, item_id, event, snap):
        return "Year Invalid"
    
    if event == False and eventCount != 0:
        return 'Invalid Data'
    elif event == True and eventCount == 0:
        return 'Invalid Data'
    
    if snap == False and snapCount != 0:
        return 'Invalid Data'
    elif snap == True and snapCount == 0:
        return 'Invalid Data'
    
    poly, model, _, _, _, _, _, _ = createModel(priceDF, year, store_id, item_id, event, snap, eventCount, snapCount)
    
    base_price, base_demand = getBase(demandDF, priceDF, year-1, store_id, item_id, event, snap)
    
    # Adjust base_demand that is below 0
    # Make sure that we can actually calculate the optimized price
    if base_demand < 1:
        base_demand = 1
    
    dummyData = getDummyData(year-1)
    costPrice, stockOnHand = getCostStock(dummyData, store_id, item_id)
    
    stockCost = stockOnHand * costPrice
    
    # print(base_price, base_demand, costPrice, stockOnHand, stockCost)
    
    revenueList = []
    discount_range = list(range(1, 100))
    for discount in discount_range:
        changeDemand, impact, demand, demandText = predictDemand(poly, model, base_demand, discount, eventCount, snapCount)
        
        sell_price = round(base_price - (base_price*discount/100), 2)
        # Round down the demand
        rounded_demand = int(np.floor(demand))
        
        revenue = round(sell_price * rounded_demand, 3)
        
        if revenue > stockCost:
            profitLoss = 'PROFIT'
        else: 
            profitLoss = 'LOSS'
        
        if rounded_demand <= stockOnHand and rounded_demand > 0:
            revenueList.append((discount, sell_price, rounded_demand, revenue, profitLoss))    

    return costPrice, stockOnHand, revenueList, stockCost

def getDummyData(year: int):
    filePath = f"dummyData/data_{year}.csv"
    
    # Get access key
    access_key = get_access_key()
    secret_key = get_secret_key()

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
        
    data = pd.read_csv(s3.Bucket(name='fit3164bucket').Object(filePath).get()['Body'])
    
    return data

def getCostStock(dummyData, store_id: str, item_id: str):
    filteredData = dummyData[(dummyData['store_id'] == store_id) & (dummyData['item_id'] == item_id)].reset_index(drop=True)
    
    if len(filteredData) == 0:
        return f'Dummy data not found for {item_id} in store {store_id}'
    elif len(filteredData) > 1:
        return f'ERROR: Multiple dummy data found'
    
    return filteredData['costPrice'][0], filteredData['stockOnHand'][0]

if __name__ == '__main__':
    # Get access key
    access_key = get_access_key()
    secret_key = get_secret_key

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    demandDF = pd.read_csv(s3.Bucket(name='fit3164bucket').Object('demand.csv').get()['Body'])
    
    demandDF['withoutBoth'] = demandDF['withoutBoth'].apply(ast.literal_eval)
    demandDF['withBoth'] = demandDF['withBoth'].apply(ast.literal_eval)
    demandDF['onlyEvent'] = demandDF['onlyEvent'].apply(ast.literal_eval)
    demandDF['onlySNAP'] = demandDF['onlySNAP'].apply(ast.literal_eval)
    
    demandDF['start_date'] = pd.to_datetime(demandDF['start_date'])
    demandDF['end_date'] = pd.to_datetime(demandDF['end_date'])
    
    priceDF = pd.read_csv(s3.Bucket(name='fit3164bucket').Object('price.csv').get()['Body'])
    
    priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
    priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
    priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
    priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    year = 2014
    store_id = "TX_1"
    item_id = "FOODS_1_040"
    
    costPrice, stockOnHand, revenueList, stockCost = calculateRevenue(demandDF, priceDF, year, store_id, item_id, event=False, snap=True, eventCount=0, snapCount=3)
    
    getOptimizedPrice(revenueList, stockCost)
    discount, optimizedPrice, totalSold, totalRevenue, profitLoss, totalDay = getOptimizedPrice(revenueList, stockCost)