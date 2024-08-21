import boto3
import json
import pandas as pd
import ast

import numpy as np
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from secret_manager import get_access_key, get_secret_key
from selectLevel import levelSelection, getProductInfo
from getData import getBase, getYearList
from priceElasticityModel import createModel

def testModel(priceDF, data, year: int):
    results = []
    
    for store_id in data['store_id'].unique():
        storeData = data[data['store_id'] == store_id]
        itemData = storeData['item_id'].unique()
        uniqueId = len(itemData)
        randomIndex = np.random.randint(uniqueId, size=4)
        for index in randomIndex:
            item_id = itemData[index]
            for event in [True, False]:
                for snap in [True, False]:
                    print(f"Processing: Year {year}, Store {store_id}, Item {item_id}, Event {event}, Snap {snap}")
                    if event == True:
                        eventCount = 1
                    else:
                        eventCount = 0
                        
                    if snap == True:
                        snapCount = 1
                    else:
                        snapCount = 0
                        
                    result = createModel(priceDF, year+1, store_id, item_id, event, snap, eventCount, snapCount)
                    # print(result)
                    if result == 'Year Invalid':
                        break
                    else:
                        results.append({'store_id': store_id, 'item_id': item_id, 'event': event, 'snap': snap, 'RMSE': result[2], 'score': result[3]})
    
    results = pd.DataFrame(results)
    
    return results

if __name__ == '__main__':
     # Get access key
    access_key = get_access_key()
    secret_key = get_secret_key()

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    priceDF = pd.read_csv(s3.Bucket(name='fit3164bucket').Object('price.csv').get()['Body'])
    
    priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
    priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
    priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
    priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    for year in range(2011, 2017):
        filePath = f"modelData/data_{year}.csv"
        data = pd.read_csv(s3.Bucket(name='fit3164bucket').Object(filePath).get()['Body'])
        
        testData = testModel(priceDF, data, year)
        testData.to_csv(f"accuracyTestData/data_{year}.csv", index=False)