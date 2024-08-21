import boto3
import json
import pandas as pd
import ast

import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from selectLevel import levelSelection, getProductInfo
from getData import getBase, getYearList

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def createModel(priceDF, year: int, store_id: str, item_id: str, event: bool, snap: bool, eventCount: int, snapCount: int):
    
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

    # Fetch and filter data from previous year based on event and snap conditions
    data = getDataset(year-1, event, snap)
    
    # Choose the level of the model
    level = levelSelection(priceDF, data, year, store_id, item_id, event, snap)
    
    # Get the productInfo (state_id, store_id, cat_id, dept_id, item_id)
    productInfo = getProductInfo(store_id, item_id)
    
    # Filter the data based on the selected level
    dataModel = filterData(data, productInfo, level)
    dataModel = dataModel.sort_values(by='priceDiscount')
    
    # Split features and target variable
    x = dataModel[['priceDiscount', 'eventCount', 'snapCount']]
    y = dataModel['demandPercent']

    # Split data into training and test sets
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
    
    # Create Polynomial Features
    poly = PolynomialFeatures(degree=3, include_bias=False)
    X_train = poly.fit_transform(x_train)
    
    # Train Linear Regression model
    poly_reg_model = LinearRegression()
    poly_reg_model.fit(X_train, y_train)
    
    # Generate values 0 to 100 by 0.01 for price discount
    x_values = np.arange(0, 100.01, 0.01)
    
    # Prediction
    prediction_data = pd.DataFrame({'priceDiscount': x_values, 'eventCount': eventCount, 'snapCount': snapCount})
    x_values_poly = poly.transform(prediction_data)
    y_predicted = poly_reg_model.predict(x_values_poly)
    
    # Calcualte RMSE
    X_test = poly.transform(x_test)
    y_test_predicted = poly_reg_model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_test_predicted))
    score = poly_reg_model.score(X_test, y_test)
    # print("RMSE:", rmse)
    # print("score:", score)
    
    # plt.figure(figsize=(10, 6))
    # plt.scatter(x['priceDiscount'], y, color='goldenrod',label='Actual Data')
    # plt.plot(x_values, y_predicted, color="red", label='Polynomial Regression')
    # plt.plot(np.arange(0, 101), np.arange(0, 101), color='green', linestyle='--', label='Price Elasticity = 1')
    # plt.title(f"Price Elasticity Analysis for {item_id} in Store {store_id} at {level} Level")
    # plt.xlabel('Price Discount (%)')
    # plt.ylabel('Change in Demand (%)')
    # plt.legend()
    #* Uncomment the next line if you want to work with backend
    #* Comment the next line if you want to work with frontend
    # plt.show()
            
    return poly, poly_reg_model, rmse, score, x['priceDiscount'].values, y.values, x_values, y_predicted

def predictDemand(poly, model, base_demand, discount, eventCount: int, snapCount: int):
    predictionData = pd.DataFrame({'priceDiscount': discount, 'eventCount': eventCount, 'snapCount': snapCount}, index=[0])
    x_predict_poly = poly.transform(predictionData)
    y_predict = model.predict(x_predict_poly)
    
    y_predict[0] = round(y_predict[0], 2)
    
    if y_predict[0] == 0:
        impact = f"No Impact ({y_predict[0]}% change)"
    elif y_predict[0] > 0:
        impact = f"Increased by {y_predict[0]}%"
    elif y_predict[0] < 0:
        impact = f"Decreased by {abs(y_predict[0])}%"
    
    demand = round(base_demand + base_demand * y_predict[0]/100, 2)
    demandText = f"{demand} Items sold per week"
    
    return y_predict[0], impact, demand, demandText

def priceElasticity(discount, demandChange):
    if discount == 0:
        elasticity = float('inf')
    else:
        elasticity = round(demandChange/discount, 2)
    # print("elasticity", elasticity)
        
    if elasticity == float('inf'):
        interpretation = "Perfectly elastic"
    elif elasticity == 0:
        interpretation =  "Perfectly inelastic"
    elif elasticity > 1:
        interpretation =  "Elastic"
    elif elasticity == 1:
        interpretation =  "Unitary elastic"
    elif elasticity < 1:
        interpretation =  "Inelastic"

    
    return elasticity, interpretation

def getDataset(year, event: bool, snap: bool):
    filePath = f"modelData/data_{year}.csv"
    
    # Get access key
    with open('credentials.json') as f:
        credentials = json.load(f)

    access_key = credentials.get('aws_access_key_id')
    secret_key = credentials.get('aws_secret_access_key')

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
        
    data = pd.read_csv(s3.Bucket(name='fit3164bucket').Object(filePath).get()['Body'])
        
    if event == False and snap == False:
        data = data[(data['eventCount'] == 0 ) & (data['snapCount'] == 0)].reset_index(drop=True)
    elif event == True and snap == False:
        data = data[(data['eventCount'] != 0 ) & (data['snapCount'] == 0)].reset_index(drop=True)
    elif event == False and snap == True:
        data = data[(data['eventCount'] == 0 ) & (data['snapCount'] != 0)].reset_index(drop=True)
    elif event == True and snap == True:
        data = data[(data['eventCount'] != 0 ) & (data['snapCount'] != 0)].reset_index(drop=True)
    
    return data

def filterData(data, productInfo, level: str):
    if level in ["item_id", "dept_id", "cat_id"]:
        store_id = productInfo.get("store_id")
        filter_product = productInfo.get(level)
        filter_value = "store_id == " + f"'{store_id}'" + " and " + f"{level} == '{filter_product}'"
    else: 
        filter_product = productInfo.get(level)
        filter_value = f"{level} == '{filter_product}'"
    
    data = data.query(filter_value).reset_index(drop=True)
    
    return data


    
if __name__ == '__main__':
    # Get access key
    credentials = json.load(open('credentials.json'))

    access_key = credentials.get('aws_access_key_id')
    secret_key = credentials.get('aws_secret_access_key')

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
       
    # dept_id
    year = 2014
    store_id = "TX_1"
    item_id = "HOUSEHOLD_2_467"
    
    base_price, base_demand = getBase(demandDF, priceDF, year-1, store_id, item_id, False, False)
    base_price
    base_demand
    
    poly, model, rmse, score, x_priceDiscount, y_actual, x_values, y_predicted= createModel(priceDF, year, store_id, item_id, event=False, snap=False, eventCount=0, snapCount=0)
    plt.show()

    discount = 10
    changeDemand, impact, demand, demandText = predictDemand(poly, model, base_demand, discount, 0, 0)
    impact
    demand

    priceElasticity(discount, changeDemand)