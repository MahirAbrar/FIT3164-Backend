# import dataTransformation
import boto3
import pandas as pd
import ast
from dotenv import load_dotenv
import os
from secret_manager import get_access_key, get_secret_key, get_item, put_item

from flask import Flask, jsonify
from flask_cors import CORS
from flask import request

import getData
import priceElasticityModel
import priceOptimization

app = Flask(__name__)
CORS(app)

load_dotenv()

stores = {
    'st1Cal': 'CA_1',
    'st2Cal': 'CA_2',
    'st3Cal': 'CA_3',
    'st4Cal': 'CA_4',
    'st1Tex': 'TX_1',
    'st2Tex': 'TX_2',
    'st3Tex': 'TX_3',
    'st1Win': 'WI_1',
    'st2Win': 'WI_2',
    'st3Win': 'WI_3',
}

# Get the credentials
access_key = get_access_key()
secret_key = get_secret_key()

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


# will return something
@app.route('/test', methods=['GET'])
def test():
    print("Trying to fetch item")
    trial = get_item('get-price-elasticity?storeId=st1Cal&itemId=FOODS_1_001&yearId=2015&event=True&snap=False&eventCount=1&snapCount=0&disId=10')
    return trial

# will return "not available"
@app.route('/test2', methods=['GET'])
def test2():
    print("Trying to fetch item")
    trial = get_item('asdasd')
    print(trial)
    return trial

@app.route('/', methods=['GET'])
def home():
    return "Welcome to the Price Elasticity API for the M5 Dataset from Walmart!"



# get all items from a store
# run this in the browser: http://127.0.0.1:5000/get-items/st1Cal . This will return all the items in store 1 in California. Make sure the application is running using flask run
@app.route('/get-items/<storeId>', methods=['GET'])
def getItems(storeId):
    storeId = stores.get(storeId, "Could not find store")

    # only show the unique items in the store
    items = demandDF[demandDF['store_id'] == storeId]['item_id'].unique()
    return jsonify(items.tolist())  # Convert items to a list

# Input argument, storeId and itemId.
# Outputs all the years that item was sold in that store
# run this in the browser: http://127.0.0.1:5000/get-year?storeId=st1Cal&itemId=FOODS_1_001&event=True&snap=False
@app.route('/get-year', methods=['GET'])
def get_year():
    store_id = request.args.get('storeId')
    item_id = request.args.get('itemId')
    storeId = stores.get(store_id, "Could not find store")
    
    eventBool = request.args.get('event')
    snapBool = request.args.get('snap')
    
    event = True if eventBool.lower() == 'true' else False
    snap = True if snapBool.lower() == 'true' else False
    
    yearList = getData.getYearList(priceDF, storeId, item_id, event, snap)
    yearList = [year + 1 for year in yearList]
    
    return jsonify(yearList)

#* This will have caching
# http://127.0.0.1:5000/get-price-discount?storeId=st1Cal&itemId=FOODS_1_001&yearId=2015&event=True&snap=False&eventCount=1&snapCount=0&disId=10
@app.route('/get-price-discount', methods=['GET'])
def get_discount():
    # Getting user input
    store_id = request.args.get('storeId')
    item_id = request.args.get('itemId')
    year = int(request.args.get('yearId'))
    
    eventBool = request.args.get('event')
    snapBool = request.args.get('snap')
    
    # Assign boolean value
    event = True if eventBool.lower() == 'true' else False
    snap = True if snapBool.lower() == 'true' else False
    
    # Event value
    if event:
        eventCount = int(request.args.get('eventCount'))
    else:
        eventCount = 0
    
    # SNAP value
    if snap:
        snapCount = int(request.args.get('snapCount'))
    else:
        snapCount = 0
        
    discount = float(request.args.get('disId'))

    storeId = stores.get(store_id, "Could not find store")
    cache_key = f'get-price-discount?storeId={store_id}&itemId={item_id}&yearId={year}&event={eventBool}&snap={snapBool}&eventCount={eventCount}&snapCount={snapCount}&disId={discount}'

    cached_item = get_item(cache_key, "price_discount")

    if cached_item != "not available":
        print("Returning cached item from price discount!!!!!!!!")
        return cached_item
    else:
    # Getting base price and base demand
        base_price, base_demand = getData.getBase(demandDF, priceDF, year-1, storeId, item_id, event, snap)
        
        # Create model
        poly, model, rmse, score, x_priceDiscount, y_actual, x_values, y_predicted = priceElasticityModel.createModel(priceDF, year, storeId, item_id, event, snap, eventCount, snapCount)
    
        # Predicting demand
        # impact and demandText is for UI
        changeDemand, impact, demand, demandText = priceElasticityModel.predictDemand(poly, model, base_demand, discount, eventCount, snapCount)
        
        # Elasticity score and interpretation
        elasticity, interpretation = priceElasticityModel.priceElasticity(discount, changeDemand)
        result = {
            'Impact on Sales': str(impact),
            'Predicted Demand': str(demandText),
            'Elasticity Score': float(elasticity),
            'Elasticity Interpretation': str(interpretation)
        }
        
        # Store in cache
        put_item(cache_key, 'price_discount', result)
        
        return result
    
# Comment this code if you want to work on the backend only. This code will only work with the frontend.
# http://127.0.0.1:5000/get-price-elasticity?storeId=st1Cal&itemId=FOODS_1_001&yearId=2015&event=True&snap=False&eventCount=1&snapCount=0&disId=10
@app.route('/get-price-elasticity', methods=['GET'])
def main():   
    # Getting user input
    store_id = request.args.get('storeId')
    item_id = request.args.get('itemId')
    year = int(request.args.get('yearId'))
    
    eventBool = request.args.get('event')
    snapBool = request.args.get('snap')
    
    # Assign boolean value
    event = True if eventBool.lower() == 'true' else False
    snap = True if snapBool.lower() == 'true' else False
    
    # Event value
    if event:
        eventCount = int(request.args.get('eventCount'))
    else:
        eventCount = 0
    
    # SNAP value        
    if snap:
        snapCount = int(request.args.get('snapCount'))
    else:
        snapCount = 0
    
    discount = float(request.args.get('disId'))

    storeId = stores.get(store_id, "Could not find store")

    print(store_id, item_id, year, event, snap, eventCount, snapCount, discount)
    print("#############################################")
    
    # To show in the Front-end
    base_price, base_demand = getData.getBase(demandDF, priceDF, year-1, storeId, item_id, event, snap)
    
    # Not gonna show on UI
    poly, model, rmse, score, x_priceDiscount, y_actual, x_values, y_predicted = priceElasticityModel.createModel(priceDF, year, storeId, item_id, event, snap, eventCount, snapCount)
    
    rmse = round(rmse, 2)

    # 60 is the user input (discount)
    # impact and demandText is for UI
    # changeDemand, impact, demand, demandText = priceElasticityModel.predictDemand(poly, model, base_demand, discount, eventCount, snapCount)
    
    # elasticity, interpretation = priceElasticityModel.priceElasticity(discount, changeDemand)
    
    # Not gonna show on UI
    costPrice, stockOnHand, revenueList, stockCost = priceOptimization.calculateRevenue(demandDF, priceDF, year, storeId, item_id, event, snap, eventCount, snapCount)
    
    discount, optimizedPrice, totalSold, totalRevenue, profitLoss, totalDay = priceOptimization.getOptimizedPrice(revenueList, stockCost)   

    # For price elasticity section
    print("Printing the results (Price Elasticity)", base_price, base_demand, rmse, score)
    
    # For price optimization
    print("Printing the results (Price Optimization)", costPrice, stockOnHand, discount, optimizedPrice, totalSold, totalRevenue, profitLoss, totalDay)
    
    # print(x_priceDiscount, y_actual, x_values, y_predicted)
    
    return {
        # Model
        'Base Price': float(base_price),
        'Base Demand': float(base_demand),
        # RMSE (Root Mean Square Error) is a statistical measure used to quantify the average difference between observed values and predicted values
        'RMSE': float(rmse),
        # Measurement of the proportion of the variance in the dependent variable (target) that is predictable from the independent variables (features)
        'Score': float(score),
        
        # # Discount
        # 'Impact on Sales': str(impact),
        # 'Predicted Demand': str(demandText),
        # 'Elasticity Score': float(elasticity),
        # 'Elasticity Interpretation': str(interpretation),
        
        # Price Optimization
        'Cost Price/Item': float(costPrice),
        'Stock on Hand': int(stockOnHand),
        'Price Discount': str(int(discount)) + ' %',
        'Optimized Price': float(optimizedPrice),
        'Total item(s) sold': str(int(totalSold)) + ' Items',
        'Total Revenue': float(totalRevenue),
        'PROFIT/LOSS': str(profitLoss),
        'Gain profit in (days)': int(totalDay),
        
        # Graph
        # Scatter
        'x_actual': x_priceDiscount.tolist(),
        'y_actual': y_actual.tolist(),
        # Line
        'x_values': x_values.tolist(),
        'y_predicted': y_predicted.tolist()
    }


if __name__ == '__main__':
    app.run(debug=True)

