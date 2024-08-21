import boto3
import json
import pandas as pd
import ast
from secret_manager import get_access_key, get_secret_key

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

# Take base price and base demand from the actual year inputted (not from year predicted)
def getBase(demandDF, priceDF, year: int, store_id: str, item_id: str, event: bool, snap: bool):
    if year not in getYearList(priceDF, store_id, item_id, event, snap):
        # Improvement: put minimum year that is valid
        return "Year Invalid"
    
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)].reset_index()
    productSalesInfo = demandDF[(demandDF['item_id'] == item_id) & (demandDF['store_id'] == store_id)].reset_index()
    
    colName = getColumnName(event, snap)
    
    basePrice = productPriceInfo[f"basePrice_{colName}"][0][year]
    # print("base price: ", basePrice)

    basePriceRow = productSalesInfo[(productSalesInfo['sell_price'] == basePrice) & (productSalesInfo['start_date'].dt.year <= year) & 
                                   (productSalesInfo['end_date'].dt.year >= year)].reset_index()
    # print(basePriceRow)
    # Remove row that has num_week of 0
    basePriceRow = basePriceRow[basePriceRow.apply(lambda row: row[colName][year]['num_week'] != 0, axis=1)].reset_index(drop=True)

    # print(basePriceRow)
    if len(basePriceRow) == 1:
        baseDemand = basePriceRow[colName][0][year]['avg_sold_wk']
    else:
        baseDemand = basePriceRow[colName][0][year]['avg_sold_wk']
        for i in range(1, len(basePriceRow)):
            currBaseDemand = basePriceRow[colName][i][year]['avg_sold_wk']
            if currBaseDemand < baseDemand:
                baseDemand = currBaseDemand
    # print("base demand: ", baseDemand)
    
    return basePrice, baseDemand

# Year list for the data
# If you want to take yearList for prediction just + 1 on the list
def getYearList(priceDF, store_id: str, item_id: str, event: bool, snap: bool):
    colName = getColumnName(event, snap)
    colName = f"basePrice_{colName}"
    
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)].reset_index(drop=True)
    data = productPriceInfo[colName].iloc[0]
    
    yearList = [year for year, price in data.items() if price != 0]
    
    return yearList

def getColumnName(event: bool, snap: bool):
    if event == False and snap == False:
        colName = 'withoutBoth'
    elif event == True and snap == False:
        colName = 'onlyEvent'
    elif event == False and snap == True:
        colName = 'onlySNAP'
    elif event == True and snap == True:
        colName = 'withBoth'

    return colName

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
       
    getYearList(priceDF, "CA_1", "FOODS_1_043", True, True)
    
    getBase(demandDF, priceDF, 2011, "CA_1", "FOODS_1_074", True, False)