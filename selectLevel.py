import json
import pandas as pd
import boto3
import ast

from getData import getYearList
from secret_manager import get_access_key, get_secret_key

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def levelSelection(priceDF, data, year: int, store_id: str, item_id: str, event: bool, snap: bool):
    
    # Previous year
    year -= 1
    
    # Check year is valid first
    if year not in getYearList(priceDF, store_id, item_id, event, snap):
        # Improvement: put minimum year that is valid
        return "Year Invalid"
    
    productInfo = getProductInfo(store_id, item_id)
    
    for key, value in reversed(productInfo.items()):
        if key in ["item_id", "dept_id", "cat_id"]:
            filter_condition = "store_id == " + f"'{productInfo['store_id']}'" + " and " + f"{key} == '{value}'"
        else: 
            filter_condition =  f"{key} == '{value}'"

        filteredDF = data.query(filter_condition)
        
        if len(filteredDF) >= 500:
            level = key
            break
        
    return level

def getProductInfo(store_id: str, item_id: str):
    state_id = store_id.split('_')[0]
    dept_id = '_'.join(item_id.split('_')[:2])
    cat_id = item_id.split('_')[0]
    
    return {'state_id': state_id, 'store_id': store_id, 'cat_id': cat_id, 'dept_id': dept_id, 'item_id': item_id}
