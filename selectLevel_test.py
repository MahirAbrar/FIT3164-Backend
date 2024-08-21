import json
import pandas as pd
import boto3
import ast

from priceElasticityModel import filterData
from secret_manager import get_access_key, get_secret_key

import unittest
from selectLevel import getProductInfo, levelSelection

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

# Get access key
access_key = get_access_key()
secret_key = get_secret_key

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

data_2011 = pd.read_csv(s3.Bucket(name='fit3164bucket').Object("modelData/data_2011.csv").get()['Body'])
data_2013 = pd.read_csv(s3.Bucket(name='fit3164bucket').Object("modelData/data_2013.csv").get()['Body'])
data_2014 = pd.read_csv(s3.Bucket(name='fit3164bucket').Object("modelData/data_2014.csv").get()['Body'])

class TestSelectLevel(unittest.TestCase):
    def test_getProductInfo(self):
        productInfo = getProductInfo('WI_2', 'HOBBIES_1_100')
        self.assertEqual(productInfo, {'state_id': 'WI', 'store_id': 'WI_2', 'cat_id': 'HOBBIES', 'dept_id': 'HOBBIES_1', 'item_id': 'HOBBIES_1_100'})
    
    def test_levelSelection(self):
        data_1 = data_2011[(data_2011['eventCount'] != 0 ) & (data_2011['snapCount'] == 0)].reset_index(drop=True)
        
        level_1 = levelSelection(priceDF, data_1, 2011+1, 'CA_1', 'FOODS_1_043', True, False)
        self.assertEqual(level_1, 'Year Invalid')
        
        data_2 = data_2013[(data_2013['eventCount'] == 0 ) & (data_2013['snapCount'] == 0)].reset_index(drop=True)
        
        level_2 = levelSelection(priceDF, data_2, 2013+1, 'CA_1', 'FOODS_1_043', False, False)
        self.assertEqual(level_2, 'cat_id')
        self.assertLessEqual(len(filterData(data_2, getProductInfo('CA_1', 'FOODS_1_043'), 'dept_id')), 500)
        self.assertGreaterEqual(len(filterData(data_2, getProductInfo('CA_1', 'FOODS_1_043'), level_2)), 500)
        
        data_3 = data_2014[(data_2014['eventCount'] != 0 ) & (data_2014['snapCount'] != 0)].reset_index(drop=True)
        
        level_3 = levelSelection(priceDF, data_3, 2014+1, 'TX_1', 'HOUSEHOLD_1_442', True, True)
        self.assertEqual(level_3, 'dept_id')
        self.assertLessEqual(len(filterData(data_3, getProductInfo('TX_1', 'HOUSEHOLD_1_442'), 'item_id')), 500)
        self.assertGreaterEqual(len(filterData(data_3, getProductInfo('TX_1', 'HOUSEHOLD_1_442'), level_3)), 500)

if __name__ == '__main__': 
    unittest.main()