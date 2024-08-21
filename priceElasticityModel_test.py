import boto3
import json
import pandas as pd
import ast

from selectLevel import getProductInfo
from getData import getBase
from secret_manager import get_access_key, get_secret_key

import unittest
from priceElasticityModel import createModel, predictDemand, priceElasticity, getDataset, filterData

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

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

class TestPriceElasticityModel(unittest.TestCase):
    def test_createModel_and_predictDemand(self):
        result_1 = createModel(priceDF, 2011, 'CA_1', 'FOODS_1_001', False, False, eventCount=0, snapCount=0)
        self.assertEqual(result_1, 'Year Invalid')
        
        result_2 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', True, False, eventCount=0, snapCount=0)
        self.assertEqual(result_2, 'Invalid Data')
        
        result_3 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', False, False, eventCount=2, snapCount=0)
        self.assertEqual(result_3, 'Invalid Data')
        
        result_4 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', False, True, eventCount=0, snapCount=0)
        self.assertEqual(result_4, 'Invalid Data')
        
        result_5 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', False, False, eventCount=0, snapCount=4)
        self.assertEqual(result_5, 'Invalid Data')
        
        # Without Event and SNAP
        result_6 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', False, False, eventCount=0, snapCount=0)
        self.assertEqual(len(result_6), 8)
        
        _, base_demand = getBase(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', False, False)
        
        _, _, demand, _= predictDemand(result_6[0], result_6[1], base_demand, 10, eventCount=0, snapCount=0)
        _, _, demand_1, _ = predictDemand(result_6[0], result_6[1], base_demand, 10, eventCount=1, snapCount=0)
        _, _, demand_2, _ = predictDemand(result_6[0], result_6[1], base_demand, 10, eventCount=0, snapCount=1)
        _, _, demand_3, _ = predictDemand(result_6[0], result_6[1], base_demand, 10, eventCount=1, snapCount=1)
        
        self.assertEqual(demand, demand_1)
        self.assertEqual(demand, demand_2)
        self.assertEqual(demand, demand_3)
        
        # With only event
        result_7 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', True, False, eventCount=1, snapCount=0)
        self.assertEqual(len(result_7), 8)
        
        _, base_demand = getBase(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', True, False)
        
        _, _, demand, _ = predictDemand(result_7[0], result_7[1], base_demand, 10, eventCount=1, snapCount=0)
        _, _, demand_1, _ = predictDemand(result_7[0], result_7[1], base_demand, 10, eventCount=0, snapCount=0)
        _, _, demand_2, _ = predictDemand(result_7[0], result_7[1], base_demand, 10, eventCount=0, snapCount=1)
        _, _, demand_3, _ = predictDemand(result_7[0], result_7[1], base_demand, 10, eventCount=1, snapCount=1)
        
        self.assertNotEqual(demand, demand_1)
        self.assertNotEqual(demand, demand_2)
        self.assertEqual(demand_1, demand_2)
        self.assertEqual(demand, demand_3)
        
        # With only SNAP
        result_8 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', False, True, eventCount=0, snapCount=1)
        self.assertEqual(len(result_8), 8)
        
        _, base_demand = getBase(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', False, True)
        
        _, _, demand, _ = predictDemand(result_8[0], result_8[1], base_demand, 10, eventCount=0, snapCount=1)
        _, _, demand_1, _ = predictDemand(result_8[0], result_8[1], base_demand, 10, eventCount=1, snapCount=0)
        _, _, demand_2, _ = predictDemand(result_8[0], result_8[1], base_demand, 10, eventCount=0, snapCount=0)
        _, _, demand_3, _ = predictDemand(result_8[0], result_8[1], base_demand, 10, eventCount=1, snapCount=1)
        
        self.assertNotEqual(demand, demand_1)
        self.assertNotEqual(demand, demand_2)
        self.assertEqual(demand_1, demand_2)
        self.assertEqual(demand, demand_3)
        
        # With both Event and SNAP
        result_9 = createModel(priceDF, 2012, 'CA_1', 'FOODS_1_001', True, True, eventCount=1, snapCount=1)
        self.assertEqual(len(result_9), 8)
        
        _, base_demand = getBase(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', True, True)
        
        _, _, demand, _ = predictDemand(result_9[0], result_9[1], base_demand, 10, eventCount=1, snapCount=1)
        _, _, demand_1, _ = predictDemand(result_9[0], result_9[1], base_demand, 10, eventCount=1, snapCount=0)
        _, _, demand_2, _ = predictDemand(result_9[0], result_9[1], base_demand, 10, eventCount=0, snapCount=1)
        _, _, demand_3, _ = predictDemand(result_9[0], result_9[1], base_demand, 10, eventCount=0, snapCount=0)
        
        self.assertNotEqual(demand, demand_1)
        self.assertNotEqual(demand, demand_2)
        self.assertNotEqual(demand, demand_3)
        
    
    def test_priceElasticity(self):
        elasticity_1, interpretation_1 = priceElasticity(0, 1)
        self.assertEqual(elasticity_1, float('inf'))
        self.assertEqual(interpretation_1, "Perfectly elastic")
        
        elasticity_2, interpretation_2 = priceElasticity(5, 10)
        self.assertEqual(elasticity_2, 2)
        self.assertEqual(interpretation_2, "Elastic")
        
        elasticity_3, interpretation_3 = priceElasticity(5, 5)
        self.assertEqual(elasticity_3, 1)
        self.assertEqual(interpretation_3, "Unitary elastic")
        
        elasticity_4, interpretation_4 = priceElasticity(10, 5)
        self.assertEqual(elasticity_4, 0.5)
        self.assertEqual(interpretation_4, "Inelastic")
        
        elasticity_5, interpretation_5 = priceElasticity(5, 0)
        self.assertEqual(elasticity_5, 0)
        self.assertEqual(interpretation_5, "Perfectly inelastic")
    
    def test_getDataset_and_filterData(self):
        data_1 = getDataset(2011, True, False)
        self.assertEqual(len(data_1), 58794)
        
        data_2 = getDataset(2011, False, False)
        self.assertEqual(len(data_2), 25744)
        
        data_3 = getDataset(2011, True, True)
        self.assertEqual(len(data_3), 138809)
        
        data_4 = getDataset(2011, False, True)
        self.assertEqual(len(data_4), 105180)
        
        data_5 = getDataset(2012, True, False)
        self.assertEqual(len(data_5), 51835)
        
        data_6 = getDataset(2013, False, False)
        self.assertEqual(len(data_6), 32970)
        
        data_7 = getDataset(2014, True, True)
        self.assertEqual(len(data_7), 205672)
        
        productInfo = getProductInfo('CA_1', 'FOODS_1_011')
        filterData_1 = filterData(data_1, productInfo, 'item_id')
        self.assertEqual(len(filterData_1), 5)
        
        filterData_2 = filterData(data_1, productInfo, 'dept_id')
        self.assertEqual(len(filterData_2), 575)
        
        filterData_3 = filterData(data_1, productInfo, 'cat_id')
        self.assertEqual(len(filterData_3), 3500)
        
        filterData_4 = filterData(data_1, productInfo, 'store_id')
        self.assertEqual(len(filterData_4), 6653)
        
        filterData_5 = filterData(data_1, productInfo, 'state_id')
        self.assertEqual(len(filterData_5), 25104)
            
    
if __name__ == '__main__': 
    unittest.main()