import boto3
import json
import pandas as pd
import ast

import unittest
from priceOptimization import getOptimizedPrice, calculateRevenue, getDummyData, getCostStock

from secret_manager import get_access_key, get_secret_key

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

priceDF = pd.read_csv(s3.Bucket(name='fit3164bucket').Object('price.csv').get()['Body'])

priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)

demandDF = pd.read_csv(s3.Bucket(name='fit3164bucket').Object('demand.csv').get()['Body'])

demandDF['withoutBoth'] = demandDF['withoutBoth'].apply(ast.literal_eval)
demandDF['withBoth'] = demandDF['withBoth'].apply(ast.literal_eval)
demandDF['onlyEvent'] = demandDF['onlyEvent'].apply(ast.literal_eval)
demandDF['onlySNAP'] = demandDF['onlySNAP'].apply(ast.literal_eval)

demandDF['start_date'] = pd.to_datetime(demandDF['start_date'])
demandDF['end_date'] = pd.to_datetime(demandDF['end_date'])


class TestPriceOptimization(unittest.TestCase):
    def test_getOptimizedPrice(self):
        revList_1 = [(10, 9, 10, 90, 'PROFIT'), (15, 8.5, 15, 127.5, 'PROFIT')]
        stockCost_1 = 80
        expectedOutput_1 = (15, 8.5, 15, 127.5, 'PROFIT', 7)
        self.assertEqual(getOptimizedPrice(revList_1, stockCost_1), expectedOutput_1)
        
        revList_2 = [(10, 7, 10, 70, 'LOSS'), (15, 6.16, 10, 61.6, 'LOSS')]
        stockCost_2 = 100
        expectedOutput_2 = (10, 7, 10, 70, 'LOSS', 10)
        self.assertEqual(getOptimizedPrice(revList_2, stockCost_2), expectedOutput_2)
        
        revList_3 = [(10, 2.25, 50, 112.5, 'PROFIT'), (5, 2.38, 40, 95.2, 'LOSS'), (20, 2, 45, 90, 'LOSS')]
        stockCost_3 = 100
        expectedOutput_3 = (10, 2.25, 50, 112.5, 'PROFIT', 7)
        self.assertEqual(getOptimizedPrice(revList_3, stockCost_3), expectedOutput_3)
        
        revList_4 = []
        stockCost_4 = 100
        self.assertEqual(getOptimizedPrice(revList_4, stockCost_4), 'Empty List')

    def test_calculateRevenue(self):
        revenue_1 = calculateRevenue(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', False, False, eventCount=0, snapCount=0)
        self.assertEqual(revenue_1, 'Year Invalid')
        
        revenue_2 = calculateRevenue(demandDF, priceDF, 2012, 'CA_1', 'FOODS_1_001', True, False, eventCount=0, snapCount=0)
        self.assertEqual(revenue_2, 'Invalid Data')
        
        revenue_3 = calculateRevenue(demandDF, priceDF, 2012, 'CA_1', 'FOODS_1_001', False, False, eventCount=2, snapCount=0)
        self.assertEqual(revenue_3, 'Invalid Data')
        
        revenue_4 = calculateRevenue(demandDF, priceDF, 2012, 'CA_1', 'FOODS_1_001', False, True, eventCount=0, snapCount=0)
        self.assertEqual(revenue_4, 'Invalid Data')
        
        revenue_5 = calculateRevenue(demandDF, priceDF, 2012, 'CA_1', 'FOODS_1_001', False, False, eventCount=0, snapCount=4)
        self.assertEqual(revenue_5, 'Invalid Data')
        
        revenue_6 = calculateRevenue(demandDF, priceDF, 2012, 'CA_1', 'FOODS_1_001', event=False, snap=True, eventCount=0, snapCount=3) 
        self.assertEqual(revenue_6[0], 1) # costPrice
        self.assertEqual(revenue_6[1], 95) # stockOnHand
        self.assertEqual(revenue_6[3], 95) # stockCost
        # Check revenueList (PROFIT/LOSS)
        for tuple_item in revenue_6[2]:
            if tuple_item[-1] == 'PROFIT':
                self.assertTrue(tuple_item[3] > revenue_6[3])
            elif tuple_item[-1] == 'LOSS':
                self.assertTrue(tuple_item[3] < revenue_6[3])
                
        revenue_7 = calculateRevenue(demandDF, priceDF, 2014, 'TX_1', 'HOUSEHOLD_2_466', event=False, snap=True, eventCount=0, snapCount=3) 
        self.assertEqual(revenue_7[0], 13.82) # costPrice
        self.assertEqual(revenue_7[1], 72) # stockOnHand
        self.assertEqual(revenue_7[3], 995.04) # stockCost
        # Check revenueList (PROFIT/LOSS)
        for tuple_item in revenue_7[2]:
            if tuple_item[-1] == 'PROFIT':
                self.assertTrue(tuple_item[3] > revenue_7[3])
            elif tuple_item[-1] == 'LOSS':
                self.assertTrue(tuple_item[3] < revenue_7[3])
            

    def test_getDummyData_and_getCostStock(self):
        data_1 = getDummyData(2011)  
        self.assertEqual(len(data_1), 16762)  

        data_2 = getDummyData(2012)  
        self.assertEqual(len(data_2), 21894)

        data_3 = getDummyData(2013)  
        self.assertEqual(len(data_3), 25939)

        data_4 = getDummyData(2014)  
        self.assertEqual(len(data_4), 29584)

        data_5 = getDummyData(2015)  
        self.assertEqual(len(data_5), 30474)

        data_6 = getDummyData(2016)  
        self.assertEqual(len(data_6), 30490)


        stock_1 = getCostStock(data_1, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_1, (1, 95))  

        stock_2 = getCostStock(data_2, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_2, (1.12, 97))

        stock_3 = getCostStock(data_3, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_3, (1.12, 92))

        stock_4 = getCostStock(data_4, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_4, (1.12, 93))

        stock_5 = getCostStock(data_5, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_5, (1.12, 93))

        stock_6 = getCostStock(data_6, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_6, (1.12, 94))


if __name__ == '__main__': 
    unittest.main()