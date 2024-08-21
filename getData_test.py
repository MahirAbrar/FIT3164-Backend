import json
import pandas as pd
import boto3
import ast

from secret_manager import get_access_key, get_secret_key
import unittest
from getData import getBase, getColumnName, getYearList

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

class TestGetData(unittest.TestCase):
    def test_getBase(self):
        result_1 = getBase(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_071', False, False)
        self.assertEqual(result_1, 'Year Invalid')
        
        result_2 = getBase(demandDF, priceDF, 2013, 'CA_1', 'FOODS_1_071', True, False)
        self.assertEqual(result_2, (1.97, 5.333))
        
        result_3 = getBase(demandDF, priceDF, 2013, 'CA_1', 'FOODS_1_043', True, True)
        self.assertEqual(result_3, (1.78, 61.627))
        
        result_4 = getBase(demandDF, priceDF, 2014, 'TX_1', 'HOUSEHOLD_1_043', False, True) 
        self.assertEqual(result_4, (2.97, 0.6))
    
    def test_getColumnName(self):
        colName_withoutBoth = getColumnName(False, False)
        self.assertEqual(colName_withoutBoth, 'withoutBoth')
        
        colName_withBoth = getColumnName(True, True)
        self.assertEqual(colName_withBoth, 'withBoth')
        
        colName_onlyEvent = getColumnName(True, False)
        self.assertEqual(colName_onlyEvent, 'onlyEvent')
        
        colName_onlySNAP = getColumnName(False, True)
        self.assertEqual(colName_onlySNAP, 'onlySNAP')
    
    def test_getYearList(self):
        # Random Sample (with different yearList for different condition)
        yearList_1 = getYearList(priceDF, 'CA_1', 'FOODS_1_071', False, False)
        self.assertEqual(yearList_1, [2012, 2013, 2014, 2015, 2016])
        
        yearList_2  = getYearList(priceDF, 'CA_1', 'FOODS_1_071', True, True)
        self.assertEqual(yearList_2, [2013, 2014, 2015, 2016])
        
        yearList_3  = getYearList(priceDF, 'CA_1', 'FOODS_1_071', True, False)
        self.assertEqual(yearList_3, [2012, 2013, 2014, 2015, 2016])
        
        yearList_4  = getYearList(priceDF, 'CA_1', 'FOODS_1_071', False, True)
        self.assertEqual(yearList_4, [2012, 2013, 2014, 2015, 2016])
        
        yearList_5 = getYearList(priceDF, 'CA_1', 'FOODS_1_043', False, False)
        self.assertEqual(yearList_5, [2011, 2012, 2013, 2014, 2015, 2016])
        
        yearList_6 = getYearList(priceDF, 'CA_1', 'FOODS_1_043', True, True)
        self.assertEqual(yearList_6, [2012, 2013, 2014, 2015, 2016])
        
        yearList_7 = getYearList(priceDF, 'CA_1', 'FOODS_1_043', True, False)
        self.assertEqual(yearList_7, [2012, 2013, 2014, 2015, 2016])
        
        yearList_8 = getYearList(priceDF, 'CA_1', 'FOODS_1_043', False, True)
        self.assertEqual(yearList_8, [2012, 2013, 2014, 2015, 2016])
    
if __name__ == '__main__': 
    unittest.main()