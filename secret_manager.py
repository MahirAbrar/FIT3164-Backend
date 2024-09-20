import boto3
from botocore.exceptions import ClientError
import json

class SecretManager:
    _instance = None
    _secret = None
    _access_key = None
    _secret_key = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SecretManager, cls).__new__(cls)
            cls._instance._load_secret()
        return cls._instance

    # Loading credentials
    def _load_secret(self):
        secret_name = "fit3164s3bucket"
        region_name = "ap-southeast-2"

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = get_secret_value_response['SecretBinary']

            self._secret = json.loads(secret)
            
            # Extract access key and secret key
            self._access_key = list(self._secret.keys())[0]
            self._secret_key = self._secret[self._access_key]
        except ClientError as e:
            print(f"Error retrieving secret: {e}")
            self._secret = None

    def get_secret(self):
        return self._secret

    def get_access_key(self):
        return self._access_key

    def get_secret_key(self):
        return self._secret_key

# Initialize the secret manager
secret_manager = SecretManager()

def get_secret():
    return secret_manager.get_secret()

def get_access_key():
    return secret_manager.get_access_key()

def get_secret_key():
    return secret_manager.get_secret_key()

# Caching
class DynamoDBManager:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb',
                                       aws_access_key_id=secret_manager.get_access_key(),
                                       aws_secret_access_key=secret_manager.get_secret_key(),
                                       region_name='ap-southeast-2')
        
        self.table = self.dynamodb.Table("cache")

    def get_item(self, key, item_type):
        """
        Get an item from the DynamoDB table.
        
        :param key: The primary key of the item
        :param item_type: Either 'price_discount' or 'price_elasticity'
        :return: The requested data if available, otherwise "not available"
        """
        print(f"Getting item for key: {key}, type: {item_type}")
        try:
            response = self.table.get_item(Key={"input": key})
            
            if 'Item' in response:
                item_data = json.loads(response['Item'].get('data', '{}'))
                
                if item_type == 'price_discount':
                    if 'Impact on Sales' in item_data:
                        return {
                            'Impact on Sales': item_data['Impact on Sales'],
                            'Predicted Demand': item_data['Predicted Demand'],
                            'Elasticity Score': item_data['Elasticity Score'],
                            'Elasticity Interpretation': item_data['Elasticity Interpretation']
                        }
                elif item_type == 'price_elasticity':
                    if 'Base Price' in item_data:
                        return {
                            'Base Price': item_data['Base Price'],
                            'Base Demand': item_data['Base Demand'],
                            'RMSE': item_data['RMSE'],
                            'Score': item_data['Score'],
                            'Cost Price/Item': item_data['Cost Price/Item'],
                            'Stock on Hand': item_data['Stock on Hand'],
                            'Price Discount': item_data['Price Discount'],
                            'Optimized Price': item_data['Optimized Price'],
                            'Total item(s) sold': item_data['Total item(s) sold'],
                            'Total Revenue': item_data['Total Revenue'],
                            'PROFIT/LOSS': item_data['PROFIT/LOSS'],
                            'Gain profit in (days)': item_data['Gain profit in (days)'],
                            'x_actual': item_data['x_actual'],
                            'y_actual': item_data['y_actual'],
                            'x_values': item_data['x_values'],
                            'y_predicted': item_data['y_predicted']
                        }
                
                print("Could not find specific data for the item type")
                return "not available"
            else:
                print("Could not find item: return not available")
                return "not available"
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'ResourceNotFoundException':
                return "Table not found"
            elif error_code == 'ProvisionedThroughputExceededException':
                return "Throughput exceeded"
            else:
                return f"An error occurred: {error_message}"
                
        except Exception as e:
            return f"An unexpected error occurred: {str(e)}"
        

    # requires key and all the values
    def put_item(self, key, item_type, data):
        """
        Put an item into the DynamoDB table.
        
        :param key: The primary key of the item
        :param item_type: Either 'price_discount' or 'price_elasticity'
        :param data: The data to be stored
        :return: Response from DynamoDB or error message
        """
        print(f"Putting item for key: {key}, type: {item_type}")
        try:
            # First, try to get the existing item
            existing_item = self.table.get_item(Key={"input": key}).get('Item', {})
            existing_data = json.loads(existing_item.get('data', '{}'))
            
            # Update the existing data with the new data
            if item_type == 'price_discount':
                print("updating cache data!!!!")
                print("######################################")
                existing_data.update({
                    'Impact on Sales': data['Impact on Sales'],
                    'Predicted Demand': data['Predicted Demand'],
                    'Elasticity Score': data['Elasticity Score'],
                    'Elasticity Interpretation': data['Elasticity Interpretation']
                })
            elif item_type == 'price_elasticity':
                existing_data.update({
                    'Base Price': data['Base Price'],
                    'Base Demand': data['Base Demand'],
                    'RMSE': data['RMSE'],
                    'Score': data['Score'],
                    'Cost Price/Item': data['Cost Price/Item'],
                    'Stock on Hand': data['Stock on Hand'],
                    'Price Discount': data['Price Discount'],
                    'Optimized Price': data['Optimized Price'],
                    'Total item(s) sold': data['Total item(s) sold'],
                    'Total Revenue': data['Total Revenue'],
                    'PROFIT/LOSS': data['PROFIT/LOSS'],
                    'Gain profit in (days)': data['Gain profit in (days)'],
                    'x_actual': data['x_actual'],
                    'y_actual': data['y_actual'],
                    'x_values': data['x_values'],
                    'y_predicted': data['y_predicted']
                })
            else:
                return f"Invalid item_type: {item_type}"

            # Put the updated item back into the table
            item = {
                "input": key,
                "data": json.dumps(existing_data)
            }
            response = self.table.put_item(Item=item)
            return response
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            if error_code == 'ResourceNotFoundException':
                return "Table not found"
            elif error_code == 'ProvisionedThroughputExceededException':
                return "Throughput exceeded"
            else:
                return f"An error occurred: {error_message}"
                
        except Exception as e:
            return f"An unexpected error occurred: {str(e)}"



dynamodb_manger = DynamoDBManager()
print(dynamodb_manger)

def put_item(key, item_type, data):
    return dynamodb_manger.put_item(key, item_type, data)

def get_item(key, item_type):
    return dynamodb_manger.get_item(key, item_type)

def delete_item(key):
    return dynamodb_manger.delete_item(key)