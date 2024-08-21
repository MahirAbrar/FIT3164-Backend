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