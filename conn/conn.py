import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

env_path = ".env"
load_dotenv(dotenv_path=env_path)

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

EXTERAL_USER = os.getenv('EXTERAL_USER')
EXTERNAL_PASSWORD = os.getenv('EXTERNAL_PASSWORD')
EXTERNAL_HOST = os.getenv('EXTERNAL_HOST')
EXTERNAL_APP_NAME = os.getenv('EXTERNAL_APP_NAME')

EXTERNAL_URI = f"mongodb+srv://{EXTERAL_USER}:{EXTERNAL_PASSWORD}@{EXTERNAL_HOST}/?retryWrites=true&w=majority&appName={EXTERNAL_APP_NAME}"

# Create a new client and connect to the server
EXTERNAL_CLIENT = MongoClient(EXTERNAL_URI, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    EXTERNAL_CLIENT.admin.command('ping')
    print("Pinged your deployment. You successfully connected to EXTERNAL DATA RESOURCES")
except Exception as e:
    print(e)


DATALAKE_USER = os.getenv('DATALAKE_USER')
DATALAKE_PASSWORD = os.getenv('DATALAKE_PASSWORD')
DATALAKE_HOST = os.getenv('DATALAKE_HOST')

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

DATALAKE_URI = f"mongodb+srv://{DATALAKE_USER}:{DATALAKE_PASSWORD}@{DATALAKE_HOST}/?retryWrites=true&w=majority"

# Create a new client and connect to the server
DATALAKE_CLIENT = MongoClient(DATALAKE_URI)

# Send a ping to confirm a successful connection
try:
    DATALAKE_CLIENT.admin.command('ping')
    print("Pinged your deployment. You successfully connected to DATALAKE!")
except Exception as e:
    print(e)

