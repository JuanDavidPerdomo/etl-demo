import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

env_path = ".env"
load_dotenv(dotenv_path=env_path)

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

EXTERAL_USER = os.getenv("EXTERAL_USER")
EXTERNAL_PASSWORD = os.getenv("EXTERNAL_PASSWORD")
EXTERNAL_HOST = os.getenv("EXTERNAL_HOST")
EXTERNAL_APP_NAME = os.getenv("EXTERNAL_APP_NAME")

EXTERNAL_URI = f"mongodb+srv://{EXTERAL_USER}:{EXTERNAL_PASSWORD}@{EXTERNAL_HOST}/?retryWrites=true&w=majority&appName={EXTERNAL_APP_NAME}"

# Create a new client and connect to the server
EXTERNAL_CLIENT = MongoClient(EXTERNAL_URI, server_api=ServerApi("1"))

# Send a ping to confirm a successful connection
try:
    EXTERNAL_CLIENT.admin.command("ping")
    print(
        "Pinged your deployment. You successfully connected to EXTERNAL DATA RESOURCES DB"
    )
except Exception as e:
    print(e)


ETL_USER = os.getenv("ETL_USER")
ETL_PASSWORD = os.getenv("ETL_PASSWORD")
ETL_HOST = os.getenv("ETL_HOST")

ETL_URI = (
    f"mongodb+srv://{ETL_USER}:{ETL_PASSWORD}@{ETL_HOST}/?retryWrites=true&w=majority"
)

# Create a new client and connect to the server
ETL_CLIENT = MongoClient(ETL_URI)

# Send a ping to confirm a successful connection
try:
    ETL_CLIENT.admin.command("ping")
    print("Pinged your deployment. You successfully connected to ETL DB")
except Exception as e:
    print(e)

external_db = EXTERNAL_CLIENT.cirotex
inhouse_datalake_db = ETL_CLIENT.inHouseDataLake
datawarehouse_db = ETL_CLIENT.warehouse
