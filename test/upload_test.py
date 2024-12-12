import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pymongo import MongoClient
from src.utils import generate_connection_string, format_documents
import json
from dotenv import load_dotenv
import os

load_dotenv()
# Your Atlas connection string should look something like:
# mongodb+srv://<username>:<password>@<cluster-url>/<database-name>?retryWrites=true&w=majority

username = os.getenv('MONGODB_USERNAME')
password = os.getenv('MONGODB_PASSWORD')
database_name = os.getenv('APP_NAME')
cluster_url = os.getenv('CLUSTER_URL')
connection_string = generate_connection_string(username, password, database_name, cluster_url)

# # Connect to your Atlas cluster
client = MongoClient(connection_string)

# # Select your database and collection
db = client['nfl']
collection = db['play_by_play']

# Read and insert the JSON file
with open('assets/play_by_play_silver_202412121404.json', 'r') as file:
    data = json.load(file)

    documents = list(data.values())[0]
    formatted_documents = format_documents(documents)
    collection.insert_many(formatted_documents)
    print(f"Total documents in collection: {collection.count_documents({})}")





    # # If your JSON contains a single document
    # if isinstance(data, dict):
    #     result = collection.insert_one(data)
    #     print(f"Inserted document with id: {result.inserted_id}")
    
    # # If your JSON contains multiple documents in an array
    # elif isinstance(data, list):
    #     result = collection.insert_many(data)
    #     print(f"Inserted {len(result.inserted_ids)} documents")

# # Verify the upload
# print(f"Total documents in collection: {collection.count_documents({})}")