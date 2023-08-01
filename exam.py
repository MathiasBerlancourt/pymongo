from pymongo import MongoClient

from pprint import pprint

client = MongoClient(
    host="127.0.0.1",
    port=27018,
    username="admin",
    password="pass"
)
print("Databases: ", client.list_database_names())
print("Collections: ", client["sample"].list_collection_names())
pprint(client["sample"]["books"].find_one())
