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
print("count: ", client["sample"]["books"].count_documents({}))
print("count books with pageCount > 400: ",
      client["sample"]["books"].count_documents({"pageCount": {"$gt": 400}}))
print("count books with pageCount > 400 and published: ",
      client["sample"]["books"].count_documents({"$and": [{"pageCount": {"$gt": 400}}, {"status": "PUBLISH"}]}))
print("count books with the keyword Android in the shortDescription or longDescription :", client["sample"]["books"].count_documents(
    {"$or": [{"shortDescription": {"$regex": "Android"}}, {"longDescription": {"$regex": "Android"}}]}))
