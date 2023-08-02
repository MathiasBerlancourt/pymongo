from pymongo import MongoClient

from pprint import pprint

client = MongoClient(
    host="127.0.0.1",
    port=27018,
    username="admin",
    password="pass"
)
print("Databases: \n", client.list_database_names())
print("Collections: \n", client["sample"].list_collection_names())
pprint(client["sample"]["books"].find_one())
print("count: ", client["sample"]["books"].count_documents({}))
print("count books with pageCount > 400:\n ",
      client["sample"]["books"].count_documents({"pageCount": {"$gt": 400}}))
print("count books with pageCount > 400 and published:\n ",
      client["sample"]["books"].count_documents({"$and": [{"pageCount": {"$gt": 400}}, {"status": "PUBLISH"}]}))
print("count books with the keyword Android in the shortDescription or longDescription :\n", client["sample"]["books"].count_documents(
    {"$or": [{"shortDescription": {"$regex": "Android"}}, {"longDescription": {"$regex": "Android"}}]}))
print("category 1 and category 2 list:\n", list(client["sample"]["books"].aggregate([
    {"$project": {
        "categorie1": {"$arrayElemAt": ["$categories", 0]},
        "categorie2": {"$arrayElemAt": ["$categories", 1]}
    }},
    {"$group": {
        "_id": None,
        "Categorie1": {"$addToSet": "$categorie1"},
        "Categorie2": {"$addToSet": "$categorie2"}
    }}
])))
print("count books with the keywords Python or Java or C++ or Scala in the longDescription : \n",
      client["sample"]["books"].count_documents({"longDescription": {"$regex": "Python|Java|C++|Scala"}}))
pprint(list(client["sample"]["books"].aggregate([
    {"$unwind": "$categories"},
    {"$group": {
        "_id": "$categories",
        "max_pages": {"$max": "$pageCount"},
        "min_pages": {"$min": "$pageCount"},
        "average_pages": {"$avg": "$pageCount"},
        "count": {"$sum": 1}
    }}
])))

pprint(list(client["sample"]["books"].aggregate([
    {"$project": {
        "title": 1,
        "isbn": 1,
        "pageCount": 1,
        "publishedDate": 1,
        "thumbnailUrl": 1,
        "longDescription": 1,
        "status": 1,
        "authors": 1,
        "categories": 1,
        "year": {"$year": "$publishedDate"},
        "month": {"$month": "$publishedDate"},
        "day": {"$dayOfMonth": "$publishedDate"}
    }},
    {"$match": {
        "year": {"$gt": 2009}
    }},
    {"$limit": 20}
])))

pprint(list(client["sample"]["books"].aggregate([
    {"$project": {
        "title": 1,
        "isbn": 1,
        "pageCount": 1,
        "publishedDate": 1,
        "thumbnailUrl": 1,
        "longDescription": 1,
        "status": 1,
        "categories": 1,
        "year": {"$year": "$publishedDate"},
        "authors_count": {"$size": "$authors"},
        "author1": {"$arrayElemAt": ["$authors", 0]},
        "author2": {"$arrayElemAt": ["$authors", 1]},
        "author3": {"$arrayElemAt": ["$authors", 2]},
    }},
    {"$match": {
        "year": {"$gt": 2009}
    }},
    {"$sort": {
        "publishedDate": 1
    }},
    {"$limit": 20}
])))
pprint(list(client["sample"]["books"].aggregate([
    {"$project": {
        "title": 1,
        "publishedDate": 1,
        "first_author": {"$arrayElemAt": ["$authors", 0]},
    }},
    {"$group": {
        "_id": "$first_author",
        "count": {"$sum": 1}
    }},
    {"$sort": {
        "count": -1
    }},
    {"$limit": 10}
])))
