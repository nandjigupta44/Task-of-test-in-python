from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["test_glasses"]
pl_table = db["pl"]
pdp_table = db["pdp"]
