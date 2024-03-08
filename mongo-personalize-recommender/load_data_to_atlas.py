from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB Atlas
client = MongoClient("<connection_uri>”)
db = client["movielens"]

# Load movies.csv
movies = pd.read_csv('path_to_extracted_files/movies.csv')
movies_collection = db["movies"]
movies_collection.insert_many(movies.to_dict("records"))

# Load ratings.csv
ratings = pd.read_csv('path_to_extracted_files/ratings.csv')
ratings_collection = db["ratings"]
ratings_collection.insert_many(ratings.to_dict("records"))

# Load tags.csv
tags = pd.read_csv('path_to_extracted_files/tags.csv')
tags_collection = db["tags"]
tags_collection.insert_many(tags.to_dict("records"))

# Load links.csv
links = pd.read_csv('path_to_extracted_files/links.csv')
links_collection = db["links"]
links_collection.insert_many(links.to_dict("records"))

print("Data loaded successfully!")