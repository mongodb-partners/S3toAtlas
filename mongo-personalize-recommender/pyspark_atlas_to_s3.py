import sys
import json
import logging
import boto3
import pyspark
import csv

###################GLUE import##############
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import *

#### ###creating spark and gluecontext ###############

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BUCKET_NAME', 'COLLECTION_NAME1', 'COLLECTION_NAME2','COLLECTION_NAME3', 'DATABASE_NAME', 'OUTPUT_PREFIX', 'OUTPUT_FILENAME1',  'OUTPUT_FILENAME2', 'PREFIX', 'REGION_NAME', 'SECRET_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## setup the MongoDB Credentials : update the Secret Name and Region ###
def get_secret():

    secret_name = args['SECRET_NAME']
    region_name = args['REGION_NAME']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secrets_json = json.loads(secret)
            return (secrets_json['USERNAME'], secrets_json['PASSWORD'], secrets_json['SERVER_ADDR'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
            

BUCKET_NAME=args['BUCKET_NAME'], 
PREFIX=args['PREFIX'], 


user_name, password, server_addr = get_secret()

#### MongoDB Atlas Connection ###. 

uri = "mongodb+srv://{}.mongodb.net/?retryWrites=true&w=majority".format(server_addr) 


######## Read "ratings" from the MongoDB Atlas & enrich data ###############
read_mongo_options = {
    "connection.uri": uri,
    "database":args['DATABASE_NAME'],
    "collection": args['COLLECTION_NAME1'],   
    "username": user_name,   
    "password": password  
}
ds_ratings = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options= read_mongo_options)

## Transform ratings data
# Rename multiple fields
field_mappings = [("userId", "USER_ID"),
                  ("movieId", "ITEM_ID"),
                  ("timestamp", "TIMESTAMP"),
                 ]

for old_name, new_name in field_mappings:
    ds_ratings = ds_ratings.rename_field(old_name, new_name)

# Define a function to add a new field 'event_type' based on the 'rating' column
def add_event_type(record):
    # Check if 'rating' is less than or equal to 2
    if int(record["rating"]) <= 2:
        record["EVENT_TYPE"] = "click"
    else:
        record["EVENT_TYPE"] = "watch"
    return record

# Apply the mapping function to add the new field 'event_type'
ds_ratings = Map.apply(frame=ds_ratings, f=add_event_type)

# Drop unwanted fields. Final columns => ITEM_ID, TIMESTAMP, USER_ID, EVENT_TYPE
fields_to_drop = ["_id", "rating"]
ds_ratings = ds_ratings.drop_fields(fields_to_drop) 


################# Read "tags" from the MongoDB Atlas. Columns => userId, movieId, tag, timestamp ################
read_mongo_options = {
    "connection.uri": uri,
    "database":args['DATABASE_NAME'],
    "collection": args['COLLECTION_NAME2'],   
    "username": user_name,   
    "password": password  
}
ds_tags = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options= read_mongo_options).toDF()

################# Read "movies" from the MongoDB Atlas. Columns => movieId, title, genres ###############
read_mongo_options = {
    "connection.uri": uri,
    "database":args['DATABASE_NAME'],
    "collection": args['COLLECTION_NAME3'],   
    "username": user_name,   
    "password": password  
}
ds_movies = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options= read_mongo_options).toDF()

################ Join movies and tags on movieId ###############
ds_joined = ds_tags.join(ds_movies, on="movieId", how='left')  
ds_joined = ds_joined.toPandas()
ds_items = ds_joined[["movieId", "genres", "timestamp"]]

ds_items = ds_items.rename(columns={'movieId':'ITEM_ID', 'genres': 'GENRES', 'timestamp': 'CREATION_TIMESTAMP'})

#rename_map = {'movieId':'ITEM_ID', 'genres': 'GENRES', 'timestamp': 'CREATION_TIMESTAMP'}

#for old_name, new_name in rename_map.items():
#    ds_items = ds_items.withColumnRenamed(old_name, new_name)

ds_items = spark.createDataFrame(ds_items)
ds_items = DynamicFrame.fromDF(ds_items, glueContext, "ds_items")

############### Write data to S3 ###############

logger = glueContext.get_logger()
logger.info("Connecting...")

# Write ds_ratings DynamicFrame to  s3 bucket
path = "s3://{}/{}".format(args['BUCKET_NAME'], args['PREFIX'])
glueContext.write_dynamic_frame.from_options(ds_ratings, connection_type = "s3", connection_options={"path": path}, format="csv", format_options = {"writeHeader": "true"})

logger.info("written ds_ratings to S3!")


#Renaming created file
import boto3

conn = boto3.client('s3')  # again assumes boto.cfg setup, assume AWS S3
data = conn.list_objects(Bucket=args['BUCKET_NAME'], Prefix=args['PREFIX'], Delimiter='/')

# Loop in S3 bucket to find the right object
for objects in data['Contents']:
   print(objects['Key'])
   if objects['Key'].startswith(args['PREFIX'] + 'run-'):
       print('Key', objects['Key'])
       s3_resource = boto3.resource('s3')
       copy_source = {
          'Bucket': args['BUCKET_NAME'], 
           'Key': objects['Key']
       }
       s3_resource.Object(args['BUCKET_NAME'], '{}/{}.csv'.format(args['OUTPUT_PREFIX'], args['OUTPUT_FILENAME1'])).copy(copy_source)
       conn.delete_object(Bucket=args['BUCKET_NAME'], Key=objects['Key'])

logger.info('Renamed file as {}/{}'.format(args['OUTPUT_PREFIX'], args['OUTPUT_FILENAME1']))


# Write ds_items DynamicFrame to  s3 bucket
path = "s3://{}/{}".format(args['BUCKET_NAME'], args['PREFIX'])
glueContext.write_dynamic_frame.from_options(ds_items, connection_type = "s3", connection_options={"path": path}, format="csv", format_options = {"writeHeader": "true"})

logger.info("written ds_items to S3!")


#Renaming created file
import boto3

conn = boto3.client('s3')  # again assumes boto.cfg setup, assume AWS S3
data = conn.list_objects(Bucket=args['BUCKET_NAME'], Prefix=args['PREFIX'], Delimiter='/')

# Loop in S3 bucket to find the right object
for objects in data['Contents']:
   print(objects['Key'])
   if objects['Key'].startswith(args['PREFIX'] + 'run-'):
       print('Key', objects['Key'])
       s3_resource = boto3.resource('s3')
       copy_source = {
          'Bucket': args['BUCKET_NAME'], 
           'Key': objects['Key']
       }
       s3_resource.Object(args['BUCKET_NAME'], '{}/{}.csv'.format(args['OUTPUT_PREFIX'], args['OUTPUT_FILENAME2'])).copy(copy_source)
       conn.delete_object(Bucket=args['BUCKET_NAME'], Key=objects['Key'])

logger.info('Renamed file as {}/{}'.format(args['OUTPUT_PREFIX'], args['OUTPUT_FILENAME2']))