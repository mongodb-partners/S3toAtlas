import sys
import json
import logging
import boto3
import pyspark

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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Read from the S3 bucket ###

ds = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options={"paths": ["s3://xxxx/customer_activity.json"]}, format="json", format_options={}, transformation_ctx = "")

#### MongoDB Atlas Connection ###

mongo_uri = "mongodb://XXXXX.frzascf.mongodb.net:27017,xxxxxxx.frzascf.mongodb.net:27017,xxxxxxxx.frzascf.mongodb.net:27017/?ssl=true&replicaSet=atlas-br364o-shard-0&authSource=admin&retryWrites=true&w=majority"

logger = glueContext.get_logger()
logger.info("Connecting...")


write_mongo_options = {
    "uri": mongo_uri,
    "database": "<databasename>",    ## update the Database name ##
    "collection": "<collection name>",   ## update the Collection name ##
    "username": "<database username>",   ## update the Database user name ##
    "password": "<database password>"  ## update the password  ##
}

# Write DynamicFrame to MongoDB and DocumentDB
glueContext.write_dynamic_frame.from_options( ds, connection_type="mongodb", connection_options= write_mongo_options)


print("written to Atlas!")
