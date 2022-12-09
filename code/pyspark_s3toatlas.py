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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SECRET_NAME', 'REGION_NAME', 'BUCKET_NAME', 'DATABASE_NAME', 'COLLECTION_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## setup the MongoDB Credentials ###
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
            
user_name, password, server_addr = get_secret()

## Read from the S3 bucket ###.  ## UPDATE the S3 bucket name ##

ds = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options={"paths": ["s3://{}/airports.json".format(args['BUCKET_NAME'])]}, format="json", format_options={}, transformation_ctx = "")

#### MongoDB Atlas Connection ###. 
mongo_uri = "mongodb+srv://{}.mongodb.net/?retryWrites=true&w=majority".format(server_addr) 


logger = glueContext.get_logger()
logger.info("Connecting...")

write_mongo_options = {
    "uri": mongo_uri,
    "database": args['DATABASE_NAME'],
    "collection":args['COLLECTION_NAME'],
    "username": user_name,   
    "password": password  
}

# Write DynamicFrame to MongoDB and DocumentDB
glueContext.write_dynamic_frame.from_options( ds, connection_type="mongodb", connection_options= write_mongo_options)


print("written to Atlas!")
