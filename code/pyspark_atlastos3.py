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

## setup the MongoDB Credentials : update the Secret Name and Region ###
def get_secret():

    secret_name = $SECRET_NAME
    region_name = $REGION_NAME

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
            

BUCKET_NAME=$BUCKET_NAME
PREFIX=$PREFIX


user_name, password, server_addr = get_secret()

#### MongoDB Atlas Connection ###. 

uri = "mongodb+srv://{}.mongodb.net/?retryWrites=true&w=majority'".format(server_addr) 

read_mongo_options = {
    "uri": uri,
    "database":$DATABASE_NAME,    
    "collection": $COLLECTION_NAME,   
    "username": user_name,   
    "password": password  
}

## Read from the MongoDB Atlas
ds = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options= read_mongo_options)

logger = glueContext.get_logger()
logger.info("Connecting...")

# Write DynamicFrame to  s3 bucket
glueContext.write_dynamic_frame.from_options(ds, connection_type = "s3", connection_options={"path": "s3://<<BUCKET_NAME>>/<<PREFIX>>"}, format="json")

logger.info("written to S3!")


#Renaming created file
import boto3
BUCKET_NAME='glue-partner'
PREFIX='atlas_data/'

conn = boto3.client('s3')  # again assumes boto.cfg setup, assume AWS S3
data = conn.list_objects(Bucket=BUCKET_NAME, Prefix=PREFIX, Delimiter='/')

#Loop in S3 bucket to find the right object
for objects in data['Contents']:
    print(objects['Key'])
    if objects['Key'].startswith(PREFIX + 'run-unnamed'):
        print('Key', objects['Key'])
        s3_resource = boto3.resource('s3')
        copy_source = {
            'Bucket': BUCKET_NAME,
            'Key': objects['Key']
        }
        s3_resource.Object(BUCKET_NAME, 'dumps_bucket/new_data.json').copy(copy_source)
        conn.delete_object(Bucket=BUCKET_NAME, Key=objects['Key'])
