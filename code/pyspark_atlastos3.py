import sys
import json
import logging
import boto3
import pyspark
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get parameters
args = getResolvedOptions(
    sys.argv, 
    ['JOB_NAME', 'BUCKET_NAME', 'COLLECTION_NAME', 'DATABASE_NAME', 'OUTPUT_PREFIX', 'OUTPUT_FILENAME', 'PREFIX', 'REGION_NAME', 'SECRET_NAME']
)

# Initialize Glue and Spark context
sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to get MongoDB credentials from Secrets Manager
def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secrets_json = json.loads(secret)
            return secrets_json['USERNAME'], secrets_json['PASSWORD'], secrets_json['SERVER_ADDR']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise e

# Retrieve MongoDB credentials
user_name, password, server_addr = get_secret(args['SECRET_NAME'], args['REGION_NAME'])

# MongoDB Atlas connection URI
uri = f"mongodb+srv://{server_addr}.mongodb.net/?retryWrites=true&w=majority"
read_mongo_options = {
    "connection.uri": uri,
    "database": args['DATABASE_NAME'],
    "collection": args['COLLECTION_NAME'],
    "username": user_name,
    "password": password
}

# Read data from MongoDB Atlas
ds = glueContext.create_dynamic_frame_from_options(connection_type="mongodb", connection_options=read_mongo_options)
logger.info("Data read from MongoDB Atlas")

# Write DynamicFrame to S3 with a temporary directory
temp_output_path = f"s3://{args['BUCKET_NAME']}/{args['OUTPUT_PREFIX']}/temp/"
glueContext.write_dynamic_frame.from_options(ds, connection_type="s3", connection_options={"path": temp_output_path}, format="json")
logger.info(f"Data written to temporary S3 path at {temp_output_path}")

# Renaming created file
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

try:
    data = s3_client.list_objects(Bucket=args['BUCKET_NAME'], Prefix=f"{args['OUTPUT_PREFIX']}/temp/")
    if 'Contents' not in data:
        logger.warning(f"No objects found with prefix: {args['OUTPUT_PREFIX']}/temp/")
    else:
        # Loop in S3 bucket to find the right object
        for obj in data['Contents']:
            old_key = obj['Key']
            new_key = f"{args['OUTPUT_PREFIX']}/{args['OUTPUT_FILENAME']}"
            copy_source = {'Bucket': args['BUCKET_NAME'], 'Key': old_key}
            s3_resource.Object(args['BUCKET_NAME'], new_key).copy(copy_source)
            s3_client.delete_object(Bucket=args['BUCKET_NAME'], Key=old_key)
            logger.info(f"Renamed file to {new_key}")
except ClientError as e:
    logger.error(f"Error interacting with S3: {str(e)}")
except Exception as e:
    logger.error(f"An unexpected error occurred: {str(e)}")

# Commit the job
job.commit()
