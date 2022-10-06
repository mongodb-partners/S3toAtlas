# ETL with MongoDB Atlas and AWS Glue Studio (through Pyspark scripts)

## Introduction
The amount of information is growing minute by minute and storing the volumes of data is paramount for any analytics or business intelligence. Enterprises are now generating the DataLake to consolidate all their federated data to a single location. 
	
The ETL (Extract Transform and Load) process is key functionality to having a well-structured process for the data lake. 
	
AWS provides various services for data transfer and AWS Glue is the prime service for their ETL offering. AWS Glue studio is also made available to have a graphical user interface to ease the ETL process.

In this document, we will demonstrate how to integrate MongoDB Atlas with the AWS Glue services. We will show a practical guide for loading the data from S3 through AWS Glue Crawler, Mapping, and Data Catalog services to MongoDB Atlas.
	
This can be extended to any of the source connectors of AWS GLue like CSV, XLS, Text, RDBMS, Stream data, etc.

This article is to demonstrate the capabilities of MongoDB Atlas and AWS Glue Studio Integration.


## [MongoDB Atlas](https://www.mongodb.com/atlas)

MongoDB Atlas is an all purpose database having features like Document Model, Geo-spatial , Time-seires, hybrid deployment, multi cloud services. It evolved as "Developer Data Platform", intended to reduce the developers workload on development and management the database environment. It also provide a free tier to test out the application / database features.


## [AWS Glue Studio](https://docs.aws.amazon.com/glue/latest/ug/what-is-glue-studio.html)
AWS Glue Studio is a new graphical interface that makes it easy to create, run, and monitor extract, transform, and load (ETL) jobs in AWS Glue. You can visually compose data transformation workflows and seamlessly run them on AWS Glue’s Apache Spark-based serverless ETL engine. You can inspect the schema and data results in each step of the job.

## Integration Features

With AWS Glue Studio, we can now create scripts for integrations with all the data source. In this module, we utilized the MongoDB Atlas's Spark connectors to connect to the MongoDB Atlas.


## Steps for Integration

### 1.Set up the MongoDB Atlas cluster

Please follow the [link](https://www.mongodb.com/docs/atlas/tutorial/deploy-free-tier-cluster) to setup a free cluster in MongoDB Atlas

Configure the database for [network security](https://www.mongodb.com/docs/atlas/security/add-ip-address-to-list/) and [access](https://www.mongodb.com/docs/atlas/tutorial/create-mongodb-user-for-cluster/).


### 2.Upload the sample JSON file to S3 bucket

Upload the sample [airport.json](https://github.com/mongodb-partners/S3toAtlas/blob/main/code/airports.json) file to the S3 bucket
![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/13.S3%20upload.png)


### 3.Create a Glue Studio Job and run

Login to [AWS Console](https://aws.amazon.com/console/)


Search for AWS Glue Studio and select from the dropdown (Features --> AWS Glue Studio)

![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/33.AWS%20Glue%20Studio%20Search.png)



Click on the Job from the menu and select "Spark script editor"


Click "Create Job"

![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/28.create%20a%20job.png)


Copy the Code from the [link](https://github.com/mongodb-partners/S3toAtlas/blob/main/code/pyspark_s3toatlas.py) and paste 


Update the code for S3 bucket details (#ds) , MongoDB Atlas Connection  (#mongo_uri), Database, collection, Username and Password details


![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/29.copy%20the%20code.png)




Configure the parameters in "Job details" tab

Update the following parameters:

a. Name 

b. IAM Role

You can keep the default values for all other parameters.


![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/30.update%20the%20job%20details.png)

![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/31.update%20the%20job%20details.png)



Save the job and click "Run" on the top right.

Click on the "Runs" tab and ensure the job ran successfully. You can refer the logs in the "Runs" tab for troubleshooting


### 4.Validate the Data in MongoDB Atlas

Validate the S3 data are created as a document in MongoDB Atlas

![](https://github.com/Babusrinivasan76/atlasgluestudiointegration/blob/main/images/VPC%20Creation/32.validat%20the%20MongoDB%20data.png)


## Summary

Hope this technical guide helped you in migrating the data into the MongoDB Atlas cluster using the AWS Glue services.

This solution can be extended to other data sources through AWS Glue Studio - Catalog service. Refer [link](https://github.com/Babusrinivasan76/atlasgluestudiointegration).

For any assistance please reach out to partners@mongodb.com
