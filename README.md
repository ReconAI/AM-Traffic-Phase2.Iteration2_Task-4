# AM-Traffic-Phase2.Iteration2_Task-4
Crawler

In this task I have made a crawler on AWS using Lambda function , S3 , DynamoDB and CloudWatcher package.zip can be found in AWS S3 bucket 'reconai-traffic' : contains the dependencies and scripts to run lambda function map : can be found in the map directory in AWS S3 bucket 'reconai-traffic'

**image_name** : 'camera id'_ r'road_condition'_ w'weather_condition'_ 'measuredTime'

UPDATE: 25/02/2020

Added the handler (sensors_handler.py) of 'LambdaTrafficSensors' lambda function that loads sensors_database.csv from S3 bucket 'reconai-traffic' to save sensors data in DynamoDB table.

# Deployment
## Lambda function
**On Local machine**: create package that contains scripts + used python libraries that are installed and packed as follows:

```sh
pip install package – t .
chmod -R 755 .
zip -r ../package.zip .
```
**PS**: Use Python 3.6 or similar version.

**On AWS console**: Upload the zip directly on Lambda console or on S3 bucket. Once uploaded, just save and be sure to check on the handler. Then, just run a test.

**In IAM console manager**: Add required policies to the corresponding roles .
<p align="center">
  <img src="figures/lambda.png">
</p>

## S3 Bucket
**On AWS console**: Just create a new S3 Bucket that will contain all the downloaded images and where you can also load the lambda package.zip

**In IAM console manager**: Add required policies to the corresponding roles.
<p align="center">
  <img src="figures/s3.png">
</p>

## DynamoDB
**On AWS console**: Create tables in the DynamoDB that will contain your Images data and Sensors data.

**In IAM console manager**: Add required policies to the corresponding roles.
<p align="center">
  <img src="figures/DynamoDB.png">
</p>

## Step Function
**On AWS console**: Once you create a State Machine, adjust the workflow 
(eg. Start -> Lambda1 -> Lambda2 -> End)<br/>
Adjust the CloudWatch Cron in order to make the State Machine trigger in specific time.<br/>
*PS*: Timer set on GMT.

**In IAM console manager**: Add required policies to the corresponding roles.
<p align="center">
  <img src="figures/stepFunctions.png">
</p>

## IAM (role manager):
**In IAM console manager**: Add required policies to the corresponding roles .
<p align="center">
  <img src="figures/iam.png">
</p>
