# AM-Traffic-Phase2.Iteration2_Task-4
Crawler

In this task I have made a crawler on AWS using Lambda function , S3 , DynamoDB and CloudWatcher package.zip can be found in AWS S3 bucket 'reconai-traffic' : contains the dependencies and scripts to run lambda function map : can be found in the map directory in AWS S3 bucket 'reconai-traffic'

**image_name** : 'camera id'_ r'road_condition'_ w'weather_condition'_ 'measuredTime'

UPDATE: 25/02/2020

Added the handler (sensors_handler.py) of 'LambdaTrafficSensors' lambda function that loads sensors_database.csv from S3 bucket 'reconai-traffic' to save sensors data in DynamoDB table.
