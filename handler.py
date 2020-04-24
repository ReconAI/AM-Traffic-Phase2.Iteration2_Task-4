# Import libraries
from __future__ import print_function
from io import StringIO
import sys
import pickle
import json
import uuid
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import boto3
import botocore.config
from data_processing import *


jsons = ['camera-data', 'camera-stations', 'road-conditions',
         'weather-data', 'forecast-sections', 'weather-stations']

cfg = botocore.config.Config(retries={'max_attempts': 0})
client = boto3.client('s3', config=cfg)

def file_checker(client, bucket, key):
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            return False
        else:
            raise
def make_soup(url):
    html = urlopen(url).read()
    return BeautifulSoup(html, "lxml")

def save_file_to_s3(bucket, file_name, ext, urlfile):
    """
    Download files (Json and Images)

    Arguments:
    bucket -- s3 bucket name where to save file
    ext -- the extension of the file
    file_name -- the name of downloaded file
    path -- the path where to save the file
    """
    try:
        r = requests.get(urlfile, stream=True)
        r.raw.decode_content = True
        if ext == 'json':
            client.put_object(Body=pickle.dumps(r.json()), Bucket=bucket, Key=file_name+'.'+ext)
        else:
            client.put_object(Body=r.content, Bucket=bucket, Key='images/'+file_name+'.'+ext)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('url_file', urlfile)
        print("Oups!!!! an exception: ", e, exc_type, exc_tb.tb_lineno)

def batch_write(table_name, items, add_key=False):
    """
    write list of items dictionaries into a DynamoDB table

    Arguments:
    table_name -- name of the DynamoDB table to be filled
    items -- list of dictionaries (items)
    add_key -- if a generated primary key need to be added to the items
    """
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table(table_name)
    with db.batch_writer() as batch:
        for item in items:
            if add_key:
                item["elt_id"] = str(uuid.uuid4())
            batch.put_item(Item=item)

def load_json(bucket, file_name):
    """
    Load the downloaded json file

    Arugments:
    bucket -- s3 bucket name from where the json file will be loaded
    path -- the path where the file is saved
    Return:
    data -- loaded data from json file
    """
    s3_clientobj = client.get_object(Bucket=bucket, Key=file_name+'.json')
    s3_clientdata = s3_clientobj['Body'].read()
    data = pickle.loads(s3_clientdata)
    return data

def get_json_links(section_url):
    """
    Extract the json links

    Arguments:
    section_url -- the url of webpage to crawl 'https://www.digitraffic.fi/en/road-traffic/'

    Return :
    links -- a list of the extracted links
    """
    links = []
    soup = make_soup(section_url)
    for link in soup.find_all('a'):
        li = link.get('href')
        file_name = li.split('/')[-1]
        if file_name in jsons:
            if file_name in ['road-conditions', 'forecast-sections', 'weather-stations']:
                if li.split('/')[-3] == 'v1':
                    save_file_to_s3('reconai-traffic', file_name, 'json', li)
                    links.append(li)
                else:
                    pass
            else:
                save_file_to_s3('reconai-traffic', file_name, 'json', li)
                links.append(li)
    return links

def extract_data(camera_data, cameraStations_data, weatherStations_data, weather_data):
    """
    Parse the json files and extract useful informations (Map , images & sensors databases)
    """
    traffic_crawler = TrafficCrawler(camera_data, weather_data,
                                     weatherStations_data, cameraStations_data)
    images_database = traffic_crawler.build_dataset()
    return images_database, traffic_crawler.sensors_data

def scrape(event, context):
    """
    Handler of the lambda function 'LambdaTraffic':
    Json files are downloaded from the traffic website to s3 bucket 'reconai-traffic'.
    From these files informations are extracted: dataframe of the camera stations images.
    and dataframe of the data of sensors corresponding
    to the weather stations located nearby camera stations.
    Once informations are extracted, the images are downloaded in the directory
    'images' in 'reconai-traffic' bucket and json files are deleted.
    Then the images database is saved in DynamoDB table 'images_database',
    and the sensors data is saved as a csv file in 'reconai-traffic' bucket
    to be used by another lambda function 'LambdaTrafficSensors'.
    """
    if file_checker(client, "reconai-traffic", 'sensors_data.csv'):
        client.delete_object(Bucket="reconai-traffic", Key='sensors_data.csv')

    get_json_links('https://www.digitraffic.fi/en/road-traffic/')
    # ***************** Load Json files ******************
    camera_data = load_json('reconai-traffic', 'camera-data')
    cameraStations_data = load_json('reconai-traffic', 'camera-stations')
    weatherStations_data = load_json('reconai-traffic', 'weather-stations')
    weather_data = load_json('reconai-traffic', 'weather-data')
    images_database, sensors_data = extract_data(camera_data, cameraStations_data,
                                                 weatherStations_data, weather_data)
    weatherStations_keep = images_database['nearestWeatherStationId'].unique().tolist()
    new_sensors_data = sensors_data[sensors_data['weatherStationId'].astype('float64').isin(weatherStations_keep)]
    # ***************** Delete jsons ******************
    for k in jsons:
        client.delete_object(Bucket="reconai-traffic", Key=k+'.json')
    # ***************** Download the images ******************
    for i in range(len(images_database)):
        save_file_to_s3('reconai-traffic', images_database.iloc[i]['image_name'],
                        'jpg', images_database.iloc[i]['imageUrl'])
    # ***************** upload sensors data csv to s3 bucket ******************
    csv_buffer = StringIO()
    new_sensors_data.to_csv(csv_buffer)
    client.put_object(Body=csv_buffer.getvalue(), Bucket='reconai-traffic', Key='sensors_data.csv')
    # ***************** Fill dynamodb *****************************************
    images_database = images_database.astype(str)
    database = images_database.to_dict('records')
    batch_write('images_database', database)
