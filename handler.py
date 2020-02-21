# Import libraries
import boto3
import botocore.config
import json
import uuid
import requests
from bs4 import BeautifulSoup
import datetime
import os
import sys
from urllib.request import urlopen
import pickle
from data_processing import *


jsons = ['camera-data', 'camera-stations', 'road-conditions', 'weather-data', 'forecast-sections', 'weather-stations']

cfg = botocore.config.Config(retries={'max_attempts': 0})
client = boto3.client('s3', config=cfg)

def make_soup(url):
  html = urlopen(url).read()
  return BeautifulSoup(html, "lxml")

def save_file_to_s3(bucket,file_name,ext,urlfile):
  """
  Download files (Json and Images)
  Arguments: 
  urlfile -- the link used for downloading the file
  ext -- the extension of the file
  file_name -- the name of downloaded file
  path -- the path where we are going to save the file
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
  dynamodb = boto3.resource('dynamodb')
  db = dynamodb.Table(table_name)

  with db.batch_writer() as batch:
    for item in items:
      if add_key:
        item["elt_id"] = str(uuid.uuid4())
      batch.put_item(Item=item)

def load_json(bucket,file_name):
  """
  Load the downloaded json file

  Arugments:
  file_name -- the name of downloaded file
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
      if file_name == 'road-conditions' or file_name == 'forecast-sections' or file_name == 'weather-stations':
        if li.split('/')[-3]=='v1':
          save_file_to_s3('reconai-traffic',file_name,'json',li)
          links.append(li)
        else:
          pass
      else:
        save_file_to_s3('reconai-traffic',file_name,'json',li)
        links.append(li)
  return links
def extract_data(camera_data, cameraStations_data, weatherStations_data, weather_data):

  # ***************** Parse the json files and extract useful informations (Map , images & sensors databases) ****************** 
  traffic_crawler = TrafficCrawler(camera_data, weather_data, weatherStations_data, cameraStations_data)
  #sensors_nearby_camera, cameras_nearby_sensors = traffic_crawler.build_map('bucket_url',save_map = False) # Path to edit with bucket/map url
  images_database = traffic_crawler.build_dataset()

  return images_database,traffic_crawler.sensors_data


def scrape(event, context):
  s3 = boto3.resource('s3')
  my_bucket = s3.Bucket('reconai-traffic')
  get_json_links('https://www.digitraffic.fi/en/road-traffic/')
  # ***************** Load Json files ****************** 
  camera_data = load_json('reconai-traffic','camera-data')
  cameraStations_data = load_json('reconai-traffic','camera-stations')
  weatherStations_data = load_json('reconai-traffic','weather-stations')
  weather_data = load_json('reconai-traffic','weather-data')
  images_database, sensors_data = extract_data(camera_data, cameraStations_data, weatherStations_data, weather_data)
  # ***************** Delete jsons ****************** 
  for k in jsons:
    client.delete_object(Bucket="reconai-traffic", Key=k+'.json')
  # ***************** Download the images ****************** 
  for i in range(len(images_database)):
    save_file_to_s3('reconai-traffic', images_database.iloc[i]['image_name'], 'jpg', images_database.iloc[i]['imageUrl'])
  # ***************** Fill dynamodb ****************** 
  images_database = images_database.astype(str)
  sensors_data = sensors_data.astype(str)
  database = images_database.to_dict('records')
  sensors = sensors_data.to_dict('records')
  batch_write('images_database', database)
  batch_write('sensors_database', sensors, add_key=True)



