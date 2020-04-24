# Import libraries
import os
import sys
import shutil
import urllib.request
import requests
from bs4 import BeautifulSoup
from data_processing import *


jsons = ['camera-data', 'camera-stations', 'road-conditions',
         'weather-data', 'forecast-sections', 'weather-stations']
def make_soup(url):
    html = urllib.request.urlopen(url).read()
    return BeautifulSoup(html, "lxml")

def downloader(urlfile, ext, file_name, path):
    """
    Download files (Json and Images)

    Arguments:
    urlfile -- the link used for downloading the file
    ext -- the extension of the file
    file_name -- the name of downloaded file
    path -- the path where we are going to save the file
    """
    try:
        req = urllib.request.Request(url=urlfile)
        httpresp = urllib.request.urlopen(req, timeout=10)
        r = requests.get(urlfile, stream=True)
        with open(os.path.join(path, file_name+'.'+ext), 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('url_file', urlfile)
        print("Oups!!!! an exception: ", e, exc_type, exc_tb.tb_lineno)

def get_json_links(section_url):
    """
    Extract the json links

    Arguments:
    section_url -- the url of webpage to crawl
    'https://www.digitraffic.fi/en/road-traffic/'

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
                    downloader(li, 'json', file_name, '/content/json')
                    links.append(li)
                else:
                    pass
            else:
                downloader(li, 'json', file_name, '/content/json')
                links.append(li)
    return links

def load_json(path, file_name):
    """
    Load the downloaded json file

    Arugments:
    file_name -- the name of downloaded file
    path -- the path where the file is saved

    Return:
    data -- loaded data from json file
    """
    with open(os.path.join(path, file_name+'.json')) as json_file:
        data = json.load(json_file)
    return data

get_json_links('https://www.digitraffic.fi/en/road-traffic/')

# ***************** Load Json files ******************
camera_data = load_json('/content/json', 'camera-data')
cameraStations_data = load_json('/content/json', 'camera-stations')
weatherStations_data = load_json('/content/json', 'weather-stations')
weather_data = load_json('/content/json', 'weather-data')

# ******** Parse the json files and extract useful informations*******
# *********(Map , images & sensors databases) *********
traffic_crawler = TrafficCrawler(camera_data, weather_data,
                                 weatherStations_data, cameraStations_data)
traffic_crawler.build_map()
