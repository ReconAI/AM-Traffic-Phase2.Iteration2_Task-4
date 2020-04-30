# -*- coding: utf-8 -*-
"""
Contains helper functions.
"""

# Import libraries
import numpy as np
import pandas as pd
from geopy import distance

# Dictionaries of categories for weather and road conditions
weather_dic = {'Clear':0, 'Weak rain':1, 'Mediocre rain':2, 'Heavy rain':3,
               'Weak snow/sleet':4, 'Mediocre snow/sleet':5, 'Heavy snow/sleet':6, 'Mist/Fog':7}

road_dic = {'Dry':0, 'Moist':1, 'Wet':2, 'Wet and salty':3, 'Frost':4,
            'Snow':5, 'Ice':6, 'Probably moist and salty':7, 'Slushy':8}
# Dictionaries of weights for weather and road conditions according to severity
road_condition_weights = {'Frost':0.2, 'Ice':0.18, 'Snow':0.16, 'Slushy':0.14,
                          'Wet and salty':0.1, 'Wet':0.09, 'Moist':0.07,
                          'Probably moist and salty':0.05, 'Dry':0.01}

weather_condition_weights = {'Mist/Fog':0.2, 'Heavy snow/sleet':0.18, 'Mediocre snow/sleet':0.16,
                             'Weak snow/sleet':0.14, 'Heavy rain':0.12, 'Mediocre rain':0.1,
                             'Weak rain':0.08, 'Clear':0.02}

few_conditions = ['Wet sleet', 'Sleet', 'Ice crystals',
                  'Snow grains', 'Graupel', 'Freezing drizzle', 'Freezing rain']
def radius_calc(origStation_point, destStation_points_list, id_destStation):
    """
    Calculate the radius and check if an origin(weather or camera station)
    station is inside a 200m radius of a given destination station

    Arguemnts:
    origStation_point -- tuple of the coordinates (latitude, longitude) of origin station
    destStation_points_list -- list of tuples of the coordinates
    (latitude, longitude) of all destination stations
    id_destStation -- list of the ids of all the destination stations

    Return:
    near -- list of the ids of the destination stations that are located nearby the origin station
    """
    radius = 200 # in meter
    near = []
    for num, point in enumerate(destStation_points_list):
        dis = distance.distance(origStation_point, point).m
        if dis <= radius:
            near.append(id_destStation[num])
        else:
            pass
    return near

def nearby(origStations_map, destStations_map, id_destStation):
    """
    Check for the nearby stations

    Arguments:
    origStations_map -- origin stations dataframe (to build the map)
    destStations_map -- destination stations dataframe (to build the map)
    id_destStation -- list of the ids of all the destination stations

    Return:
    nearby_200m -- list of list of the ids of all the destination stations
    located nearby each origin station
    """
    destStation_points_list = list(zip(destStations_map['latitude'].tolist(),
                                       destStations_map['longitude'].tolist()))
    nearby_200m = []
    for i in range(len(origStations_map)):
        origStation_point = (origStations_map.iloc[i]['latitude'],
                             origStations_map.iloc[i]['longitude'])
        near = radius_calc(origStation_point, destStation_points_list, id_destStation)
        nearby_200m.append(near)
    return nearby_200m

def weather_intensity(x):
    """
    Adapting the sensors descriptions to the categories that will be used

    Arguments:
    x -- weather description

    Return:
    x -- modified weather description (if necessary)
    """
    if type(x) == str:
        if x.find(' / ') != -1:
            intensity = x.split(' / ')[1]
            if intensity in ['Weak', 'Mediocre', 'Heavy']:
                x = x+' rain'
        if x.split(' / ')[0] in few_conditions:
            x = 'snow/sleet'
    return x

def sensors_values(data_weather):
    """
    Extract informations from selected sensors

    Arguments:
    data_weather -- loaded data from 'weather-data.json'

    Return:
    weatherdf -- dataframe that contains specific sensors informations
    """
    weather = data_weather['weatherStations']
    sensors_values = [dico['sensorValues'] for dico in weather]
    sensors_values = [d[i] for d in sensors_values for i in range(len(d))]
    sensors_info = [list(map(dico.get, ['id', 'roadStationId', 'oldName',
                                        'sensorValue', 'sensorUnit',
                                        'sensorValueDescriptionEn',
                                        'measuredTime'])) for dico in sensors_values]
    list_columns = ['id_sensor', 'roadStationId', 'oldName', 'sensorValue',
                    'sensorUnit', 'sensorValueDescriptionEn', 'measuredTime']
    weatherdf = pd.DataFrame(sensors_info, columns=list_columns)
    weatherdf['measuredTime'] = pd.to_datetime(weatherdf['measuredTime'])
    weatherdf.sort_values(by='measuredTime', inplace=True)
    return weatherdf

def weather_stations(data_weather_stations, data_weather, weatherdf):
    """
    Get the informations that are related to the weather stations and their sensors

    Arguments:
    data_weather_stations -- loaded data from 'weather-stations.json'
    data_weather -- loaded data from 'weather-data.json'
    weatherdf -- dataframe that contains specific sensors informations

    Return:
    weatherStations_map -- weather stations dataframe :
    id, location and stations sensors (to build the map)
    sensors_data -- dataframe that contains all sensors with their respective coordinates
    """
    coordinates_weatherStation = list(zip(*[sub_dict['geometry']['coordinates'] for sub_dict in data_weather_stations['features']]))
    ids_weatherStation = [sub_dict['id'] for sub_dict in data_weather_stations['features']]
    sensors_weatherStation = [sub_dict['properties']['stationSensors'] for sub_dict in data_weather_stations['features']]
    coord_weatherStation_dict = {"weatherStationId": ids_weatherStation,
                                 'longitude': list(coordinates_weatherStation[0]),
                                 'latitude': list(coordinates_weatherStation[1]),
                                 "stationSensors":sensors_weatherStation}
    weatherStations_map = pd.DataFrame(coord_weatherStation_dict)
    sensors_data = weatherdf.merge(weatherStations_map[['weatherStationId',
                                                        'longitude', 'latitude']],
                                   left_on='roadStationId',
                                   right_on='weatherStationId')
    sensors_data.drop(columns='roadStationId', inplace=True)
    return weatherStations_map, sensors_data

def camera_stations(data_camera, data_camera_stations):
    """
    Get the informations that are related to the camera stations

    Arguments:
    data_camera -- loaded data from 'data-camera.json'
    data_camera_stations -- loaded data from 'camera-stations.json'

    Return:
    cameraStations_map -- camera stations dataframe :camera_id, location
    and camera_station_id, nearest_weather_stations_id (to build the map)
    cameraStations -- dataframe that contains informations related to the camera_ids,
    the road number, nearestWeatherStationId and cameraStationId
    """
    cameraStations = pd.DataFrame(data_camera['cameraStations'])
    cameraStations['cameraPresets'] = cameraStations['cameraPresets'].apply(lambda x: [dico['id'] for dico in x])
    cameraStations.rename(columns={'id':'id_cameraStation'}, inplace=True)

    coordinates_camera_station = list(zip(*[sub_dict['geometry']['coordinates'] for sub_dict in data_camera_stations['features']]))
    ids_camera_station = [sub_dict['properties']['id'] for sub_dict in data_camera_stations['features']]
    coord_camera_station_dict = {"id_cameraStation": ids_camera_station,
                                 'longitude': list(coordinates_camera_station[0]),
                                 'latitude': list(coordinates_camera_station[1])}
    coord_camera_stationdf = pd.DataFrame(coord_camera_station_dict)
    cameraStations_map = cameraStations.merge(coord_camera_stationdf,
                                              on='id_cameraStation', how='inner')
    return cameraStations, cameraStations_map

def weather_road_conditions(data_weather, weatherdf):
    """
    Generate a dataframe that combine the results of two similar type of sensors related
    to the road and weather conditions for each weather station

    Argument:
    data_weather -- loaded data from 'weather-data.json'
    weatherdf -- dataframe that contains specific sensors informations

    Return:
    conditiondf -- dataframe that associate weather and road conditions (going to be processed)
    to every weather station
    """

    def find_condi(df, sensor1, sensor2):
        """
        Combine the results of two similar type of sensors related to
        the road and weather conditions

        Arguments:
        df -- dataframe (conditiondf) contains values of the two road condition sensors
        for each weather station
        sensor1 -- first sensor
        sensor2 -- second sensor

        Return:
        road_cond -- str of combined sensors values
        """
        if sensor1 in sensors and sensor2 in sensors:
            condi1 = (df[df['oldName'] == sensor1]['sensorValueDescriptionEn']).tolist()[0]
            condi2 = (df[df['oldName'] == sensor2]['sensorValueDescriptionEn']).tolist()[0]
            if condi1 not in ['The sensor has a fault', None] and condi2 not in ['The sensor has a fault', None]:
                if condi1 == condi2:
                    road_cond = condi1
                else:
                    road_cond = condi1+' / '+condi2

            elif condi1 not in ['The sensor has a fault', None]:
                road_cond = condi1
            elif condi2 not in ['The sensor has a fault', None]:
                road_cond = condi2
            else:
                road_cond = np.nan
        elif sensor1 in sensors:
            condi1 = (df[df['oldName'] == sensor1]['sensorValueDescriptionEn']).tolist()[0]
            if condi1 not in ['The sensor has a fault', None]:
                road_cond = condi1
            else:
                road_cond = np.nan
        elif sensor2 in sensors:
            condi2 = (df[df['oldName'] == sensor2]['sensorValueDescriptionEn']).tolist()[0]
            if condi2 not in ['The sensor has a fault', None]:
                road_cond = condi2
            else:
                road_cond = np.nan
        else:
            road_cond = np.nan

        return road_cond

    weatherdf_mod = weatherdf.copy()
    weatherdf_mod['sensorValueDescriptionEn'] = weatherdf_mod['sensorValueDescriptionEn'].replace('Dry weather', 'Dry')
    used_sensors = list(set(weatherdf_mod[pd.notnull(weatherdf_mod['sensorValueDescriptionEn'])]['oldName'].unique().tolist())-set(['warning1', 'warning2', 'warning3']))
    roadStationIdlist = weatherdf_mod['roadStationId'].unique().tolist()
    idgrp = weatherdf_mod.groupby('roadStationId')
    road_condition = []
    weather_condition = []
    for st in roadStationIdlist:
        stgrp = idgrp.get_group(st)
        stgrpnew = stgrp[stgrp['oldName'].isin(used_sensors)]
        sensors = stgrpnew['oldName'].tolist()
        road_condition.append(find_condi(stgrpnew, 'roadsurfaceconditions1',
                                         'roadsurfaceconditions2'))
        weather_condition.append(find_condi(stgrpnew, 'precipitationtype', 'precipitation'))
    conditiondf = pd.DataFrame(list(zip(roadStationIdlist,
                                        road_condition, weather_condition)),
                               columns=['roadStationId', 'road_condition', 'weather_condition'])
    conditiondf.rename(columns={'roadStationId':'nearestWeatherStationId'}, inplace=True)
    conditiondf['nearestWeatherStationId'] = conditiondf['nearestWeatherStationId'].astype('float64')

    return conditiondf
