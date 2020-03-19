# Import libraries
import numpy as np
import pandas as pd
import json
from utils import *
import folium


# data = 'camera-data.json / weather-data / weather-stations
class TrafficCrawler():
	"""
	Create database of images and build Finland map that contains camera and weather stations
	"""
	def __init__(self, data_camera, data_weather, data_weather_stations,data_camera_stations):
		"""
		The constructor of the class -intiate the class attributes:
		data_camera, data_weather, data_weather_stations,data_camera_stations -- loaded data from json files
		weatherdf -- dataframe that contains specific sensors informations
		weatherStations_map -- weather stations dataframe :id, location and stations sensors (to build the map)
		sensors_data -- dataframe that contains all sensors with their respective coordinates
		cameraStations -- dataframe that contains informations related to the camera_ids, the road number, nearestWeatherStationId and cameraStationId
		conditiondf -- dataframe that associate weather and road conditions (going to be processed) to every weather station
		"""
		self.data_camera = data_camera
		self.data_weather = data_weather
		self.data_weather_stations = data_weather_stations
		self.data_camera_stations = data_camera_stations
		self.weatherdf = sensors_values(self.data_weather)
		self.weatherStations_map, self.sensors_data = weather_stations(self.data_weather_stations, self.data_weather, self.weatherdf)
		self.cameraStations, self.cameraStations_map = camera_stations(self.data_camera,self.data_camera_stations)
		self.conditiondf = weather_road_conditions(self.data_weather, self.weatherdf)

	def vote_roadCondition(self, road_condition3):
		"""
		Vote between different road conditions provided by sensors of similar types

		Arguements:
		road_condition3 -- list that contains combined results of road_conditions sensors

		Return:
		vote_condition -- list of the voted road condition 
		"""
		vote_condition = []
		for i in range(len(self.conditiondf)):
			if type(road_condition3[i])==str:
				road_condition=road_condition3[i].split(' / ')
				if len(road_condition)==1:
					vote_condition.append(road_condition[0])
				else:
					occur = [road_condition.count(cond) for cond in road_condition]
					if len(list(set(occur))) == 1:
						weights = [road_condition_weights[cond] for cond in road_condition]
						vote_condition.append(list(road_condition_weights.keys())[list(road_condition_weights.values()).index(max(weights))])
					else:
						vote_condition.append(road_condition[occur.index(max(occur))])
			else:
				vote_condition.append(np.nan)
		return vote_condition

	def vote_weatherCondition(self):
		"""
		Vote between different weather conditions provided by sensors of similar types

		Return:
		vote_weather -- list of the voted weather condition 
		"""
		vote_weather = []
		for i in range(len(self.conditiondf)):
			if type(self.conditiondf.iloc[i]['weather_condition'])==str:
				weather_condition=self.conditiondf.iloc[i]['weather_condition'].split(' / ')
				if len(weather_condition)==1:
					if weather_condition[0] not in list(weather_condition_weights.keys()):
						if weather_condition[0] == 'snow/sleet' or weather_condition[0] == 'Rain':
							vote_weather.append('Weak '+weather_condition[0].lower())
						else:
							vote_weather.append(np.nan)
					else:
						vote_weather.append(weather_condition[0])
				else:
					varia = [j in list(weather_condition_weights.keys()) for j in weather_condition]
					if sum(varia)> 0:
						if sum(varia) == 1:
							vote_weather.append(weather_condition[varia.index(True)])
						else:
							weights = [weather_condition_weights[cond] for cond in weather_condition]
							vote_weather.append(list(weather_condition_weights.keys())[list(weather_condition_weights.values()).index(max(weights))])
					elif weather_condition[1].title()+' '+weather_condition[0].lower() in list(weather_condition_weights.keys()):
						vote_weather.append(weather_condition[1].title()+' '+weather_condition[0].lower())
					else:
						vote_weather.append(np.nan)
			else:
				vote_weather.append(np.nan)
		return vote_weather


	def build_map(self,path=None,save_map = True):
		"""
		Build and save Suomi map and put pins in the locations of weather stations and camera stations
		Also search for the cameras nearby located sensors (radius 200m) and sensors nearby located cameras
		"""
		id_cameraStation = self.cameraStations_map['id_cameraStation'].tolist()
		id_weatherStation = self.weatherStations_map['weatherStationId'].tolist()
		sensors_nearby_200m = nearby(self.cameraStations_map, self.weatherStations_map, id_weatherStation)
		cameras_nearby_200m = nearby(self.weatherStations_map, self.cameraStations_map, id_cameraStation)
	
		sensors_nearby_camera = pd.DataFrame(list(zip(id_cameraStation, sensors_nearby_200m)), columns = ['id_cameraStation','sensors_nearby(radius_200m)'])
		cameras_nearby_sensors = pd.DataFrame(list(zip(id_weatherStation, cameras_nearby_200m)), columns = ['id_weatherStation','cameras_nearby(radius_200m)'])
		if save_map & path:
			Finland_COORDINATES = (63.2467777,25.9209164)
			# Create an empty map zoomed in Finland
			map = folium.Map(location = Finland_COORDINATES, zoom_start= 9)
			cluster = folium.plugins.MarkerCluster(name ='Cameras').add_to(map)
			# Adding a marker for every record in the filtered data, using a cluster view
			for each in self.cameraStations_map.iterrows():
				folium.Marker(
				  location = [each[1]['latitude'],each[1]['longitude']], popup='<b>id_cameraStation: </b>%s<br></br><b>cameraPresets: </b>%s<br></br><b>nearestWeatherStationId: </b>%s<br></br><b>sensors nearby 200m: </b>%s<br></br>'
				  %(each[1]['id_cameraStation'],each[1]['cameraPresets'],each[1]['nearestWeatherStationId'],sensors_nearby_camera[sensors_nearby_camera['id_cameraStation']==each[1]['id_cameraStation']]['sensors_nearby(radius_200m)'].iloc[0])
				,tooltip='<b>Camera_Station</b>' ,icon=folium.Icon(color='red',icon='camera')).add_to(cluster)
			for each in self.weatherStations_map.iterrows():
				folium.Marker(
				  location = [each[1]['latitude'],each[1]['longitude']], popup='<b>weatherStationId: </b>%s<br></br><b>stationSensors: </b>%s<br></br><b>cameras nearby 200m: </b>%s<br></br>'
				  %(each[1]['weatherStationId'],each[1]['stationSensors'],cameras_nearby_sensors[cameras_nearby_sensors['id_weatherStation']==each[1]['weatherStationId']]['cameras_nearby(radius_200m)'].iloc[0]),tooltip= '<b>Weather_Station</b>' ,icon=folium.Icon(icon='cloud')).add_to(cluster)
			map.save(path+'Finland_stations_map.html')
		return sensors_nearby_camera, cameras_nearby_sensors

	def build_dataset(self):
		"""
		Generate 'images_database' dataframe that contains: information about the images(image_url,image_name), road condition, weather condition... 

		Return:
		images_database -- dataframe
		"""
		def road_conditions_sameLocated_weatherStations():
			"""
			Check the weather and road conditions of the same located weather stations

			Return:
			road_condition3 -- list of combined conditions (candidates for the vote section)
			"""
			road_condition2 = []
			for ro in range(len(self.conditiondf)):
				if self.conditiondf.iloc[ro]['nearestWeatherStationId']in id1:
					variable1 = self.conditiondf[self.conditiondf['nearestWeatherStationId']==(id2[id1.index(self.conditiondf.iloc[ro]['nearestWeatherStationId'])])]['road_condition'].values
					if variable1:
						road_condition2.append(variable1[0])
					else:
						road_condition2.append(np.nan)

				elif self.conditiondf.iloc[ro]['nearestWeatherStationId']in id2:
					variable1 = self.conditiondf[self.conditiondf['nearestWeatherStationId']==(id1[id2.index(self.conditiondf.iloc[ro]['nearestWeatherStationId'])])]['road_condition'].values
					if variable1:
						road_condition2.append(variable1[0])
					else:
						road_condition2.append(np.nan)
				else:
					road_condition2.append(np.nan)
			self.conditiondf['road_condition2'] = road_condition2
			self.conditiondf['road_condition2'] = self.conditiondf['road_condition2'].replace(np.nan,'undefined')
			self.conditiondf['road_condition'] = self.conditiondf['road_condition'].replace(np.nan,'undefined')

			road_condition3 = []
			for j in range(len(self.conditiondf)):
				x = [self.conditiondf.iloc[j]['road_condition'],self.conditiondf.iloc[j]['road_condition2']]
				new_condition = ' / '.join(np.array(x) [[i!= 'undefined' for i in x]])
				if new_condition:
					road_condition3.append(new_condition)
				else:
					road_condition3.append(np.nan)
			return road_condition3
		cameraPres = [dico['cameraPresets'] for dico in self.data_camera['cameraStations']]
		cameraPres = [d[i] for d in cameraPres for i in range(len(d))]
		cameraPresets = pd.DataFrame(cameraPres)
		cameraPresets.rename(columns= {'id':'id_camera'}, inplace= True)
		cameraPresets['id_cameraStation'] = cameraPresets['id_camera'].apply( lambda x : x[:6])
		camera_dataset = cameraPresets.merge(self.cameraStations[['id_cameraStation','nearestWeatherStationId']], on="id_cameraStation", how = 'inner')
		weather_group = self.weatherStations_map.groupby(by=['latitude', 'longitude'])
		id1=[]
		id2=[]
		for ar in weather_group.indices.values():
			ar = ar.tolist()
			if len(ar)==2:
				id1.append(self.weatherStations_map.iloc[ar[0]]['weatherStationId'])
				id2.append(self.weatherStations_map.iloc[ar[1]]['weatherStationId'])
		road_condition3 = road_conditions_sameLocated_weatherStations()
		vote_condition = self.vote_roadCondition(road_condition3)
		self.conditiondf['vote_roadCondition'] = vote_condition
		self.conditiondf['weather_condition']=self.conditiondf['weather_condition'].str.replace('Moderate', 'Mediocre').str.replace('Light','Weak').str.replace('Abundant','Heavy').str.replace('Drizzle','Weak rain').str.replace('Hails','Heavy snow/sleet').str.replace('Dry','Clear').str.replace('Snowfall','snow/sleet')
		self.conditiondf['weather_condition'] = self.conditiondf['weather_condition'].apply(weather_intensity)
		vote_weather = self.vote_weatherCondition()
		self.conditiondf['vote_weatherCondition'] = vote_weather
		self.conditiondf.drop(columns=['road_condition','road_condition2','weather_condition'],inplace=True)
		images_database = camera_dataset.merge(self.conditiondf, on='nearestWeatherStationId', how = 'inner')
		images_database['measuredTime'] = pd.to_datetime(images_database['measuredTime'])
		images_database['vote_roadCondition'].fillna('undefined',inplace=True)
		images_database['vote_weatherCondition'].fillna('undefined',inplace=True)
		images_database['measuredTime'].fillna('undefined',inplace=True)
		images_database['image_name'] = images_database.apply(lambda x:"{}_r{}_w{}_{}".format(x['id_camera'],road_dic.get(x['vote_roadCondition'],'nan'),weather_dic.get(x['vote_weatherCondition'],'nan'),
			str(x['measuredTime']).replace(' ','_').split('+')[0].replace(':','-')),axis=1)
		return images_database
