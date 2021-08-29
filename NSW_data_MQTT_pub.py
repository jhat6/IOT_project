# -*- coding: utf-8 -*-
"""
Created on Sat Aug 28 14:07:08 2021

https://www.weather.gov/documentation/services-web-api#/default/get_alerts_active_area__area_

@author: jeffa
"""

import requests
import pandas as pd
import json
import numpy as np
import paho.mqtt.client as mqtt
import time


#NOAA access token for future reference
#Token = 'dIWmnfJOIfzguEznVvhCMCqTtJSuhUoY'

#Concord, NC Station ID
STATIONID = 'KJQF'

# MQTT broker info
BROKER_IP = "192.168.50.48"
PORT = 1883
USER_NAME = ''
PASSWORD = ''


def on_connect(client, userdata, flags, rc):
    """ callback function executed when the CONNACK response is received     
    from broker"""    
    print('Connected with result code ' + str(rc))    

# publish data
def on_publish(client, userdata, result):
    """ Callback function for publishing """            
    print("data published \n")
    pass
            
# Set client 
clientDesktop = mqtt.Client("Local")                           
clientDesktop.username_pw_set(USER_NAME, PASSWORD)
clientDesktop.on_connect = on_connect
# On Publish Callback function
clientDesktop.on_publish = on_publish                          
# Connect to Broker
clientDesktop.connect(BROKER_IP, PORT)                           

# set delay time for publishing 
delay = 3600

while True:
    
    #Get latest weather data from NWS
    url = 'https://api.weather.gov/stations/' + STATIONID + '/observations/latest'
    latestRequest = requests.get(url)
    # Decode json
    latestData = json.loads(latestRequest.text)
    
    # Extract data of interest
    outdoorTemp = int(np.round(latestData['properties']['temperature']['value']*9/5+32))
    dewPoint = int(np.round(latestData['properties']['dewpoint']['value']*9/5+32))
    windSpeed = np.round(latestData['properties']['windSpeed']['value'], decimals=1)
    windDirection = latestData['properties']['windDirection']['value']
    humidity = int(np.round((latestData['properties']['relativeHumidity']['value'])))
    barometer = np.round(latestData['properties']['barometricPressure']['value']/1000, decimals=1)
    precipitationLast6Hours = latestData['properties']['precipitationLast6Hours']['value']    
    
    # Publish data to MQTT broker
    ret = clientDesktop.publish("home/NWS/outdoorTemp", str(outdoorTemp))
    print("Temperature:", outdoorTemp, 'F')
    
    ret = clientDesktop.publish("home/NWS/dewPoint", str(dewPoint))
    print("Dewpoint:", dewPoint, 'F')
    
    ret = clientDesktop.publish("home/NWS/windSpeed", str(windSpeed))
    print("Wind Speed:", windSpeed, 'kph')
    
    ret = clientDesktop.publish("home/NWS/windDirection", str(windDirection))
    print("Wind Direction:", windDirection, 'Deg')
    
    ret = clientDesktop.publish("home/NWS/humidity", str(humidity))
    print("Humidity:", humidity, '%')
    
    ret = clientDesktop.publish("home/NWS/barometer", str(barometer))
    print("Barometer:", barometer, 'kpa')
    
    # publish heat index only in valid temp range
    if outdoorTemp >= 80:
        heatIndex = int(np.round(latestData['properties']['heatIndex']['value'])*9/5+32)
        ret = clientDesktop.publish("home/NWS/heatIndex", str(heatIndex))
        print("Heat Index:", heatIndex, 'F')
        
    # publish wind chill only in valid temp range
    if outdoorTemp <= 40:
        windChill = int(np.round(latestData['properties']['windChill']['value'])*9/5+32)
        ret = clientDesktop.publish("home/NWS/windChill", str(windChill))
        print("Heat Index:", windChill, 'F')
        
    if precipitationLast6Hours is None:
        # convert NoneType to 0
        precipitationLast6Hours = 0
    else:
        # convert m to cm if not NoneType
        precipitationLast6Hours = 100*precipitationLast6Hours
    
    ret = clientDesktop.publish("home/NWS/precipitationLast6Hours", str(precipitationLast6Hours))
    print("Precipitation (6 hrs):", precipitationLast6Hours, 'cm')
    
    time.sleep(delay)    
    
#%%
#url = 'https://api.weather.gov/alerts/active/area/NC'

#r = requests.get(url)
#d = json.loads(r.text)
