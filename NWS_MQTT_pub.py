# -*- coding: utf-8 -*-
"""
Created on Sat Aug 28 14:07:08 2021

https://www.weather.gov/documentation/services-web-api#/default/get_alerts_active_area__area_

@author: jeffa
"""

import requests
#import pandas as pd
import json
import numpy as np
import paho.mqtt.client as mqtt
import time
import yaml
from yaml import SafeLoader


#NOAA access token for future reference
#Token = 'dIWmnfJOIfzguEznVvhCMCqTtJSuhUoY'

#Concord, NC Station ID
STATIONID = 'KCLT'

# read configuration info
with open('IOT_config.yml', "r") as f:
    cfg = yaml.load(f, Loader=SafeLoader)
    
# set broker connection info
BROKER_IP = cfg[0]['broker']['broker_ip']
PORT = cfg[0]['broker']['port']
USER_NAME = cfg[0]['broker']['user_name']
PASSWORD = cfg[0]['broker']['password']


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

# set delay time between publishing calls
pubDelay = 0.3
# set delay time for executing the while loop
loopDelay = 3600

while True:
    # Connect to Broker
    clientDesktop.connect(BROKER_IP, PORT)                           

    print("\n")
    print(time.localtime(), "\n")
    #Get latest weather data from NWS
    url = 'https://api.weather.gov/stations/' + STATIONID + '/observations/latest'
    latestRequest = requests.get(url)
    # Decode json
    latestData = json.loads(latestRequest.text)

    # list weather variables of interest
    variables = ['temperature', 'dewpoint', 'windSpeed', 'windDirection', 'relativeHumidity',
                'barometricPressure', 'heatIndex', 'windChill', 'precipitationLast6Hours']

    for varName in variables:
        
        value = latestData['properties'][varName]['value']
        
        if value is None:
            value = 0
        elif varName=="temperature" or varName=="dewpoint" or varName=="heatIndex" or varName=="windChill":
            # convert to F
            value = int(np.round(value*9/5+32))
        elif varName=="barometricPressure":
            # convert to kpa
            value = np.round(value/1000, decimals=1)
        elif varName=="windSpeed":
            value = np.round(value, decimals=1)
        elif varName=="precipitationLast6Hours":
            # convert m to cm 
            value = 100*value        
            
        print(varName, value)
        # publish to borker
        ret = clientDesktop.publish("home/NWS/" + varName, str(value))
        # set small delay time between writing to the broker
        time.sleep(pubDelay)    
    
    # delay time between data retreival 
    time.sleep(loopDelay)    
    
#%%
#url = 'https://api.weather.gov/alerts/active/area/NC'

#r = requests.get(url)
#d = json.loads(r.text)