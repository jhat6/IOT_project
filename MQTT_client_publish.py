# -*- coding: utf-8 -*-
"""
Created on Sun May 16 16:51:51 2021

Simple example script to publish data to an MQTT broker.  
Uses either a random array or Fibonacci numbers

@author: jeffa
"""

import paho.mqtt.client as mqtt
import time
import psutil
import yaml
from yaml import SafeLoader

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

def on_publish(client, userdata, result):
    """ Callback function for publishing """            
    print("data published \n")
    pass

def publish_disk_usage():
    for part in psutil.disk_partitions():
            usage = psutil.disk_usage(part.mountpoint)
            
            # Add drive letter into the topic
            topic = 'home/officePC/' + part.mountpoint[0] + '_freeDiskSpace'
            ret = clientDesktop.publish(topic, str(usage.free))
            print(time.asctime(), "->" , "Free Disk Space: ", usage.free, "\n")
            time.sleep(2)
            
            # Add drive letter into the topic
            topic = 'home/officePC/' + part.mountpoint[0] + '_totalDiskSpace'
            ret = clientDesktop.publish(topic, str(usage.total))
            print(time.asctime(), "->" , "Total Disk Space: ", usage.total, "\n")
            time.sleep(2)
            
# Set client 
clientDesktop = mqtt.Client("Desktop 2")                           
clientDesktop.username_pw_set(USER_NAME, PASSWORD)
clientDesktop.on_connect = on_connect
# On Publish Callback function
clientDesktop.on_publish = on_publish                          
# Connect to Broker
clientDesktop.connect(BROKER_IP, PORT)                           

publish_disk_usage()

count = 1
# set delay time for publishing 
delay = 10
# set trigger for publishing disk usage once per day
trigger = int(3600*24/delay)
while count >= 1:
    mem = psutil.virtual_memory()
    percentMemUsed = mem[2]
    ret = clientDesktop.publish("home/officePC/percentMemUsed", str(percentMemUsed))
    print(time.asctime(), "->" , "Mem Used %: ", percentMemUsed, "\n")
    time.sleep(delay/2)
    
    cpuLoad = psutil.cpu_percent()
    ret = clientDesktop.publish("home/officePC/CPULoadPer", str(cpuLoad))
    print(time.asctime(), "->" , "CPU Load %: ", cpuLoad, "\n")
    time.sleep(delay/2)    
    
    count+= 1
    
    # Publish disk usage once a day
    if count >=trigger:
        publish_disk_usage()
        count = 1    