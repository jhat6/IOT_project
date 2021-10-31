# -*- coding: utf-8 -*-
"""
Created on Sun May 16 16:04:24 2021

Simple example script to connect to an MQTT broker and read data from a topic.

Sources:
    http://www.steves-internet-guide.com/publishing-messages-mqtt-client/
    https://diyi0t.com/microcontroller-to-raspberry-pi-wifi-mqtt-communication/

@author: jeffa
"""

import paho.mqtt.client as mqtt
import matplotlib.pyplot as plt
from time import ctime
from yaml import SafeLoader

# read configuration info
with open('IOT_config.yml', "r") as f:
    cfg = yaml.load(f, Loader=SafeLoader)

BROKER_ADDRESS = cfg[0]['broker']['broker_ip']
USER_NAME = cfg[0]['broker']['user_name']
PASSWORD = cfg[0]['broker']['password']

tempTOPIC = 'home/office/temp'
humidityTOPIC = 'home/office/humidity'
lightTOPIC = 'home/office/light'
piCPULoad = 'home/RaspPi/CPULoadPer'


def on_connect(client, userdata, flags, rc):
    """ callback function executed when the CONNACK response is received     
    from broker"""    
    print('Connected with result code ' + str(rc))
    client.subscribe([(tempTOPIC, 0), (humidityTOPIC, 0), (lightTOPIC, 0), \
                      (piCPULoad, 0)])


def on_message(client, userdata, msg):
    """Callback executed when a message is received from the broker"""
    print(ctime() + ' ' + msg.topic + ' ' + str(msg.payload))    
    

def main():
    clientDesktop = mqtt.Client()
    clientDesktop.username_pw_set(USER_NAME, PASSWORD)
    clientDesktop.on_connect = on_connect
    clientDesktop.on_message = on_message

    clientDesktop.connect(BROKER_ADDRESS, 1883)
    clientDesktop.loop_forever()


if __name__ == '__main__':
    print('MQTT Subscribe to data from R4')
    main()
