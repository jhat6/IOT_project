import re
from typing import NamedTuple
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import yaml
from yaml import SafeLoader

# read configuration info
with open('IOT_config.yml', "r") as f:
    cfg = yaml.load(f, Loader=SafeLoader)
    
# set broker connection info
MQTT_ADDRESS = cfg[0]['broker']['broker_ip']
MQTT_PORT = cfg[0]['broker']['port']
MQTT_USER = cfg[0]['broker']['user_name']
MQTT_PASSWORD = cfg[0]['broker']['password']
MQTT_TOPIC = 'home/+/+'
MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = 'MQTTInfluxDBBridge'

# Database connection info.
INFLUXDB_ADDRESS = cfg[1]['database']['db_ip']
INFLUXDB_USER = cfg[1]['database']['db_user_name']
INFLUXDB_PASSWORD = cfg[1]['database']['db_password']
INFLUXDB_DATABASE = cfg[1]['database']['db_name']

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

class SensorData(NamedTuple):
    location: str
    Measurement: str
    value: float
    
def on_connect(client, userdata, flags, rc):
    """The callback for when the client receives a CONNACK response from the server."""
    print("Connected with result code" + str(rc))
    client.subscribe(MQTT_TOPIC)
    
def _parse_mqtt_message(topic, payload):
    match = re.match(MQTT_REGEX, topic)
    if match:
        location = match.group(1)
        measurement = match.group(2)
        if measurement == 'status':
            return None
        return SensorData(location, measurement, float(payload))
    else:
        return None
        

def _send_sensor_data_to_influxdb(sensor_data):
    json_body = [
        {
            'measurement': sensor_data.Measurement,
            'tags': {
                'location': sensor_data.location
                },
            'fields': {
                'value': sensor_data.value
                }
            }
        ]
    try:
        influxdb_client.write_points(json_body)
    except:
        print("INFLUX DB DATA WRITE ERROR")
    
def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic +''+str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        print(sensor_data)
        _send_sensor_data_to_influxdb(sensor_data)
        
def _init_influxdb_database():
    databases = influxdb_client.get_list_database()
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        influxdb_client.create_database(INFLUXDB_DATABASE)
    
    influxdb_client.switch_database(INFLUXDB_DATABASE)
    
def main():
    _init_influxdb_database()
    
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    mqtt_client.connect(MQTT_ADDRESS, MQTT_PORT)
    mqtt_client.loop_forever()
    
if __name__ =='__main__':
    print('MQTT to InfluxDB bridge')
    main()

