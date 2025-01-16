#!python3
# python3.6

import random
from ruuvi_decoders import Df3Decoder, Df5Decoder
from json import dumps, loads
from paho.mqtt import client as mqtt_client
from influxdb import InfluxDBClient
import datetime
from datetime import date
from time import gmtime
import logging
import sys

broker = "localhost"
port = 1883
topic_prefix = "ruuvi/D2:99:F0:AD:65:CE"
username = "ruuviuser"
password = "WithRuuviGateway"
influx_host = "localhost"
influx_port = "8086"
influx_user = "ruuvicollector"
influx_password = "Rc20213005#"
influx_database = "ruuvi"

# set logging
fmt = logging.Formatter("%(name)s: %(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(process)d >>> %(message)s")
logger = logging.getLogger("ruuviMQTT")
logFileHandler =logging.FileHandler("ruuviMQTT.log")
logFileHandler.setLevel(logging.DEBUG)
logFileHandler.setFormatter(fmt)
logger.addHandler(logFileHandler)
logConsoleHandler =logging.StreamHandler(stream=sys.stdout)
logConsoleHandler.setLevel(logging.DEBUG)
logConsoleHandler.setFormatter(fmt)
logger.addHandler(logConsoleHandler)

# my sensors
sensors = {"C0:E7:B2:DD:8B:1A" : "Fence",
	"D9:27:7C:28:2F:E6" : "Mobile",
	"EC:07:DA:3E:5F:F2" : "Freezer",
	"D6:EC:67:41:9D:76" : "Fridge" }

just_now =datetime.datetime.now()

# write every this second
writing_time  =60

# initial writing times
last_times = { "Fence" :just_now, "Mobile" : just_now, "Freezer" : just_now, "Fridge" : just_now } 

# initial measurement
measurement = [{"measurement": "ruuvi_measurements",
	"tags": {
		"dataFormat": 0,
		"mac": ""
	},
	"fields": {
		"absoluteHumidity" : 0.00,
		"accelerationAngleFromX" : 0.00,
		"accelerationAngleFromY" : 0.00,
		"accelerationAngleFromZ" : 0.00,
		"accelerationTotal" : 0.00,
		"accelerationX" : 0.00,
		"accelerationY" : 0.00,
		"accelerationZ" : 0.00,
		"airDensity" : 0.00,
		"batteryVoltage" : 0.00,
		"dewPoint" : 0.00,
		"equilibriumVaporPressure" : 0.00,
		"humidity" : 0.00,
		"measurementSequenceNumber" : 0,
		"movementCounter" : 0,
		"pressure" : 0.00,
		"rssi" : 0.00,
		"temperature" : 0.00,
		"txPower" : 0.00

	}
	}
	]

# generate client ID with pub prefix randomly
client_id = f"ruuvimqtt-{random.randint(0, 100)}"
dbClient = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_database)

def write_to_influx(my_sensor = None, data =None):
	if data is None or my_sensor is None:
		return

	try:
#		dbClient.write_points(measurement)
		measurement[0]['tags']['mac'] =my_sensor
#data['mac']
		measurement[0]['fields']['absoluteHumidity'] =data['humidity']
		measurement[0]['fields']['accelerationX'] =data['acceleration_x']
		measurement[0]['fields']['accelerationY'] =data['acceleration_y']
		measurement[0]['fields']['accelerationZ'] =data['acceleration_z']
		measurement[0]['fields']['accelerationTotal'] =data['acceleration']
		measurement[0]['fields']['batteryVoltage'] =data['battery']
		measurement[0]['fields']['humidity'] =data['humidity']
		measurement[0]['fields']['measurementSequenceNumber'] =data['measurement_sequence_number']
		measurement[0]['fields']['movementCounter'] =data['movement_counter']
		measurement[0]['fields']['pressure'] =data['pressure']
#		measurement[0]['fields']['rssi'] =data['rssi']
		measurement[0]['fields']['temperature'] =data['temperature']
		measurement[0]['fields']['txPower'] =data['tx_power']

		logger.debug(f"'{measurement}'")

		dbClient.write_points(measurement)

	except(KeyError):
		logger.error(f"Exception --> {KeyError}")

# there may be other ruuvi devices around and I don't wnat them to mess this script
# so filter out those
def is_my_sensor(mac):
	sensor = None
	try:
		sensor =sensors[mac]
	except(NameError):
		logger.error(f"exception --> {NameError}")

	return sensor

# this is called every time in client.loop_forever when the subscribed message has been read from mq
def on_message(client, userdata, msg):
	logger.debug(f"Received '{msg.payload.decode()}' from '{msg.topic}' topic")
	data = loads(msg.payload.decode())
	data =data.get("data")
	if data is not None:
		try:
			clean_data =data.split("FF9904")[1]

			format = clean_data[0:2]
			data ={}
			decoder =None
			if "03" == format:
				decoder =Df3Decoder()
			else:
				decoder =Df5Decoder()
			
			data = decoder.decode_data(clean_data)
			if data is not None:
				sender = msg.topic.split(topic_prefix +"/")[1]
				my_sensor =is_my_sensor(sender)
				if my_sensor is not None:
					logger.debug(f"Sender = '{my_sensor}', Data = '{data}'")

					my_datenow =datetime.datetime.now()
					my_sensordate =last_times[my_sensor]

					do_write =False
					time_diff =(my_datenow -my_sensordate).total_seconds()
					logger.debug(f"my_datenow ='{my_datenow}', my_sensordate ='{my_sensordate}', time_diff ='{time_diff}")

					if abs(time_diff) >writing_time:
						do_write =True
						last_times[my_sensor] =my_datenow

					if True ==do_write:
						logger.info(f"WRITING TO INFLUX: {data}")
						write_to_influx(my_sensor, data)

		except (AttributeError, ValueError, TypeError):
			logger.error(f"Error --> {msg.payload}")


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logger.debug("Connected to MQTT Broker!")
        else:
            logger.error("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
   client.subscribe(topic_prefix + "/+")
   client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()

