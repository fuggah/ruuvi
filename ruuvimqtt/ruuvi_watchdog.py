#!python3
# python3.6

import random
from ruuvi_decoders import Df3Decoder, Df5Decoder
import json
from paho.mqtt import client as mqtt_client
import datetime
from datetime import date
from time import gmtime
import logging
from queue import Queue
import sys

__DEBUG__ =True
VAT = 1.24
_confFile = "./config.json"
_uid =None
_pwd =None
_host =None
_port =None
_sensor =None
_operand =None
_operator =None
_value =None
_queue = Queue()

_logging_level =logging.DEBUG
_logfile_name ="ruuvi_watchdog.log"

_sensors = {"C0:E7:B2:DD:8B:1A" : "Fence",
	"D9:27:7C:28:2F:E6" : "Mobile",
	"EC:07:DA:3E:5F:F2" : "Freezer",
	"D6:EC:67:41:9D:76" : "Fridge" }

# initial measurement
_measurement = [{"measurement": "ruuvi_measurements",
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

#def write_to_influx(my_sensor = None, data =None):
#	if data is None or my_sensor is None:
#		return
#
#	try:
##		dbClient.write_points(measurement)
#		measurement[0]['tags']['mac'] =my_sensor
##data['mac']
#		measurement[0]['fields']['absoluteHumidity'] =data['humidity']
#		measurement[0]['fields']['accelerationX'] =data['acceleration_x']
#		measurement[0]['fields']['accelerationY'] =data['acceleration_y']
#		measurement[0]['fields']['accelerationZ'] =data['acceleration_z']
#		measurement[0]['fields']['accelerationTotal'] =data['acceleration']
#		measurement[0]['fields']['batteryVoltage'] =data['battery']
#		measurement[0]['fields']['humidity'] =data['humidity']
#		measurement[0]['fields']['measurementSequenceNumber'] =data['measurement_sequence_number']
#		measurement[0]['fields']['movementCounter'] =data['movement_counter']
#		measurement[0]['fields']['pressure'] =data['pressure']
##		measurement[0]['fields']['rssi'] =data['rssi']
#		measurement[0]['fields']['temperature'] =data['temperature']
#		measurement[0]['fields']['txPower'] =data['tx_power']
#
#		logger.debug(f"'{measurement}'")
#
#		dbClient.write_points(measurement)
#
#	except(KeyError):
#		logger.error(f"Exception --> {KeyError}")

_logger = logging.getLogger(__name__)
logging.basicConfig(level=_logging_level, format ='%(asctime)s: %(name)s: %(levelname)s - %(message)s')
#logging.basicConfig(level=_logging_level, filename = _logfile_name, format ='%(asctime)s: %(name)s: %(levelname)s - %(message)s')

def getMQTTSettings(fle):
	uid =None
	pwd =None
	host =None
	port =None
	sensor =None
	operand =None
	operator =None
	value =None

	_logger.debug(f"Loading MQTT settings from '{fle}'")
	try:
		with open(fle, "r", encoding="utf-8") as f:
			data = json.load(f)
			_logger.debug(f"Loaded data: {data}")
			uid = data.get("uid")
			pwd = data.get("pwd")
			host = data.get("host")
			port = data.get("port")
			sensor =data.get("sensor")
			operand = data.get("operand")
			operator =data.get("operator")
			value = data.get("value")

	except (FileNotFoundError, json.JSONDecodeError) as e:
		_logger.error(f"Error: {e}")

	_logger.debug(f"MQTT settings: uid={uid}, pwd={pwd}, host={host}, port={port}, sensor={sensor}, operand={operand}, operator={operator}, value={value}")
	return uid, pwd, host, port, sensor, operand, operator, value
	
# this is called every time in client.loop_forever when the subscribed message has been read from mq
def on_message(client, userdata, msg):
	try:
		myMsg =msg.payload.decode()
		_logger.debug(f"Received '{myMsg}' from '{msg.topic}' topic")
		data = json.loads(myMsg)
		_queue.put(data)
	except Exception as e:
		_logger.error(f"Error '{e}' decoding message: '{msg.payload}'")

def process_message(data):
	data =data.get("data")
	if data is not None:
		clean_data =data.split("FF9904")[1]

		format = clean_data[0:2]
		decoder =None
		if "03" == format:
			decoder =Df3Decoder()
		elif "05" == format:
			decoder =Df5Decoder()
		else:
			_logger.error(f"Unknown data format: {format}")
			return 1
		
		myData =None
		myData = decoder.decode_data(clean_data)
		if myData is not None:

			_logger.debug(f"Got data: {myData}")

			operator =myData[_operator]

			_logger.debug(f"Checking condition: {operator} {_operand} {_value}")
			res = eval(f"{operator} {_operand} {_value}")

			_logger.debug(f"Condition result: {res}")

			if True ==res:
				_logger.info(f"Condition '{_operator}' '{_operand}' '{_value}' met for sensor '{_sensor}' with value: {operator}")
				return 2
			else:
				_logger.info(f"Condition '{_operator}' '{_operand}' '{_value}' NOT met for sensor '{_sensor}' with value: {operator}")

		else:
			_logger.error("Decoded data is None")
			return 1

	else:
		_logger.error("No data found in message")
		return 1
	
	return 0


def connect_mqtt() -> mqtt_client:

	# generate client ID with pub prefix randomly
	client_id = f"ruuvimqtt-{random.randint(0, 100)}"

	client = mqtt_client.Client(client_id)

	client.username_pw_set(_uid, _pwd)
	try:
		client.connect(_host, _port)
	except Exception as e:
		_logger.error(f"Failed to connect to MQTT broker at {_host}:{_port} with error: {e}")
		client = None

	return client

def subscribe(client: mqtt_client):
	subscribed ="ruuvi/+/" +_sensor + "/#"
	_logger.debug(f"Subscribing to topic '{subscribed}'")
	client.subscribe(subscribed)
	client.on_message = on_message

def run():
	client = connect_mqtt()
	if None ==client:
		_logger.error("Failed to connect to MQTT broker. Exiting.")
		res =1
	else:
		subscribe(client)
		client.loop_start()

		data =_queue.get()
		res =process_message(data)

		client.loop_stop()
		client.disconnect()

	_logger.debug(f"Process result: {res}")
	return res

if __name__ == '__main__':
	_uid, _pwd, _host, _port, _sensor, _operand, _operator, _value = getMQTTSettings(_confFile)
	if _uid is None or _pwd is None or _host is None or _port is None or _sensor is None or _operand is None or _operator is None or _value is None:
		_logger.error("MQTT settings are not properly configured.")
		sys.exit(1)	

	sys.exit(run())
	

