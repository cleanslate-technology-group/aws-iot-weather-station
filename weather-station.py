#!/usr/bin/python
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from sense_hat import SenseHat
from datetime import datetime

import time
import sys
import json
import logging

logging.basicConfig()
logger = logging.getLogger("aws_iot_weather_station")
logger.setLevel(logging.INFO)

IOT_DEVICE_CLIENT_ID = "RaspberryPi4_WeatherStation"                    #String that denotes the client identifier used to connect to AWS IoT
IOT_END_POINT_URL = "antwe8zdhpgnb-ats.iot.us-east-1.amazonaws.com"     #The host name of the user-specific AWS IoT endpoint
IOT_END_POINT_PORT = 8883                                               #Integer that denotes the port number to connect [8883/TLS1.2]
IOT_CERTIFICATE_PATH = "/var/project/certs/"                            #Used to configure the rootCA, private key and certificate file locations
IOT_CERTIFICATE_PREFIX = "863cc3ae71"                                   #Prefix of the certificate files that were created by AWS IoT
IOT_SUBSCRIBE_TOPIC = "output/weather_station"                          #The topic on which the device will receive data from the AWS IoT Platform (platform output)
IOT_PUBLISH_TOPIC = "input/weather_station"                             #The topic on which the device will send data to the AWS IoT Platform (platform input)

G = (0, 255, 0)      #Green
R = (227, 11, 93)    #Raspberry
W = (255, 255, 255)  #White
O = (0,0,0)          #Off

#Configure and establish the AWS IoT Client Connection
def setup_iot_client():
    logger.info("Initializing AWS IoT Client...")
    myIoTClient = AWSIoTMQTTClient(IOT_DEVICE_CLIENT_ID) 
    myIoTClient.configureEndpoint(IOT_END_POINT_URL, IOT_END_POINT_PORT)
    myIoTClient.configureCredentials(\
        "{0}root-ca.pem".format(IOT_CERTIFICATE_PATH),\
        "{0}{1}-private.pem.key".format(IOT_CERTIFICATE_PATH, IOT_CERTIFICATE_PREFIX),\
        "{0}{1}-certificate.pem.crt".format(IOT_CERTIFICATE_PATH, IOT_CERTIFICATE_PREFIX))  #Used to configure the rootCA, private key and certificate files
    myIoTClient.configureOfflinePublishQueueing(-1)                                         #Used to configure the queue size and drop behavior for the offline requests queueing (-1 is infinite size)
    myIoTClient.configureDrainingFrequency(5)                                               #Used to configure the draining speed to clear up the queued requests when the connection is back
    myIoTClient.configureConnectDisconnectTimeout(10)                                       #Used to configure the time in seconds to wait for a disconnect to complete
    myIoTClient.configureMQTTOperationTimeout(5)                                            #Used to configure the timeout in seconds for MQTT QoS publish, subscribe and unsubscribe
    status = myIoTClient.connect()
    
    if status == True:
        logger.info("AWS IoT Client Connection Established...") 
    
    return myIoTClient
    
#Subscribe to an AWS IoT Topic for messages
def subscribe_to_iot_topic(client, topic, function):    
    return client.subscribe(topic, 0, function)

#Publish a message to an AWS IoT Topic
def publish_iot_message(client, topic, message):
    return client.publish(topic, message, 0)

#Invoked when a message is sent to this device
def receive_iot_message(client, userdata, message):
    logger.info("Topic: %s", message.topic)
    logger.info("Payload: %s", message.payload)

#Initialize the Raspberry Pi SenseHat
def init_sense_hat():
    logger.info("Initializing Raspberry Pi SenseHat...")
    sense = SenseHat()
    sense.clear()
    sense.low_light = False
    sense.set_rotation(0)
    sense.set_pixels(get_pi_logo())
    return sense

#Convert Celcius to Fahrenheit
def convert_fahrenheit(temp):
    return 1.8 * (temp) + 32

#Return a Calibrated Temperature based upon Device Temperate minus an offset to 
#account for internal thermal warning of the Raspberry Pi board
def get_calibrated_temp(sense):
    tempf = convert_fahrenheit(sense.get_temperature())
    tempcf = tempf - 0.0   #purchased a new case to avoid CPU thermal heat     
    return tempcf

def get_pi_logo():
    return [
    O, G, G, O, O, G, G, O, 
    O, O, G, G, G, G, O, O,
    O, O, R, R, R, R, O, O, 
    O, R, R, R, R, R, R, O,
    R, R, R, R, R, R, R, R,
    R, R, R, R, R, R, R, R,
    O, R, R, R, R, R, R, O,
    O, O, R, R, R, R, O, O,
    ]

def get_checkmark_logo():
    return [
    O, O, G, G, G, G, O, O, 
    O, G, G, G, G, G, G, O, 
    G, G, G, G, G, G, W, G, 
    G, G, G, G, G, W, G, G, 
    G, G, G, G, W, G, G, G, 
    G, W, G, W, G, G, G, G, 
    O, G, W, G, G, G, G, O, 
    O, O, G, G, G, G, O, O, 
    ]

def get_xmark_logo():
    return [
    O, O, R, R, R, R, O, O, 
    O, W, R, R, R, R, W, O, 
    R, R, W, R, R, W, R, R, 
    R, R, R, W, W, R, R, R, 
    R, R, R, W, W, R, R, R,  
    R, R, W, R, R, W, R, R, 
    O, W, R, R, R, R, W, O, 
    O, O, R, R, R, R, O, O,
    ] 
    
def main():    
    logger.info("Starting AWS IoT Weather Station...")
    sense = init_sense_hat()
    myIoTClient = setup_iot_client()
    subscribe_to_iot_topic(myIoTClient, IOT_SUBSCRIBE_TOPIC, receive_iot_message)
    t, h = [0]*15, [0]*15
    try:
        while True:
            sense.set_pixels(get_pi_logo())
            #Measure every 2 seconds for 30 seconds, average the results and publish
            for x in range(1, 16):  
                t[x-1], h[x-1] = get_calibrated_temp(sense), sense.get_humidity()
                
                if(x % 15 == 0):
                    temp, humidity = sum(t)/len(t), sum(h)/len(h)
                    payload = json.dumps(\
                        {"deviceId" : IOT_DEVICE_CLIENT_ID,\
                        "timestamp" : datetime.now().isoformat(),\
                        "temperature" : temp,\
                        "humidity" : humidity})                   
                    logger.info("Payload: %s", payload)
                    result = publish_iot_message(myIoTClient, IOT_PUBLISH_TOPIC, payload)
                    t, h = [0]*15, [0]*15
                    if(result == True):
                        sense.set_pixels(get_checkmark_logo())
                    else:
                        sense.set_pixels(get_xmark_logo())
                time.sleep(2)
                
    except KeyboardInterrupt: 
        pass
        
    finally:    
        sense.clear()
        myIoTClient.disconnect()
        
main()













