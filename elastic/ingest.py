#!/usr/bin/env python3

'''
Python util to ingest temperature and humidity readings from an MQTT topic.  The DHT22 can give spurious readings 
from time to time, most likely caused by noise / interference, for this reason filter the readings excluding readings outside of 
the normal range. 

See https://forum.dexterindustries.com/t/solved-dht-sensor-occasionally-returning-spurious-values/2939/5

Pre-requisites
pip intall paho -- mqtt client
pip install numpy -- required for the filter
'''

import os
from datetime import datetime
import ssl
import paho.mqtt.client as mqtt
from elasticsearch import Elasticsearch
from time import sleep
import numpy

mqtt_user = os.environ.get("MQTT_USER")
mqtt_pass = os.environ.get("MQTT_PASS")
mqtt_host = os.environ.get("MQTT_HOST")
mqtt_port = int(os.environ.get("MQTT_PORT"))
mqtt_topic = os.environ.get("MQTT_TOPIC")
es_host = os.environ.get("ES_HOST")

def get_data_set(topic):
    qry = { "query": { "bool": { "must": [ {"match_phrase": { "topic": topic }} ], "filter": [ {"range": {"timestamp": {"gte": "now-3h", "lte": "now"}}}]}}}
    results = es.search(index="sensor-1", doc_type="sensor", size=1000, body=qry)

    sampleset = []
    for i, v in enumerate(results['hits']['hits']):
        sampleset.append(v['_source']['sensorData'])

    return sampleset

# we determine the standard normal deviation and we exclude anything that goes beyond a threshold
# think of a probability distribution plot - we remove the extremes
# the greater the std_factor, the more "forgiving" is the algorithm with the extreme values
def is_in_range(topic, value, std_factor = 10):
    values = get_data_set(topic)
    if not values or len(values) < 100:
        return True

    mean = numpy.mean(values)
    standard_deviation = numpy.std(values)
    lo = mean - std_factor * standard_deviation
    hi = mean + std_factor * standard_deviation

    result = (value > lo and value < hi)
    if not result:
        print("%f is out of range [%f - %f], skipping." % (msg.payload, lo, hi))

    return result

def on_connect(client, userdata, flags, rc):
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    if is_in_range(msg.topic, float(msg.payload)):
        post_body = {"topic": msg.topic, "sensorData": float(msg.payload), "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}
        result = es.index(index="sensor-1", doc_type="sensor", body=post_body)

es = Elasticsearch([es_host])

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(ca_certs="/etc/ssl/certs/DST_Root_CA_X3.pem", cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
client.username_pw_set(mqtt_user, mqtt_pass)
client.connect(mqtt_host, mqtt_port, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
client.loop_forever()

