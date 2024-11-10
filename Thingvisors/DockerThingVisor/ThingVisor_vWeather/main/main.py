import os
import json
import time
from threading import Thread
import paho.mqtt.client as mqtt
from context import Context
from concurrent.futures import ThreadPoolExecutor
import requests
import urllib

# 共有ボリュームからデータを読み込む
with open('/app/data/entry.json', 'r') as f:
    data = json.load(f)

thing_visor_ID = data["thing_visor_ID"]
MQTT_data_broker_IP = data["MQTTDataBrokerIP"]
MQTT_data_broker_port = int(data["MQTTDataBrokerPort"])
MQTT_control_broker_IP = data["MQTTControlBrokerIP"]
MQTT_control_broker_port = int(data["MQTTControlBrokerPort"])
tv_control_prefix = data["tv_control_prefix"]
v_thing_prefix = data["v_thing_prefix"]
in_data_suffix = data["in_data_suffix"]
out_data_suffix = data["out_data_suffix"]
in_control_suffix = data["in_control_suffix"]
out_control_suffix = data["out_control_suffix"]
v_silo_prefix = data["v_silo_prefix"]

params = data["params"]
cities = params["cities"]
sensors = [
    {"id": "_temp", "type": "temp", "description": "現在の気温（ケルビン）", "dataType": "temperature", "thing": "thermometer"},
    {"id": "_humidity", "type": "humidity", "dataType": "humidity", "description": "現在の湿度（%）", "thing": "hygrometer"},
    {"id": "_pressure", "type": "pressure", "dataType": "pressure", "description": "現在の気圧（hPa）", "thing": "barometer"}
]
contexts = {}
v_things = []

for city in cities:
    for sens in sensors:
        thing = sens["thing"]
        label = sens["thing"] + " in " + str(city)
        identifier = thing_visor_ID + "/" + city + sens["id"]
        description = sens["description"]
        topic = f"vThing/{identifier}"
        v_things.append({
            "vThing": {"label": label, "id": identifier, "description": description},
            "topic": topic,
            "type": sens["type"],
            "dataType": sens["dataType"],
            "city": city,
            "thing": thing
        })
        contexts[identifier] = Context()

executor = ThreadPoolExecutor(1)
if "rate" in params:
        refresh_rate = params["rate"]
else:
    refresh_rate = 300

class FetcherThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        for city in cities:
            for v_thing in v_things:
                if v_thing['city'] != city:
                    continue
                v_thing_id = v_thing["vThing"]["id"]
                sens_type = v_thing["type"]
                data = 0.0
                data_type = v_thing["dataType"]
                thing_name = v_thing["thing"]
                ngsi_ld_entity1 = {
                    "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                    "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                    "type": data_type,
                    thing_name: {"type": "Property", "value": data}
                }
                contexts[v_thing_id].set_all([ngsi_ld_entity1])

    def send_message(self, url, message):
        mqtt_data_client.publish(url, message)
    
    def run(self):
        print("Thread Fetcher started")
        while True:
            for city in cities:
                location = urllib.parse.quote(city)
                print(location)
                url = 'https://api.openweathermap.org/data/2.5/weather?q=' + location + '&appid=f5dd231b269189f270094476aa8399a5'
                r = requests.get(url, timeout=1)
                print(r)
                try:
                    r = requests.get(url, timeout=1)
                    print(url)
                    print(r.json())
                except requests.exceptions.Timeout:
                    print("Request timed out - " + city)
                    continue
                except Exception as ex:
                    print("Some Error with city " + city)
                    print(ex)
                    continue

                for v_thing in v_things:
                    if v_thing['city'] != city:
                        continue
                    v_thing_id = v_thing["vThing"]["id"]
                    sens_type = v_thing["type"]
                    data = r.json()["main"][sens_type]
                    data_type = v_thing["dataType"]
                    thing_name = v_thing["thing"]

                    ngsi_ld_entity1 = {
                        "@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                        "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                        "type": data_type,
                        thing_name: {"type": "Property", "value": data}
                    }

                    contexts[v_thing_id].update([ngsi_ld_entity1])
                    message = {"data": [ngsi_ld_entity1], "meta": {"vThingID": v_thing_id}}
                    print(str(message))
                    print("aaa")
                    future = executor.submit(self.send_message, v_thing["topic"] + '/' + out_data_suffix, json.dumps(message))
                    time.sleep(0.2)
            time.sleep(refresh_rate)

class mqttDataThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        print("Thread mqtt data started")
        global mqtt_data_client
        mqtt_data_client.connect(MQTT_data_broker_IP, MQTT_data_broker_port, 30)
        mqtt_data_client.loop_forever()
        print("Thread '" + self.name + "' terminated")

class mqttControlThread(Thread):
    def on_message_get_thing_context(self, jres):
        silo_id = jres["vSiloID"]
        v_thing_id = jres["vThingID"]
        message = {"command": "getContextResponse", "data": contexts[v_thing_id].get_all(), "meta": {"vThingID": v_thing_id}}
        mqtt_control_client.publish(v_silo_prefix + "/" + silo_id + "/" + in_control_suffix, json.dumps(message))

    def send_destroy_v_thing_message(self):
        for v_thing in v_things:
            v_thing_ID = v_thing["vThing"]["id"]
            msg = {"command": "deleteVThing", "vThingID": v_thing_ID, "vSiloID": "ALL"}
            mqtt_control_client.publish(v_thing_prefix + "/" + v_thing_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    def send_destroy_thing_visor_ack_message(self):
        msg = {"command": "destroyTVAck", "thingVisorID": thing_visor_ID}
        mqtt_control_client.publish(tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    def on_message_destroy_thing_visor(self, jres):
        self.send_destroy_v_thing_message()
        self.send_destroy_thing_visor_ack_message()
        print("Shutdown completed")
    
    def __init__(self):
        Thread.__init__(self)

    def on_message_in_control_vThing(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload))
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            if command_type == "getContextRequest":
                self.on_message_get_thing_context(jres)
        except Exception as e:
            print(e)
        return
    
    def on_message_in_control_TV(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload))
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            if command_type == "destroyTV":
                self.on_message_destroy_thing_visor(jres)
        except Exception as e:
            print(e)
        return 'invalid command'
    
    def run(self):
        print("Thread mqtt control started")
        global mqtt_control_client
        mqtt_control_client.connect(MQTT_control_broker_IP, MQTT_control_broker_port, 30)

        for v_thing in v_things:
            v_thing_topic = v_thing["topic"]
            v_thing_message = {"command": "createVThing",
                               "thingVisorID": thing_visor_ID,
                               "vThing": v_thing["vThing"]}
            mqtt_control_client.publish(tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix,
                                        json.dumps(v_thing_message))

            mqtt_control_client.message_callback_add(v_thing_topic + "/" + in_control_suffix,
                                                     self.on_message_in_control_vThing)
            mqtt_control_client.message_callback_add(tv_control_prefix + "/" + thing_visor_ID + "/" + in_control_suffix,
                                                     self.on_message_in_control_TV)
            mqtt_control_client.subscribe(v_thing_topic + '/' + in_control_suffix)
            mqtt_control_client.subscribe(tv_control_prefix + "/" + thing_visor_ID + "/" + in_control_suffix)
            time.sleep(0.1)
        mqtt_control_client.loop_forever()
        print("Thread '" + self.name + "' terminated")

if __name__ == '__main__':
    mqtt_control_client = mqtt.Client()
    mqtt_data_client = mqtt.Client()

    data_thread = FetcherThread()
    data_thread.start()
    print("データ取得スレッド開始")

    mqtt_control_thread = mqttControlThread()
    mqtt_control_thread.start()
    print("MQTTコントロールスレッド開始")

    mqtt_data_thread = mqttDataThread()
    mqtt_data_thread.start()
    print("MQTTデータスレッド開始")

    """while True:
        try:
            time.sleep(3)
        except:
            print("キーボード割り込み")
            time.sleep(1)
            os._exit(1)"""

    