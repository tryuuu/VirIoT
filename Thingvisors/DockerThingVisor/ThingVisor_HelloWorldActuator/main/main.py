#! /usr/local/bin/python3

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Fed4IoT ThingVisor hello world actuator

import time
import os
import random
import json
import traceback
import string
import paho.mqtt.client as mqtt
import jsonschema
from threading import Thread
from pymongo import MongoClient
from context import Context
import grpc
from concurrent import futures
import proto.thingvisor_pb2 as thingvisor_pb2
import proto.thingvisor_pb2_grpc as thingvisor_pb2_grpc

from concurrent.futures import ThreadPoolExecutor

class ThingVisorInitializer:
    def __init__(self, config_path='/app/data/entry.json'):
        self.config_path = config_path
        self.load_config()
        self.set_mqtt_clients()
        self.set_thing_attributes()
        self.set_params()
        self.executor = ThreadPoolExecutor(1)
        print("ThingVisorInitializer initialized successfully", flush=True)

    def load_config(self):
        with open(self.config_path, 'r') as f:
            self.data = json.load(f)
        self.thing_visor_collection = self.data["thing_visor_collection"]
        self.thing_visor_ID = self.data["thing_visor_ID"]
        self.MQTT_data_broker_IP = self.data["MQTTDataBrokerIP"]
        self.MQTT_data_broker_port = self.data["MQTTDataBrokerPort"]
        self.MQTT_control_broker_IP = self.data["MQTTControlBrokerIP"]
        self.MQTT_control_broker_port = self.data["MQTTControlBrokerPort"]
        self.tv_control_prefix = self.data["tv_control_prefix"]
        self.v_thing_prefix = self.data["v_thing_prefix"]
        self.in_data_suffix = self.data["in_data_suffix"]
        self.out_data_suffix = self.data["out_data_suffix"]
        self.in_control_suffix = self.data["in_control_suffix"]
        self.out_control_suffix = self.data["out_control_suffix"]
        self.v_silo_prefix = self.data["v_silo_prefix"]
        self.params = self.data.get("params", {})
        self.v_thing_topic = "v_thing_prefix" + "/" + self.thing_visor_ID

    def set_mqtt_clients(self):
        self.mqtt_control_client = mqtt.Client()
        self.mqtt_data_client = mqtt.Client()
        print("MQTT clients set up successfully", flush=True)

    def set_thing_attributes(self):
        self.v_thing_name = "Lamp01"
        self.v_thing_type_attr = "Lamp"
        self.v_thing_ID = self.thing_visor_ID + "/" + self.v_thing_name
        self.v_thing_ID_LD = f"urn:ngsi-ld:{self.thing_visor_ID}:{self.v_thing_name}"
        self.v_thing_label = "helloWorldActuator"
        self.v_thing_description = "hello world actuator simulating a colored lamp"
        self.v_thing = {
            "label": self.v_thing_label,
            "id": self.v_thing_ID,
            "description": self.v_thing_description,
            "type": "actuator"
        }
        print("Thing attributes set", flush=True)

    def set_params(self):
        self.sleep_time = self.params.get("rate", 5)

    @staticmethod
    def random_string(string_length=10):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(string_length))

class DataThread(Thread):
    # Class used to:
    # 1) handle actuation command workflow
    # 2) publish actuator status when it changes
    global mqtt_data_client, LampActuatorContext, executor, commands

    def send_commandResult(self, cmd_name, cmd_info, id_LD, result_code):
        pname = cmd_name+"-result"
        pvalue = cmd_info.copy()
        pvalue['cmd-result'] = result_code
        ngsiLdEntityResult = {"id": id_LD,
                                "type": v_thing_type_attr,
                                pname: {"type": "Property", "value": pvalue},
                                "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
                                }
        data = [ngsiLdEntityResult]
        # LampActuatorContext.update(data)
        
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format message
        if "cmd-nuri" in cmd_info:
            if cmd_info['cmd-nuri'].startswith("viriot://"):
                topic = cmd_info['cmd-nuri'][len("viriot://"):]
                self.publish(message, topic)
            else:
                self.publish(message)
        else:
            self.publish(message)

    def send_commandStatus(self, cmd_name, cmd_info, id_LD, status_code):
        print("ccc", flush=True)
        pname = cmd_name+"-status"
        pvalue = cmd_info.copy()
        pvalue['cmd-status'] = status_code
        ngsiLdEntityStatus = {"id": id_LD,
                                "type": self.v_thing_type_attr,
                                pname: {"type": "Property", "value": pvalue},
                                "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
                                }
        data = [ngsiLdEntityStatus]  
        message = {"data": data, "meta": {
            "vThingID": self.v_thing_ID}}  # neutral-format message
        print(message, flush=True)
        if "cmd-nuri" in cmd_info:
            if cmd_info['cmd-nuri'].startswith("viriot://"):
                topic = cmd_info['cmd-nuri'][len("viriot://"):]
                self.publish(message, topic)
            else:
                self.publish(message)
        else:
            self.publish(message)


    def receive_commandRequest(self, cmd_entity):
        print("bbb", flush=True)
        try:  
            #jsonschema.validate(data, commandRequestSchema)
            id_LD = cmd_entity["id"]
            for cmd_name in commands:
                if cmd_name in cmd_entity:
                    cmd_info = cmd_entity[cmd_name]['value']
                    fname = cmd_name.replace('-','_')
                    fname = "on_"+fname
                    f=getattr(self,fname)
                    if "cmd-qos" in cmd_info:
                        if int(cmd_info['cmd-qos']) == 2:
                            self.send_commandStatus(cmd_name, cmd_info, id_LD, "PENDING")
                    future = executor.submit(f, cmd_name, cmd_info, id_LD, self)
                    

        #except jsonschema.exceptions.ValidationError as e:
            #print("received commandRequest got a schema validation error: ", e)
        #except jsonschema.exceptions.SchemaError as e:
            #print("commandRequest schema not valid:", e)
        except Exception as ex:
            traceback.print_exc()
        return

    def on_set_color(self, cmd_name, cmd_info, id_LD, actuatorThread):
        global LampActuatorContext
        # function to change the color of the Lamp should be written here
        # update the Context, publish new actuator status on data_out, send result
        ngsiLdEntity = {"id": id_LD,
                        "type": v_thing_type_attr,
                        "color": {"type": "Property", "value": cmd_info['cmd-value']},
                        "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
                        }
        data = [ngsiLdEntity]
        self.LampActuatorContext.update(data)
        
        # publish changed status
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format
        self.publish(message)

        # publish command result
        if "cmd-qos" in cmd_info:
            if int(cmd_info['cmd-qos']) > 0:
                self.send_commandResult(cmd_name, cmd_info, id_LD, "OK")

    def on_set_status(self, cmd_name, cmd_info, id_LD, actuatorThread):
        global LampActuatorContext
        # function to change the status of the Lamp should be written here
        # update the Context, publish new actuator status on data_out, send result
        ngsiLdEntity = {"id": id_LD,   
                        "type": v_thing_type_attr,
                        "status": {"type": "Property", "value": cmd_info['cmd-value']},
                        "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
                        }
        data = [ngsiLdEntity]
        self.LampActuatorContext.update(data)
        
        # publish changed status
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format message
        self.publish(message)

        # publish command result
        if "cmd-qos" in cmd_info:
            if int(cmd_info['cmd-qos']) > 0:
                self.send_commandResult(cmd_name, cmd_info, id_LD, "OK")

    def publish(self, message, topic=""):
        print("ddd", flush=True)
        msg=json.dumps(message)

        if topic == "":
            out_topic = self.v_thing_topic + '/' + self.out_data_suffix
        else:
            out_topic = topic
        #msg = str(message).replace("\'", "\"")
        print("Message sent on "+out_topic + "\n" + msg+"\n", flush=True)
        # publish data to out_topic
        mqtt_data_client.publish(out_topic, msg)

    def on_message_data_in_vThing(self, mosq, obj, msg):
        print("aaa", flush=True)
        payload = msg.payload.decode("utf-8", "ignore")
        print("Message received on "+msg.topic + "\n" + payload+"\n")
        jres = json.loads(payload.replace("\'", "\""))
        try:
            data = jres["data"]
            for entity in data:
                id_LD = entity["id"]
                if id_LD != self.v_thing_ID_LD:
                    print("Entity not handled by the Thingvisor, message dropped", flush=True)
                    continue
                for cmd in commands:
                    if cmd in entity:
                        self.receive_commandRequest(entity)
                        continue
            return
        except Exception as ex:
            traceback.print_exc()
        return

    def __init__(self, initializer):
        Thread.__init__(self)
        self.initializer = initializer
        self.thing_visor_ID = initializer.thing_visor_ID
        self.v_thing_topic = initializer.v_thing_topic
        self.in_data_suffix = initializer.in_data_suffix
        self.out_data_suffix = initializer.out_data_suffix
        self.MQTT_data_broker_IP = initializer.MQTT_data_broker_IP
        self.MQTT_data_broker_port = initializer.MQTT_data_broker_port
        self.in_data_suffix = initializer.in_data_suffix
        self.LampActuatorContext = Context()
        self.v_thing_name = "Lamp01"
        self.v_thing_ID_LD = f"urn:ngsi-ld:{self.thing_visor_ID}:{self.v_thing_name}"
        self.v_thing_type_attr = "Lamp"
        self.v_thing_ID = initializer.thing_visor_ID + "/" + self.v_thing_name

    def run(self):
        global commands
        # this method should fetch the status (context) from the real actuator,
        # represent it as ngsiLdEntity,
        # and finally store it in the HelloActuatorContext

        # Create initial status
        commands = ["set-color","set-luminosity","set-status"]
        ngsiLdEntity = {"id": self.v_thing_ID_LD,
                        "type": self.v_thing_type_attr,
                        "status": {"type": "Property", "value": "off"},
                        "color": {"type": "Property", "value": "white"},
                        "luminosity": {"type": "Property", "value": "255"},
                        "commands": {"type": "Property", "value": ["set-color","set-luminosity","set-status"]},
                        "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
        }

        data = [ngsiLdEntity]
        self.LampActuatorContext.set_all(data)

        print("Thread mqtt data started", flush=True)
        result = mqtt_data_client.connect(
            self.MQTT_data_broker_IP, self.MQTT_data_broker_port, 30)
        print(result, flush=True)
        # define callback and subscriptions for data_in where to receive actuator commands
        mqtt_data_client.message_callback_add(self.v_thing_topic + "/" + self.in_data_suffix,
                                              self.on_message_data_in_vThing)
        # subscribeはsidecarでやるため不要に                                      
        """mqtt_data_client.subscribe(
            self.v_thing_topic + "/" + self.in_data_suffix)"""
        mqtt_data_client.loop_forever()
        print("Thread '" + self.name + "' terminated")

class ThingVisorNotifierServicer(thingvisor_pb2_grpc.ThingVisorNotifierServicer):
    def __init__(self):
        self.initialization_complete = False
        self.received_data = False

    def NotifyInitializationComplete(self, request, context):
        print(f"Received notification: {request.message}")
        self.initialization_complete = True
        return thingvisor_pb2.InitializationResponse(status="Success")

    def SendData(self, request, context):
        print(f"Received data: {request.data}", flush=True)
        self.received_data = True
        # MQTT メッセージ形式を模倣
        class DummyMessage:
            def __init__(self, payload, topic):
                self.payload = payload
                self.topic = topic
        message = DummyMessage(payload=request.data.encode('utf-8'), topic="gRPC/SendData")
        initializer = ThingVisorInitializer()
        data_thread = DataThread(initializer)
        data_thread.on_message_data_in_vThing(None, None, message)
        return thingvisor_pb2.DataResponse(status="Data received successfully")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notifier = ThingVisorNotifierServicer()
    thingvisor_pb2_grpc.add_ThingVisorNotifierServicer_to_server(notifier, server)
    server.add_insecure_port("0.0.0.0:50051")
    server.start()

    print("gRPC server is running...", flush=True)
    while True:
        print(f"Initialization status: {notifier.initialization_complete}", flush=True)
        time.sleep(2)
        if notifier.initialization_complete:
            break
    #server.wait_for_termination()
    return server, notifier


# main
if __name__ == '__main__':
    mqtt_control_client = mqtt.Client()
    mqtt_data_client = mqtt.Client()

    # gRPCサーバを起動
    server, notifier = serve()

    try:
        while not notifier.initialization_complete:
            print("Waiting for initialization notification...", flush=True)
            time.sleep(1)

        print("Initialization complete! Starting ThingVisorInitializer...", flush=True)
        initializer = ThingVisorInitializer()

        while not notifier.received_data:
            print("Waiting for data...", flush=True)
            time.sleep(1)
        print("Data received! Starting DataThread...", flush=True)

        data_thread = DataThread(initializer)
        data_thread.start()

        while True:
            try:
                time.sleep(3)
            except KeyboardInterrupt:
                print("KeyboardInterrupt")
                time.sleep(1)
                os._exit(1)

    except Exception as e:
        print(f"Error occurred: {e}", flush=True)
        os._exit(1)
