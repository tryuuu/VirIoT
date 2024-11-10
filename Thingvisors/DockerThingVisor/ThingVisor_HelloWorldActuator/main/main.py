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

from concurrent.futures import ThreadPoolExecutor

with open('/app/data/entry.json', 'r') as f:
    data = json.load(f)

thing_visor_collection = data["thing_visor_collection"]
thing_visor_ID = data["thing_visor_ID"]
v_thing_ID = thing_visor_ID + "/" + "hello"
v_thing_label = "helloWorld"
v_thing_description = "hello world virtual thing"
v_thing = {"label": v_thing_label,
           "id": v_thing_ID,
           "description": v_thing_description}

MQTT_data_broker_IP = data["MQTTDataBrokerIP"]
MQTT_data_broker_port = data["MQTTDataBrokerPort"]
MQTT_control_broker_IP = data["MQTTControlBrokerIP"]
MQTT_control_broker_port = data["MQTTControlBrokerPort"]
tv_control_prefix = data["tv_control_prefix"]
v_thing_prefix = data["v_thing_prefix"]
in_data_suffix = data["in_data_suffix"]
out_data_suffix = data["out_data_suffix"]
in_control_suffix = data["in_control_suffix"]
out_control_suffix = data["out_control_suffix"]
v_silo_prefix = data["v_silo_prefix"]

params = data["params"]

thing_visor_ID = os.environ["thingVisorID"]
v_thing_name = "Lamp01"
v_thing_type_attr = "Lamp"
v_thing_ID = thing_visor_ID + "/" + v_thing_name
v_thing_ID_LD = "urn:ngsi-ld:"+thing_visor_ID+":" + \
v_thing_name  # ID used in id field od ngsi-ld for data
v_thing_label = "helloWorldActuator"
v_thing_description = "hello world actuator simulating a colored lamp"
v_thing = {"label": v_thing_label,
            "id": v_thing_ID,
            "description": v_thing_description,
            "type": "actuator"}
commands=[]
LampActuatorContext = Context()
contexts = {v_thing_ID: LampActuatorContext}

if params:
    if 'rate' in params:
        sleep_time = params['rate']
    else:
        sleep_time = 5
else:
    sleep_time = 5

mqtt_control_client = mqtt.Client()
mqtt_data_client = mqtt.Client()
v_thing_topic = v_thing_prefix + "/" + v_thing_ID
executor = ThreadPoolExecutor(1)

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
        pname = cmd_name+"-status"
        pvalue = cmd_info.copy()
        pvalue['cmd-status'] = status_code
        ngsiLdEntityStatus = {"id": id_LD,
                                "type": v_thing_type_attr,
                                pname: {"type": "Property", "value": pvalue},
                                "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
                                }
        data = [ngsiLdEntityStatus]
                
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

    def receive_commandRequest(self, cmd_entity):
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
        LampActuatorContext.update(data)
        
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
        LampActuatorContext.update(data)
        
        # publish changed status
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format message
        self.publish(message)

        # publish command result
        if "cmd-qos" in cmd_info:
            if int(cmd_info['cmd-qos']) > 0:
                self.send_commandResult(cmd_name, cmd_info, id_LD, "OK")

    def publish(self, message, topic=""):
        msg=json.dumps(message)
        if topic == "":
            out_topic = v_thing_topic + '/' + out_data_suffix
        else:
            out_topic = topic
        #msg = str(message).replace("\'", "\"")
        print("Message sent on "+out_topic + "\n" + msg+"\n")
        # publish data to out_topic
        mqtt_data_client.publish(out_topic, msg)

    def on_message_data_in_vThing(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print("Message received on "+msg.topic + "\n" + payload+"\n")
        jres = json.loads(payload.replace("\'", "\""))
        try:
            data = jres["data"]
            for entity in data:
                id_LD = entity["id"]
                if id_LD != v_thing_ID_LD:
                    print("Entity not handled by the Thingvisor, message dropped")
                    continue
                for cmd in commands:
                    if cmd in entity:
                        self.receive_commandRequest(entity)
                        continue
            return
        except Exception as ex:
            traceback.print_exc()
        return

    def __init__(self):
        Thread.__init__(self)
        

    def run(self):
        global commands
        # this method should fetch the status (context) from the real actuator,
        # represent it as ngsiLdEntity,
        # and finally store it in the HelloActuatorContext

        # Create initial status
        commands = ["set-color","set-luminosity","set-status"]
        ngsiLdEntity = {"id": v_thing_ID_LD,
                        "type": v_thing_type_attr,
                        "status": {"type": "Property", "value": "off"},
                        "color": {"type": "Property", "value": "white"},
                        "luminosity": {"type": "Property", "value": "255"},
                        "commands": {"type": "Property", "value": ["set-color","set-luminosity","set-status"]},
                        "@context": [ "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" ]
        }

        data = [ngsiLdEntity]
        LampActuatorContext.set_all(data)

        print("Thread mqtt data started")
        mqtt_data_client.connect(
            MQTT_data_broker_IP, MQTT_data_broker_port, 30)
        # define callback and subscriptions for data_in where to receive actuator commands
        mqtt_data_client.message_callback_add(v_thing_topic + "/" + in_data_suffix,
                                              self.on_message_data_in_vThing)
        mqtt_data_client.subscribe(
            v_thing_topic + "/" + in_data_suffix)
        mqtt_data_client.loop_forever()
        print("Thread '" + self.name + "' terminated")


class ControlThread(Thread):

    def on_message_get_thing_context(self, jres):
        silo_id = jres["vSiloID"]
        message = {"command": "getContextResponse", "data": LampActuatorContext.get_all(), "meta": {
            "vThingID": v_thing_ID}}
        mqtt_control_client.publish(v_silo_prefix + "/" + silo_id +
                                    "/" + in_control_suffix, json.dumps(message))

    def send_destroy_v_thing_message(self):
        msg = {"command": "deleteVThing",
               "vThingID": v_thing_ID, "vSiloID": "ALL"}
        mqtt_control_client.publish(
            v_thing_prefix + "/" + v_thing_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    def send_destroy_thing_visor_ack_message(self):
        msg = {"command": "destroyTVAck", "thingVisorID": thing_visor_ID}
        mqtt_control_client.publish(
            tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    def on_message_destroy_thing_visor(self, jres):
        global db_client
        db_client.close()
        self.send_destroy_v_thing_message()
        self.send_destroy_thing_visor_ack_message()
        print("Shutdown completed")

    def on_message_control_in_vThing(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload)+"\n")
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            if command_type == "getContextRequest":
                self.on_message_get_thing_context(jres)
        except Exception as ex:
            traceback.print_exc()
        return

    def on_message_control_in_TV(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload)+"\n")
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            if command_type == "destroyTV":
                self.on_message_destroy_thing_visor(jres)
        except Exception as ex:
            traceback.print_exc()
        return 'invalid command'

        # handler for mqtt control topics

    def __init__(self):
        Thread.__init__(self)

    def run(self):
        print("Thread mqtt control started"+"\n")
        global mqtt_control_client
        mqtt_control_client.connect(
            MQTT_control_broker_IP, MQTT_control_broker_port, 30)

        # Publish on the thingVisor out_control topic the createVThing command and other parameters
        v_thing_message = {"command": "createVThing",
                           "thingVisorID": thing_visor_ID,
                           "vThing": v_thing}
        mqtt_control_client.publish(tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix,
                                    json.dumps(v_thing_message))

        # Add message callbacks that will only trigger on a specific subscription match
        mqtt_control_client.message_callback_add(v_thing_topic + "/" + in_control_suffix,
                                                 self.on_message_control_in_vThing)

        mqtt_control_client.message_callback_add(tv_control_prefix + "/" + thing_visor_ID + "/" + in_control_suffix,
                                                 self.on_message_control_in_TV)
        mqtt_control_client.subscribe(
            v_thing_topic + '/' + in_control_suffix)
        mqtt_control_client.subscribe(
            tv_control_prefix + "/" + thing_visor_ID + "/" + in_control_suffix)
        mqtt_control_client.loop_forever()
        print("Thread '" + self.name + "' terminated"+"\n")


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


# main
if __name__ == '__main__':
    mqtt_control_client = mqtt.Client()
    mqtt_data_client = mqtt.Client()
    
    data_thread = DataThread()
    data_thread.start()

    control_thread = ControlThread()  
    control_thread.start()

    while True:
        try:
            time.sleep(3)
        except:
            print("KeyboardInterrupt"+"\n")
            time.sleep(1)
            os._exit(1)
