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

# -*- coding: utf-8 -*-


class LampActuatorThread(Thread):
    # Class used to:
    # 1) handle actuation command workflow
    # 2) publish actuator status when it changes
    global mqtt_data_client, LampActuatorContext, executor

    def send_commandResult(self, cmd_id, cmd_value, cmd_nuri):
        # send command result to the notification uri (only "viriot:/topic_name" supported)
        # if cmd_nuri is void, send result on vThing data_out, i.e. to every vSilo connected to the vThing
        value = {"commandResultType": "",
                 "commandResultValue": cmd_value, "commandID": cmd_id}
        ngsiLdEntityResponse = {"id": v_thing_ID_LD_CMD_PREFIX + ":" + cmd_id,
                                "type": v_thing_type_attr,
                                "commandResult": {"type": "Property", "value": value}
                                }
        data = [ngsiLdEntityResponse]
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format
        if cmd_nuri.startswith("viriot:/"):
            topic = cmd_nuri[len("viriot:/"):]
            self.publish(message, topic)
        else:
            self.publish(message)

    def send_commandStatus(self, cmd_id, cmd_value, cmd_nuri):
        # send command status to the notification uri (only "viriot:/topic_name" supported)
        # if cmd_nuri is void, send result on vThing data_out, i.e. to every vSilo connected to the vThing
        value = {"commandStatusType": "",
                 "commandStatusValue": cmd_value, "commandID": cmd_id}
        ngsiLdEntityResponse = {"id": v_thing_ID_LD_CMD_PREFIX + ":" + cmd_id,
                                "type": v_thing_type_attr,
                                "commandStatus": {"type": "Property", "value": value}
                                }
        data = [ngsiLdEntityResponse]
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format
        if cmd_nuri.startswith("viriot:/"):
            topic = cmd_nuri[len("viriot:/"):]
            self.publish(message, topic)
        else:
            self.publish(message)

    def receive_commandRequest(self, data):
        try:
            jsonschema.validate(data, commandRequestSchema)
            id_LD = data["id"]
            # command id without the command prefix
            cmd_id = id_LD[len(v_thing_ID_LD_CMD_PREFIX)+1:]
            cmd_request_value = data['commandRequest']['value']
            cmd_type = cmd_request_value['commandType']
            cmd_value = cmd_request_value['commandValue']
            if cmd_request_value['commandID'] != cmd_id:
                # received command has a wrong inner ID
                print("Received command has a wrong inner commandID, it is " +
                      cmd_request_value['commandID']+", it should be "+cmd_id+"\n")
                return

            # nuri is the notification URI callback
            if "nuri" in cmd_request_value:
                cmd_nuri = cmd_request_value['nuri']
            else:
                cmd_nuri = ""

            if cmd_type == "set-color":
                future = executor.submit(
                    self.on_set_color, cmd_id, cmd_value, cmd_nuri, self)
                self.send_commandStatus(cmd_id, "set-color PENDING", cmd_nuri)
                return
            if cmd_type == "set-status":
                future = executor.submit(
                    self.on_set_status, cmd_id, cmd_value, cmd_nuri, self)
                self.send_commandStatus(cmd_id, "set-status PENDING", cmd_nuri)
                return
            print("Command "+cmd_type+" not supported by the actuator"+"\n")

        except jsonschema.exceptions.ValidationError as e:
            print("received commandRequest got a schema validation error: ", e)
        except jsonschema.exceptions.SchemaError as e:
            print("commandRequest schema not valid:", e)
        except Exception as ex:
            traceback.print_exc()
        return

    def on_set_color(self, cmd_id, cmd_value, cmd_nuri, actuatorThread):
        global LampActuatorContext
        # function to change the color of the lamp should be written here
        # update of the ActuatorContext and publish new actuator status on data_out
        ngsiLdEntity = {"id": v_thing_ID_LD,
                        "type": v_thing_type_attr,
                        "color": {"type": "Property", "value": cmd_value}
                        }
        data = [ngsiLdEntity]
        LampActuatorContext.update(data)
        # publish changed status
        message = {"data": data, "meta": {
            "vThingID": v_thing_ID}}  # neutral-format
        self.publish(message)

        # publish command result
        actuatorThread.send_commandResult(cmd_id, "set-color OK", cmd_nuri)

        return

    def on_set_status(self, cmd_id, cmd_value, nuri, actuatorThread):
        global LampActuatorContext
        # function to change the status of the lamp should be written here
        # update of the ActuatorContext and publish new status
        ngsiLdEntity = {"id": v_thing_ID_LD,
                        "type": v_thing_type_attr,
                        "status": {"type": "Property", "value": cmd_value}
                        }
        data = [ngsiLdEntity]
        LampActuatorContext.update(data)
        # publish changed status
        message = {"data": data, "meta": {"vThingID": v_thing_ID}}
        actuatorThread.publish(message)

        # send command result
        # publish command result
        actuatorThread.send_commandResult(cmd_id, "set-status OK", cmd_nuri)
        return

    def publish(self, message, topic=""):
        if topic == "":
            out_topic = v_thing_topic + '/' + v_thing_data_out_suffix
        else:
            out_topic = topic
        msg = str(message).replace("\'", "\"")
        print("Message sent on "+out_topic + "\n" + msg+"\n")
        # publish data to data out topic
        mqtt_data_client.publish(out_topic, msg)

    def on_message_data_in_vThing(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print("Message received on "+msg.topic + "\n" + payload+"\n")
        jres = json.loads(payload.replace("\'", "\""))
        try:
            data = jres["data"]
            id_LD = data["id"]
            if id_LD.startswith(v_thing_ID_LD_CMD_PREFIX) == True:
                # received command has a wrong command prefix
                self.receive_commandRequest(data)
                return
        except Exception as ex:
            traceback.print_exc()
        return

    def __init__(self):
        Thread.__init__(self)

    def run(self):
        # this method should fetch the status (context) from the real actuator,
        # represent it as ngsiLdEntity,
        # and finally store it in the HelloActuatorContext

        # Create initial status
        ngsiLdEntity = {"id": v_thing_ID_LD,
                        "type": v_thing_type_attr,
                        "status": {"type": "Property", "value": "off"},
                        "color": {"type": "Property", "value": "white"}
                        }
        data = [ngsiLdEntity]
        LampActuatorContext.set_all(data)

        print("Thread mqtt data started")
        mqtt_data_client.connect(
            MQTT_data_broker_IP, MQTT_data_broker_port, 30)
        # define callback and subscriptions for data_in where to receive actuator commands
        mqtt_data_client.message_callback_add(v_thing_topic + "/" + v_thing_data_in_suffix,
                                              self.on_message_data_in_vThing)
        mqtt_data_client.subscribe(
            v_thing_topic + "/" + v_thing_data_in_suffix)
        mqtt_data_client.loop_forever()
        print("Thread '" + self.name + "' terminated")


class MqttControlThread(Thread):

    def on_message_get_thing_context(self, jres):
        silo_id = jres["vSiloID"]
        message = {"command": "getContextResponse", "data": LampActuatorContext.get_all(), "meta": {
            "vThingID": v_thing_ID}}
        mqtt_control_client.publish(v_silo_prefix + "/" + silo_id +
                                    "/" + tv_control_in_suffix, str(message).replace("\'", "\""))

    def send_destroy_v_thing_message(self):
        msg = {"command": "deleteVThing",
               "vThingID": v_thing_ID, "vSiloID": "ALL"}
        mqtt_control_client.publish(
            v_thing_prefix + "/" + v_thing_ID + "/" + tv_control_out_suffix, str(msg).replace("\'", "\""))
        return

    def send_destroy_thing_visor_ack_message(self):
        msg = {"command": "destroyTVAck", "thingVisorID": thing_visor_ID}
        mqtt_control_client.publish(
            tv_prefix + "/" + thing_visor_ID + "/" + tv_control_out_suffix, str(msg).replace("\'", "\""))
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
        mqtt_control_client.publish(tv_prefix + "/" + thing_visor_ID + "/" + tv_control_out_suffix,
                                    str(v_thing_message).replace("\'", "\""))

        # Add message callbacks that will only trigger on a specific subscription match
        mqtt_control_client.message_callback_add(v_thing_topic + "/" + tv_control_in_suffix,
                                                 self.on_message_control_in_vThing)

        mqtt_control_client.message_callback_add(tv_prefix + "/" + thing_visor_ID + "/" + tv_control_in_suffix,
                                                 self.on_message_control_in_TV)
        mqtt_control_client.subscribe(
            v_thing_topic + '/' + tv_control_in_suffix)
        mqtt_control_client.subscribe(
            tv_prefix + "/" + thing_visor_ID + "/" + tv_control_in_suffix)
        mqtt_control_client.loop_forever()
        print("Thread '" + self.name + "' terminated"+"\n")


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


# main
if __name__ == '__main__':
    # v_thing_ID = os.environ["vThingID_0"]
    thing_visor_ID = os.environ["thingVisorID"]
    v_thing_name = "Lamp01"
    v_thing_type_attr = "Lamp"
    v_thing_ID = thing_visor_ID + "/" + v_thing_name
    v_thing_ID_LD = "urn:ngsi-ld:"+thing_visor_ID+":" + \
        v_thing_name  # ID used in id field od ngsi-ld for data
    # prefix ID used in id field of ngsi-ld for commands
    v_thing_ID_LD_CMD_PREFIX = v_thing_ID_LD + ":" + "cmd"
    v_thing_label = "helloWorldActuator"
    v_thing_description = "hello world actuator simulating a colored lamp"
    v_thing = {"label": v_thing_label,
               "id": v_thing_ID,
               "description": v_thing_description,
               "type": "actuator"}

    MQTT_data_broker_IP = os.environ["MQTTDataBrokerIP"]
    MQTT_data_broker_port = int(os.environ["MQTTDataBrokerPort"])
    MQTT_control_broker_IP = os.environ["MQTTControlBrokerIP"]
    MQTT_control_broker_port = int(os.environ["MQTTControlBrokerPort"])

    tv_prefix = "TV"  # prefix name for controller communication topic
    v_thing_prefix = "vThing"  # prefix name for virtual Thing data and control topics
    v_thing_data_out_suffix = "data_out"
    v_thing_data_in_suffix = "data_in"
    tv_control_in_suffix = "c_in"
    tv_control_out_suffix = "c_out"
    v_silo_prefix = "vSilo"
    v_thing_topic = v_thing_prefix + "/" + v_thing_ID

    # import paramenters from environments
    parameters = str(os.environ.get("params")).replace("'", '"')
    # parameters = os.environ["params"].replace("'", '"')
    if parameters:
        try:
            params = json.loads(parameters)
        except json.decoder.JSONDecodeError:
            # TODO manage exception
            print("error on params (JSON) decoding"+"\n")

    # Mongodb settings
    time.sleep(1.5)  # wait before query the system database
    db_name = "viriotDB"  # name of system database
    thing_visor_collection = "thingVisorC"
    db_IP = os.environ['systemDatabaseIP']  # IP address of system database
    db_port = os.environ['systemDatabasePort']  # port of system database
    db_client = MongoClient('mongodb://' + db_IP + ':' + str(db_port) + '/')
    db = db_client[db_name]
    port_mapping = db[thing_visor_collection].find_one(
        {"thingVisorID": thing_visor_ID}, {"port": 1, "_id": 0})
    print("port mapping: " + str(port_mapping)+"\n")

    # Instantiation of the Context object
    # Context object is a "map" of current virtual thing state, i.e. set of NGSI-LD properties
    LampActuatorContext = Context()

    # contexts is a map of Context, one per virtual things handled by the Thing Visor
    contexts = {v_thing_ID: LampActuatorContext}

    # JSON schemas for JSON validation
    with open('commandRequestSchema.json', 'r') as f:
        schema_data = f.read()
    commandRequestSchema = json.loads(schema_data)

    # Finally run threads for control and data

    mqtt_control_client = mqtt.Client()
    mqtt_data_client = mqtt.Client()

    # threadPoolExecutor of size one to handle one command at a time in a fifo order
    executor = ThreadPoolExecutor(1)
    # Class used to handle data and commands of the actuator
    data_thread = LampActuatorThread()
    data_thread.start()

    mqtt_control_thread = MqttControlThread()  # mqtt control thread
    mqtt_control_thread.start()

    while True:
        try:
            time.sleep(3)
        except:
            print("KeyboardInterrupt"+"\n")
            time.sleep(1)
            os._exit(1)
