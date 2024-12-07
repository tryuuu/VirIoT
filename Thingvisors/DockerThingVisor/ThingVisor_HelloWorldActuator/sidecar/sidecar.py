import os
import time
import json
import traceback
from pymongo import MongoClient
from threading import Thread
import paho.mqtt.client as mqtt
from context import Context
import grpc
import proto.thingvisor_pb2 as thingvisor_pb2
import proto.thingvisor_pb2_grpc as thingvisor_pb2_grpc
import time


class ThingVisorConfig:
    MAX_RETRY = 3

    def __init__(self):
        self.thing_visor_ID = "test"
        self.tv_control_prefix = "TV"
        self.v_thing_prefix = "vThing"
        self.in_data_suffix = "data_in"
        self.out_data_suffix = "data_out"
        self.in_control_suffix = "c_in"
        self.out_control_suffix = "c_out"
        self.v_silo_prefix = "vSilo"
        self.thing_visor_collection = "thingVisorC"
        self.db_IP = os.getenv("systemDatabaseIP")
        self.db_port = os.getenv("systemDatabasePort")
        self.db_client = MongoClient('mongodb://'+self.db_IP+':'+str(self.db_port)+'/')
        # DBからデータを取得するのではなく一旦仮想的な値を用意
        self.tv_entry = {
        "thingVisorID": self.thing_visor_ID,
        "MQTTDataBroker": {"ip": "mqtt", "port": 1883},
        "MQTTControlBroker": {"ip": "mqtt", "port": 1883},
        "params": '{"rate": 5}'
        }

    def save_to_file(self, filepath="/app/data/entry.json"):
        parameters = self.tv_entry["params"]
        if parameters:
            params = json.loads(parameters.replace("'", '"'))
        else:
            params = parameters
        data = {
            "thing_visor_ID": self.thing_visor_ID,
            "thing_visor_collection": self.thing_visor_collection,
            "db_IP": self.db_IP,
            "db_port": self.db_port,
            "tv_control_prefix": self.tv_control_prefix,
            "v_thing_prefix": self.v_thing_prefix,
            "in_data_suffix": self.in_data_suffix,
            "out_data_suffix": self.out_data_suffix,
            "in_control_suffix": self.in_control_suffix,
            "out_control_suffix": self.out_control_suffix,
            "v_silo_prefix": self.v_silo_prefix,
            "MQTTDataBrokerIP": self.tv_entry["MQTTDataBroker"]["ip"],
            "MQTTDataBrokerPort": self.tv_entry["MQTTDataBroker"]["port"],
            "MQTTControlBrokerIP": self.tv_entry["MQTTControlBroker"]["ip"],
            "MQTTControlBrokerPort": self.tv_entry["MQTTControlBroker"]["port"],
            "params": params,
            "test"  : "aaa"
        }
        with open(filepath, 'w') as f:
            json.dump(data, f)
        print("初期化完了", flush=True)


class ControlThread(Thread):
    def __init__(self,config):
        super().__init__()
        self.config = config
        self.mqtt_control_client = mqtt.Client()
        self.v_thing_ID = self.config.thing_visor_ID + "/helloWorld"
        self.v_thing = {
            "label": "helloWorld",
            "id": self.v_thing_ID,
            "description": "HelloWorldActuator"
        }
        self.LampActuatorContext = Context()
        self.db_client = MongoClient('mongodb://'+self.config.db_IP+':'+str(self.config.db_port)+'/')
        self.MQTT_control_broker_IP = self.config.tv_entry["MQTTControlBroker"]["ip"]
        self.MQTT_control_broker_port = self.config.tv_entry["MQTTControlBroker"]["port"]
        self.notifier = NotifyMain(host="main", port=50051)
        print("Control Thread初期化完了", flush=True)

    def on_message_get_thing_context(self, jres):
        print("bbb", flush=True)
        silo_id = jres["vSiloID"]
        message = {"command": "getContextResponse", "data": self.LampActuatorContext.get_all(), "meta": {
            "vThingID": self.v_thing_ID}}
        print(message, flush=True)
        self.mqtt_control_client.publish(self.config.v_silo_prefix + "/" + silo_id +
                                    "/" + self.config.in_control_suffix, json.dumps(message))

    def send_destroy_v_thing_message(self):
        msg = {"command": "deleteVThing", "vThingID": self.v_thing_ID, "vSiloID": "ALL"}
        topic = f"{self.config.v_thing_prefix}/{self.v_thing_ID}/{self.config.out_control_suffix}"
        self.mqtt_control_client.publish(topic, json.dumps(msg))

    def send_destroy_thing_visor_ack_message(self):
        msg = {"command": "destroyTVAck", "thingVisorID": self.config.thing_visor_ID}
        topic = f"{self.config.tv_control_prefix}/{self.config.thing_visor_ID}/{self.config.out_control_suffix}"
        self.mqtt_control_client.publish(topic, json.dumps(msg))
        return

    def on_message_destroy_thing_visor(self, jres):
        self.db_client.close()
        self.send_destroy_v_thing_message()
        self.send_destroy_thing_visor_ack_message()
        print("Shutdown completed")

    def on_message_control_in_vThing(self, mosq, obj, msg):
        print("aaaa", flush=True)
        payload = msg.payload.decode("utf-8", "ignore")
        jres = json.loads(payload.replace("\'", "\""))
        print(jres, flush=True)
        # dataというkeyがある場合
        if "data" in jres:
            self.call_datathread_grpc(jres)
            return
        try:
            command_type = jres["command"]
            if command_type == "getContextRequest":
                self.on_message_get_thing_context(jres)
        except Exception as ex:
            traceback.print_exc()
        return

    def on_message_control_in_TV(self, mosq, obj, msg):
        print("ccc", flush=True)
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
    
    # gRPCでmainにデータを送信
    def call_datathread_grpc(self, data):
        try:
            self.notifier.send_data(data=json.dumps(data))
        except Exception as ex:
            print(f"Error in call_datathread_grpc: {ex}")


    def run(self):
        print("Thread mqtt control started\n", flush=True)
        result = self.mqtt_control_client.connect(self.MQTT_control_broker_IP, self.MQTT_control_broker_port, 30) 
        print(result, flush=True)
        v_thing_message = {
            "command": "createVThing",
            "thingVisorID": self.config.thing_visor_ID,
            "vThing": self.v_thing
        }
        self.mqtt_control_client.publish(
            f"{self.config.tv_control_prefix}/{self.config.thing_visor_ID}/{self.config.out_control_suffix}",
            json.dumps(v_thing_message)
        )
        
        self.mqtt_control_client.message_callback_add(
            f"{self.config.v_thing_prefix}/{self.v_thing_ID}/{self.config.in_control_suffix}",
            self.on_message_control_in_vThing
        )
        # 引数はtopic(一致していたら呼び出される)
        self.mqtt_control_client.message_callback_add(
            f"{self.config.tv_control_prefix}/{self.config.thing_visor_ID}/{self.config.in_control_suffix}",
            self.on_message_control_in_TV
        )
        # mainに渡す(同様にしてsubscribeしておく)
        v_thing_topic = "v_thing_prefix/" + self.config.thing_visor_ID
        self.mqtt_control_client.message_callback_add(
            f"{v_thing_topic}/{self.config.in_data_suffix}",
            self.on_message_control_in_vThing
        )
        # 引数はtopic
        self.mqtt_control_client.subscribe(f"{self.config.v_thing_prefix}/{self.v_thing_ID}/{self.config.in_control_suffix}")
        self.mqtt_control_client.subscribe(f"{self.config.tv_control_prefix}/{self.config.thing_visor_ID}/{self.config.in_control_suffix}")
        self.mqtt_control_client.subscribe(f"{v_thing_topic}/{self.config.in_data_suffix}")

        self.mqtt_control_client.loop_forever()
        print("Thread '" + self.name + "' terminated\n")

class NotifyMain:
    def __init__(self, host="localhost", port=50051):
        time.sleep(5)
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = thingvisor_pb2_grpc.ThingVisorNotifierStub(self.channel)
    
    def notify(self, message="Initialization complete"):
        try:
            request = thingvisor_pb2.InitializationRequest(message=message) # .protoで定義したRequest(クライアント側)
            response = self.stub.NotifyInitializationComplete(request) # .protoで定義したResponse(サーバ側)
            print(f"Response from main: {response.status}", flush=True)
        except Exception as e:
            print(f"Error in notify_main: {e}")

    def send_data(self, data):
        try:
            print("sending data to main..", flush=True)
            request = thingvisor_pb2.DataRequest(data=data)
            response = self.stub.SendData(request)
            print(f"Response from main: {response.status}", flush=True)
        except Exception as e:
            print(f"Error in send_data: {e}", flush=True)


if __name__ == '__main__':
    config_thingvisor = ThingVisorConfig()
    config_thingvisor.save_to_file()
    # 初期化完了を main に通知
    notifier = NotifyMain(host="localhost", port=50051)
    notifier.notify()
    control_thread = ControlThread(config_thingvisor)  
    control_thread.start()