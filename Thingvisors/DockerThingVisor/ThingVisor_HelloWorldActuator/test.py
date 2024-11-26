import paho.mqtt.client as mqtt
import json

BROKER_IP = "localhost"  # MQTTブローカーのIP (docker-composeではlocalhost)
BROKER_PORT = 1883
V_THING_PREFIX = "vThing"
THING_VISOR_ID = "myThingVisor1"
V_THING_ID = f"{THING_VISOR_ID}/helloWorld"
IN_CONTROL_SUFFIX = "c_in"

def send_get_context_request():
    client = mqtt.Client()
    client.connect(BROKER_IP, BROKER_PORT, 30)
    
    message = {
        "command": "getContextRequest",
        "vSiloID": "testSilo1"
    }
    topic = f"{V_THING_PREFIX}/{V_THING_ID}/{IN_CONTROL_SUFFIX}"
    
    client.publish(topic, json.dumps(message))
    print(f"Published to topic {topic}: {message}")

    client.disconnect()

if __name__ == "__main__":
    send_get_context_request()
