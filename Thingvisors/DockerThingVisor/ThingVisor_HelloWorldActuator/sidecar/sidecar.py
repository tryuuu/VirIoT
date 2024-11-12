import os
import time
import json
from pymongo import MongoClient

MAX_RETRY = 3


thing_visor_ID = os.environ["thingVisorID"]

# 完全に共通
tv_control_prefix = "TV"
v_thing_prefix = "vThing"
in_data_suffix = "data_in"
out_data_suffix = "data_out"
in_control_suffix = "c_in"
out_control_suffix = "c_out"
v_silo_prefix = "vSilo"
thing_visor_collection = "thingVisorC"

# db_IP = os.environ['systemDatabaseIP']
# db_port = os.environ['systemDatabasePort']
# db_name = "viriotDB"
# db_client = MongoClient(f'mongodb://{db_IP}:{db_port}/')
# db = db_client[db_name]

print("MongoDB接続成功")

print("thingVisorエントリを検索しています...")
# tv_entry = db[thing_visor_collection].find_one({"thingVisorID": thing_visor_ID})
tv_entry = {
        "thingVisorID": thing_visor_ID,
        "MQTTDataBroker": {"ip": "192.168.80.240", "port": 30000},
        "MQTTControlBroker": {"ip": "192.168.80.240", "port": 30000},
        "params": '{"rate": 5}'
    }
print(f"検索結果: {tv_entry}")

valid_tv_entry = False
for x in range(MAX_RETRY):
    if tv_entry is not None:
        valid_tv_entry = True
        break
    print(f"再試行 {x + 1}/{MAX_RETRY}")
    time.sleep(3)
    tv_entry = db[thing_visor_collection].find_one({"thingVisorID": thing_visor_ID})

if not valid_tv_entry:
    print(f"エラー: ThingVisorエントリが見つかりません: {thing_visor_ID}")
    exit()

try:
    print("データベースからパラメータをインポートしています...")
    MQTT_data_broker_IP = tv_entry["MQTTDataBroker"]["ip"]
    MQTT_data_broker_port = int(tv_entry["MQTTDataBroker"]["port"])
    MQTT_control_broker_IP = tv_entry["MQTTControlBroker"]["ip"]
    MQTT_control_broker_port = int(tv_entry["MQTTControlBroker"]["port"])

    parameters = tv_entry["params"]
    if parameters:
        params = json.loads(parameters.replace("'", '"'))
    else:
        params = parameters
    print(f"インポートされたパラメータ: {params}")

except json.decoder.JSONDecodeError:
    print("パラメータのJSONデコードエラー" + "\n")
    exit()
except Exception as e:
    print(f"エラー: tv_entryにパラメータが見つかりません: {e}")
    exit()

# 必要なデータを保存
data = {
    "thing_visor_ID": thing_visor_ID,
    "thing_visor_collection": thing_visor_collection,
    "db_IP": db_IP,
    "db_port": db_port,
    "tv_control_prefix": tv_control_prefix,
    "v_thing_prefix": v_thing_prefix,
    "in_data_suffix": in_data_suffix,
    "out_data_suffix": out_data_suffix,
    "in_control_suffix": in_control_suffix,
    "out_control_suffix": out_control_suffix,
    "v_silo_prefix": v_silo_prefix,
    "MQTTDataBrokerIP": MQTT_data_broker_IP,
    "MQTTDataBrokerPort": MQTT_data_broker_port,
    "MQTTControlBrokerIP": MQTT_control_broker_IP,
    "MQTTControlBrokerPort": MQTT_control_broker_port,
    "params": params
}
with open('/app/data/entry.json', 'w') as f:
    json.dump(data, f)

print("初期化完了")
