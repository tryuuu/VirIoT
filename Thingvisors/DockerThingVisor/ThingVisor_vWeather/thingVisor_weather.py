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

# Thing Hypervisor copying data from a oneM2M (Mobius) container
import traceback

import paho.mqtt.client as mqtt
import time
import os
from threading import Thread
import requests
import json
import urllib
from pymongo import MongoClient
from context import Context

from concurrent.futures import ThreadPoolExecutor

# -*- coding: utf-8 -*-

# 天気データを収集し、それをMQTTブローカーに送信する仮想センサーを実装するためのクラス
class FetcherThread(Thread):
    # クラスのインスタンスが作成された際に実行される
    def __init__(self):
        Thread.__init__(self)
        for city in cities:
            for v_thing in v_things: # v_thingsはmain関数で作成
                if v_thing['city'] != city:
                    continue
                v_thing_id = v_thing["vThing"]["id"]  # e.g. "vWeather/rome_temp"
                sens_type = v_thing["type"]  # e.g. "temp"
                data = 0.0  # 気温の初期値
                data_type = v_thing["dataType"]  # e.g. "temperature"
                thing_name = v_thing["thing"]  # e.g. "thermometer"
                # 取得した天気データをNGSI-LD形式のエンティティに変換。これがブローカーに送られる
                ngsi_ld_entity1 = {"@context":["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                                   "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                                   "type": data_type,
                                   thing_name: {"type": "Property", "value": data}}
                # 仮想センサの初期状態の設定。contextsはmain関数で定義
                contexts[v_thing_id].set_all([ngsi_ld_entity1])

    # MQTTブローカーにメッセージを送信する関数
    def send_message(self, url, message):
        mqtt_data_client.publish(url, message)
    
    def run(self):
        while True:
            for city in cities:
                location = urllib.parse.quote(city)
                url = 'https://api.openweathermap.org/data/2.5/weather?q=' + location + '&appid=f5dd231b269189f270094476aa8399a5'
                try:
                    r = requests.get(url, timeout=1)
                except requests.exceptions.Timeout:
                    print("Request timed out - " + city)
                except Exception as ex:
                    print("Some Error with city " + city)
                    print(ex.with_traceback())
                for v_thing in v_things:
                    if v_thing['city'] != city:
                        continue
                    v_thing_id = v_thing["vThing"]["id"]        # e.g. "vWeather/rome_temp"
                    sens_type = v_thing["type"]                 # e.g. "temp"
                    data = r.json()["main"][sens_type]          # e.g. 気温のデータ
                    data_type = v_thing["dataType"]             # e.g. "temperature"
                    thing_name = v_thing["thing"]               # e.g. "thermometer"

                    ngsi_ld_entity1 = {"@context":["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                                       "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                                       "type": data_type,
                                       thing_name: {"type": "Property", "value": data}}

                    contexts[v_thing_id].update([ngsi_ld_entity1])
                    # 実際のデータを含むメッセージを作成
                    message = {"data": [ngsi_ld_entity1], "meta": {"vThingID": v_thing_id}}
                    print(str(message))
                    # publish received data to data topic by using neutral format
                    # ブローカーに都市ごとの気温の情報を含んだメッセージを送信。send_message関数を使用
                    future = executor.submit(self.send_message, v_thing["topic"] + '/'+v_thing_data_suffix, json.dumps(message))
                    #mqtt_data_client.publish(v_thing["topic"] + '/'+v_thing_data_suffix, json.dumps(message))
                    time.sleep(0.2)
            time.sleep(refresh_rate)

# MQTTブローカーとの安定的な接続を維持(FetcherThreadの補助的な役割)
class mqttDataThread(Thread):
    # mqtt client for sending data
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        print("Thread mqtt data started")
        global mqtt_data_client
        mqtt_data_client.connect(MQTT_data_broker_IP, MQTT_data_broker_port, 30)
        mqtt_data_client.loop_forever()
        print("Thread '" + self.name + "' terminated")


class mqttControlThread(Thread):
    # ブローカーにレスポンスを返す
    def on_message_get_thing_context(self, jres):
        # 受信したjson responseから情報を抽出
        silo_id = jres["vSiloID"]
        v_thing_id = jres["vThingID"]
        # vThingIDを含んだレスポンスメッセージを作成
        message = {"command": "getContextResponse", "data": contexts[v_thing_id].get_all(), "meta": {"vThingID": v_thing_id}}
        # メッセージをMQTTブローカー(トピック)に送信(パブリッシュ)
        mqtt_control_client.publish(v_silo_prefix + "/" + silo_id + "/" + in_control_suffix, json.dumps(message))

    # すべてのvThingに対して削除メッセージを送信
    def send_destroy_v_thing_message(self):
        for v_thing in v_things:
            v_thing_ID = v_thing["vThing"]["id"]
            msg = {"command": "deleteVThing", "vThingID": v_thing_ID, "vSiloID": "ALL"}
            mqtt_control_client.publish(v_thing_prefix + "/" + v_thing_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    # ThingVisorの削除確認メッセージを送信
    def send_destroy_thing_visor_ack_message(self):
        msg = {"command": "destroyTVAck", "thingVisorID": thing_visor_ID}
        mqtt_control_client.publish(tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix, json.dumps(msg))
        return

    # ThingVisorの削除リクエストを受け取ったときに実行される処理    
    def on_message_destroy_thing_visor(self, jres):
        global db_client
        # データベースクライアントの接続を閉じる
        db_client.close()
        # 上二つのメソッドを実行
        self.send_destroy_v_thing_message()
        self.send_destroy_thing_visor_ack_message()
        print("Shutdown completed")

    # handler for mqtt control topics
    def __init__(self):
        Thread.__init__(self)

    # 特定のMQTTブローカー(トピック)からSubscriberが受信したメッセージ(vThingの情報取得)を処理
    def on_message_in_control_vThing(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload))
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            # 特定の仮想センサー（vThing）の現在のコンテキスト情報をリクエスト
            if command_type == "getContextRequest":
                # 上のメソッドを実行
                self.on_message_get_thing_context(jres)
        except Exception as ex:
            traceback.print_exc()
        return
    
    # 特定のMQTTブローカー(トピック)からSubscriberが受信したメッセージ(ThingVisor削除リクエスト)を処理
    def on_message_in_control_TV(self, mosq, obj, msg):
        payload = msg.payload.decode("utf-8", "ignore")
        print(msg.topic + " " + str(payload))
        jres = json.loads(payload.replace("\'", "\""))
        try:
            command_type = jres["command"]
            # ThingVisorを削除するためのリクエスト
            if command_type == "destroyTV":
                self.on_message_destroy_thing_visor(jres)
        except Exception as ex:
            traceback.print_exc()
        return 'invalid command'
    
    # Classが呼び出されると実行される
    def run(self):
        print("Thread mqtt control started")
        global mqtt_control_client
        # 指定されたMQTTブローカーに接続
        mqtt_control_client.connect(MQTT_control_broker_IP, MQTT_control_broker_port, 30)

        # Publish on the thingVisor out_control topic the createVThing command and other parameters for each vThing
        for v_thing in v_things:
            v_thing_topic = v_thing["topic"]
            v_thing_message = {"command": "createVThing",
                               "thingVisorID": thing_visor_ID,
                               "vThing": v_thing["vThing"]}
            # 各仮想センサー（vThing）の作成メッセージを生成し、MQTTブローカーにパブリッシュ
            mqtt_control_client.publish(tv_control_prefix + "/" + thing_visor_ID + "/" + out_control_suffix,
                                        json.dumps(v_thing_message))

            # Add message callbacks that will only trigger on a specific subscription match
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
    MAX_RETRY = 3

    print("環境変数を読み込んでいます...")
    thing_visor_ID = os.environ["thingVisorID"]
    db_IP = os.environ['systemDatabaseIP']
    db_port = os.environ['systemDatabasePort']
    print(f"thingVisorID: {thing_visor_ID}, systemDatabaseIP: {db_IP}, systemDatabasePort: {db_port}")

    # Mosquitto設定
    tv_control_prefix = "TV" 
    v_thing_prefix = "vThing" 
    v_thing_data_suffix = "data_out"
    in_control_suffix = "c_in"
    out_control_suffix = "c_out"
    v_silo_prefix = "vSilo"
    mqtt_control_client = mqtt.Client()
    mqtt_data_client = mqtt.Client()

    # MongoDB設定
    print("MongoDBに接続しています...")
    time.sleep(1.5) 
    db_name = "viriotDB" 
    thing_visor_collection = "thingVisorC"
    db_client = MongoClient(f'mongodb://{db_IP}:{db_port}/')
    db = db_client[db_name]

    print("MongoDB接続成功")

    print("thingVisorエントリを検索しています...")
    # mongodbにデータを挿入しないとここでエラーになる
    tv_entry = db[thing_visor_collection].find_one({"thingVisorID": thing_visor_ID})
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

    # 仮想Thingとそのコンテキストオブジェクトのマッピング
    contexts = {}

    cities = params["cities"]
    sensors = [{"id": "_temp", "type": "temp", "description": "現在の気温（ケルビン）",
                "dataType": "temperature", "thing": "thermometer"},
               {"id": "_humidity", "type": "humidity", "dataType": "humidity",
                "description": "現在の湿度（%）", "thing": "hygrometer"},
               {"id": "_pressure", "type": "pressure", "dataType": "pressure",
                "description": "現在の気圧（hPa）", "thing": "barometer"}]

    if params and "rate" in params.keys():
        refresh_rate = params["rate"]
    else:
        refresh_rate = 300

    # 仮想thingの初期化
    v_things = []
    # 仮想thingの作成
    for city in cities:
        for sens in sensors:
            thing = sens["thing"] # e.g. "thermometer"
            label = sens["thing"] + " in " + str(city) # e.g. "thermometer in rome"
            identifier = thing_visor_ID + "/" + city + sens["id"] # e.g. "vWeather/rome_temp"
            description = sens["description"] # e.g. "現在の気温（ケルビン）"
            topic = v_thing_prefix + "/" + identifier # e.g. "vThing/vWeather/rome_temp"
            v_things.append({"vThing": {"label": label, "id": identifier, "description": description},
                             "topic": v_thing_prefix + "/" + identifier, "type": sens["type"],
                             "dataType": sens["dataType"], "city": city, "thing": thing})
            contexts[identifier] = Context()

    print(f"仮想Thingのマッピング: {v_things}")

    port_mapping = db[thing_visor_collection].find_one({"thingVisorID": thing_visor_ID}, {"port": 1, "_id": 0})
    print("ポートマッピング: " + str(port_mapping))

    executor = ThreadPoolExecutor(1)

    data_thread = FetcherThread() 
    data_thread.start() # runメソッドを実行
    print("データ取得スレッド開始")

    mqtt_control_thread = mqttControlThread()
    mqtt_control_thread.start()
    print("MQTTコントロールスレッド開始")

    mqtt_data_thread = mqttDataThread()
    mqtt_data_thread.start()
    print("MQTTデータスレッド開始")

    while True:
        try:
            time.sleep(3)
        except:
            print("キーボード割り込み")
            time.sleep(1)
            os._exit(1)

