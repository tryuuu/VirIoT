import json
import time
import requests
import urllib.parse
from threading import Thread
import paho.mqtt.client as mqtt

# ダミーデータの設定
cities = ["Rome", "London", "Berlin"]
v_things = [
    {"city": "Rome", "vThing": {"id": "vWeather/rome_temp"}, "type": "temp", "dataType": "temperature", "thing": "thermometer", "topic": "test/topic"},
    {"city": "London", "vThing": {"id": "vWeather/london_temp"}, "type": "temp", "dataType": "temperature", "thing": "thermometer", "topic": "test/topic"},
    {"city": "Berlin", "vThing": {"id": "vWeather/berlin_temp"}, "type": "temp", "dataType": "temperature", "thing": "thermometer", "topic": "test/topic"}
]

contexts = {v_thing["vThing"]["id"]: {} for v_thing in v_things}

# MQTTクライアントのセットアップ
mqtt_data_client = mqtt.Client()
mqtt_data_client.connect("localhost", 1883, 60)
mqtt_data_client.loop_start()

refresh_rate = 10  # データ取得の間隔（秒）
v_thing_data_suffix = "data"

# FetcherThreadクラスの定義
# 指定された都市と仮想センサーのリスト（citiesとv_things）に基づいて、初期のセンサーデータを設定
class FetcherThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        for city in cities:
            for v_thing in v_things:
                if v_thing['city'] != city:
                    continue
                v_thing_id = v_thing["vThing"]["id"]
                sens_type = v_thing["type"]
                data = 0.0 # 気温の初期値
                data_type = v_thing["dataType"]
                thing_name = v_thing["thing"]
                # 取得した天気データをNGSI-LD形式のエンティティに変換。これが出力される
                ngsi_ld_entity1 = {"@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                                   "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                                   "type": data_type,
                                   thing_name: {"type": "Property", "value": data}}
                contexts[v_thing_id] = ngsi_ld_entity1
    # MQTTブローカーにメッセージを送信
    def send_message(self, url, message):
        mqtt_data_client.publish(url, message)

    def run(self):
        while True:
            for city in cities:
                location = urllib.parse.quote(city)
                url = 'https://api.openweathermap.org/data/2.5/weather?q=' + location + '&appid=eeb5e018b6a1c71fe75391e7c6342faa'
                try:
                    r = requests.get(url, timeout=1)
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
                    data = r.json()["main"].get(sens_type, 0.0)
                    data_type = v_thing["dataType"]
                    thing_name = v_thing["thing"]

                    ngsi_ld_entity1 = {"@context": ["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"],
                                       "id": "urn:ngsi-ld:" + city + ":" + sens_type,
                                       "type": data_type,
                                       thing_name: {"type": "Property", "value": data}}

                    contexts[v_thing_id] = ngsi_ld_entity1
                    message = {"data": [ngsi_ld_entity1], "meta": {"vThingID": v_thing_id}}
                    print(str(message))
                    # ブローカーに都市ごとの気温の情報を含んだメッセージを送信
                    self.send_message(v_thing["topic"] + '/' + v_thing_data_suffix, json.dumps(message))
                    time.sleep(0.2)
            time.sleep(refresh_rate)

# テストの実行
fetcher_thread = FetcherThread()
fetcher_thread.start()
