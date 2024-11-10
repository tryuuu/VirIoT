#!/bin/bash

# 環境変数の設定
export thingVisorID=my_thing_visor_id
export systemDatabaseIP=mongo
export systemDatabasePort=27017


echo "Starting Python script"
/usr/local/bin/python3 /app/thingVisor_weather.py
echo "Python script finished"

# 無限に待機
sleep infinity
