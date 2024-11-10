#!/bin/bash

# 環境変数の設定
export thingVisorID=my_thing_visor_id
export systemDatabaseIP=mongo
export systemDatabasePort=27017

echo "Starting Python script"
/usr/local/bin/python3 /app/main.py

# Pythonスクリプトの終了ステータスを取得
status=$?

if [ $status -ne 0 ]; then
  echo "Python script failed with status $status"
else
  echo "Python script finished successfully"
fi

# 無限に待機
sleep infinity
