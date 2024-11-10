#!/bin/bash
export thingVisorID=my_thing_visor_id
export systemDatabaseIP=mongo
export systemDatabasePort=27017

echo "Starting sidecar..."

# サイドカーのPythonスクリプトを実行
python3 sidecar.py
