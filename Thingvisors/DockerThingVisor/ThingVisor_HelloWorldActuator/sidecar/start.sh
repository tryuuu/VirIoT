#!/bin/bash
export thingVisorID=test
export systemDatabaseIP=mongo
export systemDatabasePort=27017

echo "Starting sidecar..."

# サイドカーのPythonスクリプトを実行
python3 sidecar.py
