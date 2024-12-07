#! /bin/bash
sudo docker login
sudo docker build -t tryuu/thingvisor_helloworld_actuator_sidecar:latest .
sudo docker push tryuu/thingvisor_helloworld_actuator_sidecar:latest