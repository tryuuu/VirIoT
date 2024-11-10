#! /bin/bash
sudo docker login
sudo docker build -t tryuu/thingvisor_helloworld_sidecar:hello .
sudo docker push tryuu/thingvisor_helloworld_sidecar:hello