#! /bin/bash
sudo docker login
sudo docker build -t tryuu/thingvisor_helloworld_main:hello .
sudo docker push tryuu/thingvisor_helloworld_main:hello