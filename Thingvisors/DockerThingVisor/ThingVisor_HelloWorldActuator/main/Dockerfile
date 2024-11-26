# ベースイメージとしてPython 3.6を指定
FROM python:3.8

RUN apt update
RUN apt -y install screen
RUN pip3 install paho-mqtt requests pymongo jsonmerge grpcio grpcio-tools protobuf==5.28.1

COPY . /app
WORKDIR /app
ENV PYTHONPATH="/app:/app/proto"

ENTRYPOINT []
CMD ["/bin/bash", "./start.sh"]
