# ベースイメージとしてPython 3.6を指定
FROM python:3.6

RUN apt update
RUN apt -y install screen
RUN pip3 install pymongo

COPY . /app
WORKDIR /app

ENTRYPOINT []
CMD ["/bin/bash", "./start.sh"]
