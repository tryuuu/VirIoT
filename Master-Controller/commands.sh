#docker build --platform=linux/x86_64 -t tryuu/my-viriot-master-controller:latest . # x86向けのイメージをビルドする
#docker push tryuu/my-viriot-master-controller:latest

docker buildx create --name mybuilder --use
docker buildx inspect --bootstrap
docker buildx build --platform linux/amd64,linux/arm64 -t tryuu/my-viriot-master-controller:latest --push .
