FROM --platform=linux/arm64 python:3.6.10-slim as builder

# 必要なパッケージをインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    screen  # screenコマンドをインストール

# アプリケーションのワークディレクトリを設定
WORKDIR /app
COPY . /app

# Python依存関係をインストール
RUN pip install --no-cache-dir -r requirements.txt

# コンテナがリッスンするポートを指定
EXPOSE 8090

# コンテナ起動時に実行されるコマンド
CMD ["/bin/bash", "start.sh"]