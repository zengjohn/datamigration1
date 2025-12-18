#!/bin/bash

# 获取脚本所在目录的上一级目录（即安装根目录）
BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..
cd $BASE_DIR

APP_NAME="moveprog"
# 注意：这里指向 libs/etl 下的 jar
JAR_PATH="${BASE_DIR}/libs/etl/${APP_NAME}*.jar"
CONFIG_PATH="${BASE_DIR}/config/"

# 检查程序是否已运行
PID=$(ps -ef | grep "$APP_NAME" | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
    echo "$APP_NAME is already running with PID: $PID"
    exit 1
fi

echo "Starting $APP_NAME..."

# 启动命令关键点：
# 1. -Dloader.path=libs/ : 告诉 Spring Boot 去 libs 目录下找依赖 (如果是瘦Jar模式)
# 2. --spring.config.location=config/ : 强制指定配置文件目录
# 3. jar 包路径指向 libs/etl/
nohup java -server -Xms4g -Xmx8g \
  -XX:+UseZGC -XX:+ZGenerational \
  -Dfile.encoding=UTF-8 \
  -jar $JAR_PATH \
  --spring.config.location=$CONFIG_PATH \
  > /dev/null 2>&1 &

echo "$APP_NAME started."