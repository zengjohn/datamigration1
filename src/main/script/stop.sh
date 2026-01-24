#!/bin/bash

# 获取基准目录
BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..
APP_NAME="etl"
# 必须和 start.sh 中的查找逻辑一致，以确保匹配到正确的进程
JAR_PATH_PATTERN="${BASE_DIR}/libs/etl/${APP_NAME}*.jar"

# 查找 PID
# 注意：这里 grep 的是具体的 JAR 路径，防止误杀同名的其他 Java 进程
PID=$(ps -ef | grep -f <(ls $JAR_PATH_PATTERN) | grep -v grep | awk '{print $2}')

if [ -z "$PID" ]; then
    echo "$APP_NAME is not running."
    exit 0
fi

echo "Stopping $APP_NAME (PID: $PID)..."

# 发送终止信号 (SIGTERM)
kill "$PID"

# 循环等待进程结束 (最多等待 10 秒)
for i in {1..10}; do
    if ps -p "$PID" > /dev/null; then
        sleep 1
    else
        echo "$APP_NAME stopped successfully."
        exit 0
    fi
done

# 如果还在运行，强制杀死
echo "Force killing $APP_NAME..."
kill -9 "$PID"
echo "$APP_NAME killed."