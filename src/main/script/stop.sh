#!/bin/bash

APP_NAME="moveprog"

PID=$(ps -ef | grep "$APP_NAME" | grep -v grep | awk '{print $2}')

if [ -z "$PID" ]; then
    echo "$APP_NAME is not running."
    exit 0
fi

echo "Stopping $APP_NAME (PID: $PID)..."
kill $PID

# 等待循环
for i in {1..10}; do
    if ps -p $PID > /dev/null; then
        sleep 1
    else
        echo "$APP_NAME stopped successfully."
        exit 0
    fi
done

echo "Force killing $APP_NAME..."
kill -9 $PID