#!/bin/bash

# =======================================================================
# 1. 初始化路径配置
# =======================================================================
# 获取当前脚本所在目录的上一级目录作为基准目录 (即解压后的根目录)
BASE_DIR=$(cd "$(dirname "$0")"; pwd)/..
cd "$BASE_DIR" || exit

APP_NAME="etl"
# 自动查找主 Jar 包 (位于 libs/etl/ 目录下)
JAR_PATH=$(find "${BASE_DIR}/libs/etl" -name "${APP_NAME}*.jar" | head -n 1)
CONFIG_DIR="${BASE_DIR}/config/"
LOG_DIR="${BASE_DIR}/logs"

# 检查 Jar 包是否存在
if [ -z "$JAR_PATH" ] || [ ! -f "$JAR_PATH" ]; then
    echo "Error: 找不到主程序 Jar 包，请检查 libs/etl/ 目录。"
    exit 1
fi

# 创建日志目录
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# =======================================================================
# 2. 检查程序是否已运行
# =======================================================================
PID=$(ps -ef | grep "$JAR_PATH" | grep -v grep | awk '{print $2}')
if [ -n "$PID" ]; then
    echo "$APP_NAME is already running with PID: $PID"
    exit 1
fi

# =======================================================================
# 3. JVM 配置 (针对 Java 21 优化)
# =======================================================================
# -Xms / -Xmx: 堆内存设置，根据服务器实际配置调整 (例如 4g, 8g)
# -XX:+UseZGC -XX:+ZGenerational: Java 21 推荐使用分代 ZGC，延迟极低
JAVA_OPTS="-server -Xms2g -Xmx2g \
  -XX:+UseZGC -XX:+ZGenerational \
  -Dfile.encoding=UTF-8 \
  -Duser.timezone=Asia/Shanghai"

# =======================================================================
# 4. 启动应用
# =======================================================================
echo "Starting $APP_NAME..."
echo "JAVA_HOME: $JAVA_HOME"
echo "JAR: $JAR_PATH"
echo "CONFIG: $CONFIG_DIR"

# 核心启动命令说明：
# --spring.config.location: 强制指定配置文件目录，方便运维修改配置而不重打 Jar 包
# > /dev/null 2>&1: 标准输出和错误输出丢弃（由 logback-spring.xml 接管日志文件写入）
nohup java $JAVA_OPTS \
  -jar "$JAR_PATH" \
  --spring.config.location="$CONFIG_DIR" \
  > start.out 2>&1 &

# 获取新启动的 PID
NEW_PID=$(ps -ef | grep "$JAR_PATH" | grep -v grep | awk '{print $2}')

if [ -n "$NEW_PID" ]; then
    echo "$APP_NAME started successfully. PID: $NEW_PID"
    echo "Logs are located in $LOG_DIR"
else
    echo "Failed to start $APP_NAME."
    exit 1
fi