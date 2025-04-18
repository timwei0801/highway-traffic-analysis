#!/bin/bash
# 檔名: stop_db.sh

# 停止MySQL
echo "停止MySQL服務..."
brew services stop mysql
echo "MySQL已停止，現在可以安全移除硬碟"