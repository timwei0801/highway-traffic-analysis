#!/bin/bash
# 檔名: start_db.sh

# 檢查硬碟是否已掛載
if [ ! -d "/Volumes/國道資料" ]; then
    echo "硬碟未連接，請插入硬碟後再試"
    exit 1
fi

# 啟動MySQL
echo "啟動MySQL服務..."
brew services start mysql
echo "等待MySQL啟動..."
sleep 5
echo "MySQL已啟動，資料庫準備就緒"