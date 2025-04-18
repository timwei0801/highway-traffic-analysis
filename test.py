import requests
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import gzip
import io
import time
import calendar
import concurrent.futures
from tqdm import tqdm
import json
import mysql.connector
from mysql.connector import Error
import pymysql
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Text, MetaData, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("highway_data_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FreewayDataCollector")

class FreewayDataCollector:
    """國道交通資料收集器"""
    
    def __init__(self, base_dir="highway_data", cwa_api_key=None, db_config=None, use_db=True, temp_dir="temp_data"):
        """初始化收集器
        
        Args:
            base_dir: 資料儲存的根目錄
            cwa_api_key: 中央氣象署API授權碼
            db_config: 資料庫配置
            use_db: 是否使用資料庫存儲資料
            temp_dir: 臨時目錄，用於暫存下載文件
        """
        self.base_dir = base_dir
        self.temp_dir = temp_dir
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)
        
        # 氣象署API授權碼
        self.cwa_api_key = cwa_api_key
        
        # 資料庫相關設定
        self.use_db = use_db
        self.db_config = db_config
        self.db_connection = None
        self.engine = None
        
        # 如果啟用資料庫，建立連接並初始化表結構
        if use_db and db_config:
            self.init_database()
        
        # 主要研究路段設定
        self.target_sections = {
            "國1楊梅-新竹": {
                "route_id": "國1",
                "start_name": "楊梅",
                "end_name": "新竹",
                "direction": "N",  # 北向
                "start_mile": 51.0,
                "end_mile": 90.0,
            },
            "國1新竹-楊梅": {
                "route_id": "國1",
                "start_name": "新竹",
                "end_name": "楊梅",
                "direction": "S",  # 南向
                "start_mile": 90.0,
                "end_mile": 51.0,
            }
        }

        # 其他關聯路段設定
        self.related_sections = {
            "國1湖口-竹北": {
                "route_id": "國1",
                "start_name": "湖口",
                "end_name": "竹北",
                "direction": "N",
                "start_mile": 72.0,
                "end_mile": 83.0,
            },
            "國3關西-竹林": {
                "route_id": "國3",
                "start_name": "關西",
                "end_name": "竹林",
                "direction": "N",
                "start_mile": 69.0,
                "end_mile": 91.0,
            },
            "國2機場-大湳": {
                "route_id": "國2",
                "start_name": "機場系統",
                "end_name": "大湳",
                "direction": "E",
                "start_mile": 0.0,
                "end_mile": 20.4,
            }
        }

        # 資料來源基礎URL
        self.base_url = "https://tisvcloud.freeway.gov.tw"
        
        # 氣象署資料API URL
        self.cwa_base_url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
        
        # 設置請求標頭
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def init_database(self):
        """初始化資料庫連接和表結構"""
        try:
            # 建立資料庫連接
            logger.info("建立資料庫連接...")
            db_url = f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
            self.engine = create_engine(db_url, pool_recycle=3600)
            
            # 建立資料庫表結構
            Base = declarative_base()
            
            # 定義VD數據表
            class VDData(Base):
                __tablename__ = 'vd_data'
                id = Column(Integer, primary_key=True, autoincrement=True)
                update_time = Column(DateTime, index=True)
                vd_id = Column(String(50), index=True)
                lane_id = Column(String(20))
                lane_type = Column(String(20))
                speed = Column(Float)
                occupancy = Column(Float)
                volume = Column(Integer)
                date = Column(String(8), index=True)
                hour = Column(Integer)
                minute = Column(Integer)
                
            # 定義天氣數據表
            class WeatherData(Base):
                __tablename__ = 'weather_data'
                id = Column(Integer, primary_key=True, autoincrement=True)
                timestamp = Column(DateTime, index=True)
                location_name = Column(String(50), index=True)
                lat = Column(Float)
                lon = Column(Float)
                temp = Column(Float)  # 溫度
                humd = Column(Float)  # 濕度
                pres = Column(Float)  # 氣壓
                wdsd = Column(Float)  # 風速
                rain = Column(Float)  # 降雨量
                h_24r = Column(Float)  # 24小時累積雨量
                visb = Column(Float)  # 能見度
                date = Column(String(8), index=True)
                
            # 定義交通事件表
            class TrafficEvent(Base):
                __tablename__ = 'traffic_events'
                id = Column(Integer, primary_key=True, autoincrement=True)
                update_time = Column(DateTime, index=True)
                event_id = Column(String(50), unique=True)
                event_type = Column(String(50))
                event_detail = Column(Text)
                road_id = Column(String(10), index=True)
                direction = Column(String(5))
                location_mile = Column(Float)
                start_time = Column(DateTime)
                end_time = Column(DateTime)
                date = Column(String(8), index=True)
            
            # 定義eTag旅行時間表
            class ETagTravelTime(Base):
                __tablename__ = 'etag_travel_time'
                id = Column(Integer, primary_key=True, autoincrement=True)
                update_time = Column(DateTime, index=True)
                pair_id = Column(String(50), index=True)
                start_point = Column(String(50))
                end_point = Column(String(50))
                road_id = Column(String(10), index=True)
                direction = Column(String(5))
                travel_time = Column(Integer)  # 秒
                speed = Column(Float)
                count = Column(Integer)
                date = Column(String(8), index=True)
                hour = Column(Integer)
                minute = Column(Integer)
            
            # 創建表
            logger.info("建立資料庫表結構...")
            Base.metadata.create_all(self.engine)
            
            # 建立會話工廠
            self.Session = sessionmaker(bind=self.engine)
            
            logger.info("資料庫初始化成功")
            return True
            
        except Exception as e:
            logger.error(f"資料庫初始化失敗: {e}")
            self.use_db = False
            return False
    
    def get_db_connection(self):
        """獲取數據庫連接"""
        if not self.use_db:
            return None
            
        try:
            connection = mysql.connector.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            if connection.is_connected():
                return connection
        except Error as e:
            logger.error(f"資料庫連接錯誤: {e}")
        
        return None
    
    def close_db_connection(self, connection):
        """關閉數據庫連接"""
        if connection and connection.is_connected():
            connection.close()
    
    def get_weekend_dates(self, start_date, end_date):
        """獲取指定日期範圍內的所有週末日期"""
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        
        weekend_dates = []
        
        # 迭代日期範圍
        current = start
        while current <= end:
            # 如果是週六(5)或週日(6)
            if current.weekday() in [5, 6]:
                weekend_dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
            
        return weekend_dates
    
    def get_non_holiday_weekdays(self, start_date, end_date, holiday_dates=None):
        """獲取指定日期範圍內的所有非假日工作日"""
        if holiday_dates is None:
            holiday_dates = []
            
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        
        weekday_dates = []
        
        # 迭代日期範圍
        current = start
        while current <= end:
            # 如果是週一到週五，且不在假日列表中
            current_str = current.strftime("%Y%m%d")
            if current.weekday() < 5 and current_str not in holiday_dates:
                weekday_dates.append(current_str)
            current += timedelta(days=1)
            
        return weekday_dates
    
    def get_file_path(self, data_type, date, filename, is_temp=False):
        """獲取文件存儲路徑"""
        base = self.temp_dir if is_temp else self.base_dir
        return os.path.join(base, data_type, date, filename)
    
    def download_file(self, url, save_path=None, is_temp=False):
        """下載資料檔案，帶進度顯示和錯誤處理"""
        try:
            # 檢查並修正URL中的錯誤部分
            if "history-list.php/history" in url:
                url = url.replace("history-list.php/history", "history")
            
            # 顯示當前下載的文件
            logger.info(f"正在下載: {url}")
            
            # 嘗試下載
            response = requests.get(url, headers=self.headers, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            # 確保文件夾存在
            if save_path:
                if is_temp:
                    save_path = save_path.replace(self.base_dir, self.temp_dir)
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                
            # 檢查是否為gzip格式
            if url.endswith('.gz'):
                try:
                    # 先將內容保存到臨時文件
                    temp_path = save_path + '.gz'
                    with open(temp_path, 'wb') as f:
                        with tqdm(total=total_size, unit='B', unit_scale=True, 
                                desc=os.path.basename(save_path)) as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                    
                    # 嘗試解壓縮
                    try:
                        with gzip.open(temp_path, 'rb') as f_in:
                            content = f_in.read()
                        
                        with open(save_path, 'wb') as f_out:
                            f_out.write(content)
                        
                        # 刪除臨時檔案
                        os.remove(temp_path)
                        
                    except Exception as gz_error:
                        # 如果解壓縮失敗，可能文件不是gzip格式
                        logger.warning(f"解壓縮失敗: {gz_error}，將直接使用原始檔案")
                        # 直接使用原始回應內容
                        with open(save_path, 'wb') as f_out:
                            f_out.write(response.content)
                        
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    
                    return save_path
                except Exception as e:
                    logger.error(f"處理檔案時出錯: {e}")
                    return None
            else:
                # 非壓縮檔案直接保存
                if save_path:
                    with open(save_path, 'wb') as f:
                        with tqdm(total=total_size, unit='B', unit_scale=True, 
                                desc=os.path.basename(save_path)) as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                    return save_path
                else:
                    return response.content
        except requests.exceptions.RequestException as e:
            # 處理請求錯誤
            logger.error(f"下載請求失敗 {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"下載處理失敗 {url}: {e}")
            return None
    
    def download_vd_data(self, date, hour, min_interval=1, save_dir=None, is_temp=False):
        """下載特定日期和小時的VD資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "VD", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每個分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/VD/{date}/VDLive_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"VDLive_{time_str}.xml")
            
            result = self.download_file(url, save_path, is_temp)
            if result:
                files.append(result)
                # 如果使用資料庫，則解析並存儲到資料庫
                if self.use_db and result:
                    try:
                        self.process_vd_file_to_db(result, date, int(hour), minute)
                    except Exception as e:
                        logger.error(f"解析VD文件到資料庫時出錯: {e}")
                        
        return files
    
    def process_vd_file_to_db(self, xml_file, date, hour, minute):
        """解析VD XML檔案並存入資料庫
        
        Args:
            xml_file: XML檔案路徑
            date: 日期格式YYYYMMDD
            hour: 小時
            minute: 分鐘
            
        Returns:
            插入的記錄數
        """
        if not self.use_db:
            return 0
            
        try:
            # 解析XML
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 提取命名空間
            namespace = None
            if '}' in root.tag:
                namespace = root.tag.split('}')[0].strip('{')
            
            # 調整XPath以處理命名空間
            namespaces = {'ns': namespace} if namespace else {}
            
            # 尋找更新時間
            update_time_elem = root.find(".//UpdateTime") or root.find(".//ns:UpdateTime", namespaces)
            update_time_str = update_time_elem.text if update_time_elem is not None else datetime.now().isoformat()
            
            try:
                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
            except ValueError:
                # 嘗試另一種格式
                update_time = datetime.strptime(update_time_str, "%Y-%m-%dT%H:%M:%S%z")
            
            # 尋找VDLive元素
            vd_lives = root.findall(".//VDLive", namespaces) or root.findall(".//ns:VDLive", namespaces)
            
            # 準備批量插入的數據
            vd_data_list = []
            
            # 遍歷所有VD設備
            for vd_live in vd_lives:
                # 尋找VDID
                vd_id_elem = vd_live.find("./VDID", namespaces) or vd_live.find("./ns:VDID", namespaces)
                vd_id = vd_id_elem.text if vd_id_elem is not None else "unknown"
                
                # 尋找並遍歷所有車道
                lanes = vd_live.findall(".//Lane", namespaces) or vd_live.findall(".//ns:Lane", namespaces)
                
                for lane in lanes:
                    # 提取車道資料
                    get_text = lambda path: (lane.find(path, namespaces) or lane.find(f"./ns:{path.split('/')[-1]}", namespaces)).text if (lane.find(path, namespaces) or lane.find(f"./ns:{path.split('/')[-1]}", namespaces)) is not None else None
                    
                    lane_id = get_text("./LaneID") or "unknown"
                    lane_type = get_text("./LaneType") or "unknown"
                    speed = float(get_text("./Speed") or 0)
                    occupancy = float(get_text("./Occupancy") or 0)
                    volume = int(float(get_text("./Volume") or 0))
                    
                    # 將車道資料加入列表
                    vd_data = {
                        'update_time': update_time,
                        'vd_id': vd_id,
                        'lane_id': lane_id,
                        'lane_type': lane_type,
                        'speed': speed,
                        'occupancy': occupancy,
                        'volume': volume,
                        'date': date,
                        'hour': hour,
                        'minute': minute
                    }
                    vd_data_list.append(vd_data)
            
            # 批量插入到資料庫
            if vd_data_list:
                session = self.Session()
                try:
                    # 準備SQL語句
                    insert_query = """
                    INSERT INTO vd_data 
                    (update_time, vd_id, lane_id, lane_type, speed, occupancy, volume, date, hour, minute)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    # 準備參數列表
                    params = [
                        (
                            data['update_time'],
                            data['vd_id'],
                            data['lane_id'],
                            data['lane_type'],
                            data['speed'],
                            data['occupancy'],
                            data['volume'],
                            data['date'],
                            data['hour'],
                            data['minute']
                        )
                        for data in vd_data_list
                    ]
                    
                    # 執行批量插入
                    conn = self.get_db_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_query, params)
                    conn.commit()
                    
                    logger.info(f"成功將 {len(vd_data_list)} 筆VD資料存入資料庫")
                    return len(vd_data_list)
                except Exception as e:
                    logger.error(f"存儲VD資料到資料庫時出錯: {e}")
                    conn.rollback()
                finally:
                    cursor.close()
                    self.close_db_connection(conn)
                    session.close()
            
            return 0
            
        except Exception as e:
            logger.error(f"解析VD XML時出錯: {e}")
            return 0
    
    def download_all_data_for_date(self, date, data_types=None, is_temp=False):
        """下載指定日期的所有類型資料，帶進度顯示"""
        if data_types is None:
            # 企劃書需要的所有資料類型
            data_types = [
                "VD", "ETag", "News", "Traffic", "Static", 
                "M03A", "M05A", "M06A", "M08A", "Jilian_ETag", "Weather"
            ]
        
        total_files = 0
        logger.info(f"===== 開始下載 {date} 的資料 =====")
        
        # 靜態資料只需下載一次
        if "Static" in data_types:
            logger.info(f"[{date}] 下載靜態資料中...")
            static_files = self.download_static_data(date, is_temp=is_temp)
            total_files += len(static_files)
            logger.info(f"[{date}] 靜態資料下載完成: {len(static_files)} 個檔案")
        
        # 氣象資料
        if "Weather" in data_types and self.cwa_api_key:
            logger.info(f"[{date}] 下載氣象資料中...")
            weather_files = self.download_weather_data(date, is_temp=is_temp)
            total_files += len(weather_files)
            logger.info(f"[{date}] 氣象資料下載完成: {len(weather_files)} 個檔案")
        
        # 動態資料需要按小時下載
        for hour in tqdm(range(24), desc=f"[{date}] 處理24小時資料", ncols=100):
            hour_str = f"{hour:02d}"
            
            if "VD" in data_types:
                logger.info(f"[{date}] 下載 {hour_str}時 VD 資料中...")
                vd_files = self.download_vd_data(date, hour_str, is_temp=is_temp)
                total_files += len(vd_files)
                logger.info(f"[{date}] {hour_str}時 VD 資料下載完成: {len(vd_files)} 個檔案")
            
            if "ETag" in data_types:
                logger.info(f"[{date}] 下載 {hour_str}時 ETag 資料中...")
                etag_files = self.download_etag_data(date, hour_str, is_temp=is_temp)
                total_files += len(etag_files)
                logger.info(f"[{date}] {hour_str}時 ETag 資料下載完成: {len(etag_files)} 個檔案")
            
            if "News" in data_types:
                logger.info(f"[{date}] 下載 {hour_str}時 事件資料中...")
                news_files = self.download_news_data(date, hour_str, is_temp=is_temp)
                total_files += len(news_files)
                logger.info(f"[{date}] {hour_str}時 事件資料下載完成: {len(news_files)} 個檔案")
            
            if "Traffic" in data_types:
                logger.info(f"[{date}] 下載 {hour_str}時 路況資料中...")
                traffic_files = self.download_traffic_data(date, hour_str, is_temp=is_temp)
                total_files += len(traffic_files)
                logger.info(f"[{date}] {hour_str}時 路況資料下載完成: {len(traffic_files)} 個檔案")
        
        # 下載站點資料
        for data_type in ["M03A", "M04A", "M05A"]:
            if data_type in data_types:
                logger.info(f"[{date}] 下載 {data_type} 資料中...")
                station_files = self.download_station_based_data(data_type, date, is_temp=is_temp)
                total_files += len(station_files)
                logger.info(f"[{date}] {data_type} 資料下載完成: {len(station_files)} 個檔案")
        
        # 下載旅次資料
        for data_type in ["M06A", "M08A"]:
            if data_type in data_types:
                logger.info(f"[{date}] 下載 {data_type} 資料中...")
                route_files = self.download_route_data(data_type, date, is_temp=is_temp)
                total_files += len(route_files)
                logger.info(f"[{date}] {data_type} 資料下載完成: {len(route_files)} 個檔案")
        
        # 下載宜蘭地區eTag資料
        if "Jilian_ETag" in data_types:
            logger.info(f"[{date}] 下載宜蘭地區eTag資料中...")
            jilian_files = self.download_jilian_etag_data(date, is_temp=is_temp)
            total_files += len(jilian_files)
            logger.info(f"[{date}] 宜蘭地區eTag資料下載完成: {len(jilian_files)} 個檔案")
        
        logger.info(f"===== {date} 的資料下載完成，共 {total_files} 個檔案 =====")
        
        # 如果使用臨時目錄，則清理
        if is_temp and self.use_db:
            self.cleanup_temp_files(date)
        
        return total_files
    
    def cleanup_temp_files(self, date):
        """清理指定日期的臨時文件
        
        Args:
            date: 日期格式YYYYMMDD
        """
        temp_date_dir = os.path.join(self.temp_dir, date)
        if os.path.exists(temp_date_dir):
            try:
                for root, dirs, files in os.walk(temp_date_dir, topdown=False):
                    for file in files:
                        try:
                            os.remove(os.path.join(root, file))
                        except Exception as e:
                            logger.warning(f"無法刪除臨時文件 {os.path.join(root, file)}: {e}")
                    
                    for dir in dirs:
                        try:
                            os.rmdir(os.path.join(root, dir))
                        except Exception as e:
                            logger.warning(f"無法刪除臨時目錄 {os.path.join(root, dir)}: {e}")
                
                try:
                    os.rmdir(temp_date_dir)
                except Exception as e:
                    logger.warning(f"無法刪除臨時日期目錄 {temp_date_dir}: {e}")
                
                logger.info(f"已清理 {date} 的臨時文件")
            except Exception as e:
                logger.error(f"清理臨時文件時出錯: {e}")
    
    def download_multiple_dates(self, dates, data_types=None, max_workers=4, is_temp=False):
        """下載多個日期的資料，帶更完善的進度顯示"""
        total_files = 0
        
        logger.info(f"開始下載 {len(dates)} 天的資料，使用 {max_workers} 個並行處理線程")
        logger.info("-" * 60)
        
        # 先顯示將要下載的日期
        logger.info("計劃下載以下日期的資料:")
        for i, date in enumerate(dates):
            logger.info(f"{i+1}. {date[:4]}/{date[4:6]}/{date[6:]}")
        logger.info("-" * 60)
        
        # 使用並行處理加速下載
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(self.download_all_data_for_date, date, data_types, is_temp): date
                for date in dates
            }
            
            # 使用tqdm顯示總體進度
            for future in tqdm(concurrent.futures.as_completed(future_to_date), 
                            total=len(dates), desc="總體下載進度", ncols=100):
                date = future_to_date[future]
                try:
                    files = future.result()
                    total_files += files
                    logger.info(f"完成日期 {date} 的資料下載，共 {files} 個檔案")
                except Exception as e:
                    logger.error(f"下載 {date} 資料時出錯: {e}")
        
        logger.info("=" * 60)
        logger.info(f"所有資料下載完成，共 {total_files} 個檔案")
        logger.info("=" * 60)
        
        return total_files
    
    def process_vd_xml_to_df(self, xml_file):
        """解析VD XML檔案轉為DataFrame"""
        try:
            # 以下部分沿用您原本的XML解析邏輯，但針對效能進行了優化
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 提取命名空間
            namespace = None
            if '}' in root.tag:
                namespace = root.tag.split('}')[0].strip('{')
            
            # 調整XPath以處理命名空間
            namespaces = {'ns': namespace} if namespace else {}
            
            # 準備資料列表
            lane_data_list = []
            
            # 尋找更新時間
            update_time_elem = root.find(".//UpdateTime") or root.find(".//ns:UpdateTime", namespaces)
            update_time = update_time_elem.text if update_time_elem is not None else datetime.now().isoformat()
            
            # 尋找VDLive元素
            vd_lives = root.findall(".//VDLive", namespaces) or root.findall(".//ns:VDLive", namespaces)
            
            # 遍歷所有VD設備
            for vd_live in vd_lives:
                # 尋找VDID
                vd_id_elem = vd_live.find("./VDID", namespaces) or vd_live.find("./ns:VDID", namespaces)
                vd_id = vd_id_elem.text if vd_id_elem is not None else "unknown"
                
                # 尋找並遍歷所有車道
                lanes = vd_live.findall(".//Lane", namespaces) or vd_live.findall(".//ns:Lane", namespaces)
                
                for lane in lanes:
                    # 提取車道資料
                    get_text = lambda path: (lane.find(path, namespaces) or lane.find(f"./ns:{path.split('/')[-1]}", namespaces)).text if (lane.find(path, namespaces) or lane.find(f"./ns:{path.split('/')[-1]}", namespaces)) is not None else None
                    
                    lane_id = get_text("./LaneID") or "unknown"
                    lane_type = get_text("./LaneType") or "unknown"
                    speed = float(get_text("./Speed") or 0)
                    occupancy = float(get_text("./Occupancy") or 0)
                    volume = float(get_text("./Volume") or 0)
                    
                    # 將車道資料加入列表
                    lane_data = {
                        'update_time': update_time,
                        'vd_id': vd_id,
                        'lane_id': lane_id,
                        'lane_type': lane_type,
                        'speed': speed,
                        'occupancy': occupancy,
                        'volume': volume
                    }
                    lane_data_list.append(lane_data)
            
            # 創建DataFrame
            lane_data_df = pd.DataFrame(lane_data_list)
            return lane_data_df
            
        except Exception as e:
            logger.error(f"解析XML時出錯: {e}")
            return pd.DataFrame()
    
    def extract_weather_data(self, date):
        """從氣象資料中提取有用的資訊
        
        Args:
            date: 日期格式YYYYMMDD
            
        Returns:
            天氣資料DataFrame
        """
        weather_dir = os.path.join(self.base_dir, "Weather", date)
        
        # 檢查資料夾是否存在
        if not os.path.exists(weather_dir):
            logger.warning(f"氣象資料資料夾不存在: {date}")
            return pd.DataFrame()
        
        # 如果使用資料庫，則直接從資料庫中讀取
        if self.use_db:
            try:
                query = f"""
                SELECT * FROM weather_data 
                WHERE date = '{date}'
                ORDER BY timestamp
                """
                conn = self.get_db_connection()
                df = pd.read_sql(query, conn)
                self.close_db_connection(conn)
                logger.info(f"從資料庫讀取 {len(df)} 筆氣象資料")
                return df
            except Exception as e:
                logger.error(f"從資料庫讀取氣象資料時出錯: {e}")
                # 如果從資料庫讀取失敗，則嘗試從文件中讀取
        
        # 讀取所有小時氣象資料
        weather_data = []
        for hour in range(24):
            json_file = os.path.join(weather_dir, f"Weather_{date}_{hour:02d}00.json")
            
            if os.path.exists(json_file):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 提取氣象站資料
                    if 'records' in data and 'location' in data['records']:
                        for location in data['records']['location']:
                            lat = float(location['lat'])
                            lon = float(location['lon'])
                            locationName = location['locationName']
                            
                            # 提取各項觀測資料
                            weather_elements = {}
                            for elem in location['weatherElement']:
                                elem_name = elem['elementName']
                                elem_value = elem['elementValue']
                                weather_elements[elem_name] = elem_value
                            
                            # 僅保留關鍵資料
                            weather_record = {
                                'timestamp': f"{date[:4]}-{date[4:6]}-{date[6:]}T{hour:02d}:00:00",
                                'locationName': locationName,
                                'lat': lat,
                                'lon': lon,
                                'TEMP': float(weather_elements.get('TEMP', 'nan')),  # 溫度
                                'HUMD': float(weather_elements.get('HUMD', 'nan')),  # 濕度
                                'PRES': float(weather_elements.get('PRES', 'nan')),  # 氣壓
                                'WDSD': float(weather_elements.get('WDSD', 'nan')),  # 風速
                                'RAIN': float(weather_elements.get('RAIN', 'nan')),  # 降雨量
                                'H_24R': float(weather_elements.get('H_24R', 'nan')),  # 24小時累積雨量
                                'VISB': float(weather_elements.get('VISB', 'nan'))   # 能見度
                            }
                            
                            weather_data.append(weather_record)
                            
                except Exception as e:
                    logger.error(f"處理氣象資料時出錯: {json_file} - {e}")
        
        # 創建DataFrame
        if weather_data:
            return pd.DataFrame(weather_data)
        else:
            logger.warning(f"沒有找到可用的氣象資料: {date}")
            return pd.DataFrame()
    
    def filter_target_sections_vd(self, df, static_xml_file):
        """根據目標路段過濾VD資料
        
        Args:
            df: VD資料DataFrame
            static_xml_file: VD靜態資訊XML檔案
            
        Returns:
            過濾後的DataFrame
        """
        try:
            # 解析靜態XML獲取VD位置資訊
            tree = ET.parse(static_xml_file)
            root = tree.getroot()
            
            # 提取命名空間
            namespace = None
            if '}' in root.tag:
                namespace = root.tag.split('}')[0].strip('{')
            
            # 調整XPath以處理命名空間
            namespaces = {'ns': namespace} if namespace else {}
            
            # 尋找VD元素
            vd_elements = root.findall(".//VD", namespaces) or root.findall(".//ns:VD", namespaces)
            
            # 創建VD ID到路段的映射
            vd_to_section = {}
            for vd in vd_elements:
                vd_id_elem = vd.find("./VDID", namespaces) or vd.find("./ns:VDID", namespaces)
                road_id_elem = vd.find("./RoadID", namespaces) or vd.find("./ns:RoadID", namespaces)
                road_name_elem = vd.find("./RoadName", namespaces) or vd.find("./ns:RoadName", namespaces)
                road_direction_elem = vd.find("./RoadDirection", namespaces) or vd.find("./ns:RoadDirection", namespaces)
                location_mile_elem = vd.find("./LocationMile", namespaces) or vd.find("./ns:LocationMile", namespaces)
                
                if vd_id_elem is not None and road_id_elem is not None:
                    vd_id = vd_id_elem.text
                    road_id = road_id_elem.text
                    road_name = road_name_elem.text if road_name_elem is not None else ""
                    road_direction = road_direction_elem.text if road_direction_elem is not None else ""
                    location_mile = float(location_mile_elem.text) if location_mile_elem is not None else 0.0
                    
                    vd_to_section[vd_id] = {
                        'road_id': road_id,
                        'road_name': road_name,
                        'road_direction': road_direction,
                        'location_mile': location_mile
                    }
            
            # 根據目標路段過濾數據
            target_vds = []
            
            # 合併主要研究路段和其他關聯路段
            all_sections = {**self.target_sections, **self.related_sections}
            
            for vd_id, vd_info in vd_to_section.items():
                for section_name, section_info in all_sections.items():
                    # 檢查是否在目標路段上
                    if (section_info['route_id'] in vd_info['road_name'] and 
                        section_info['direction'] == vd_info['road_direction'] and
                        section_info['start_mile'] <= vd_info['location_mile'] <= section_info['end_mile']):
                        target_vds.append(vd_id)
                        break
            
            # 只保留目標路段的VD數據
            filtered_df = df[df['vd_id'].isin(target_vds)]
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"過濾目標路段時出錯: {e}")
            return df
    
    def analyze_vd_data(self, date):
        """分析指定日期的VD資料
        
        Args:
            date: 日期格式YYYYMMDD
            
        Returns:
            分析結果DataFrame
        """
        vd_dir = os.path.join(self.base_dir, "VD", date)
        static_dir = os.path.join(self.base_dir, "Static", date)
        
        # 檢查資料夾是否存在
        if not os.path.exists(vd_dir) or not os.path.exists(static_dir):
            logger.warning(f"VD資料或靜態資料資料夾不存在: {date}")
            return pd.DataFrame()
        
        # 檢查靜態資料是否存在
        static_xml_file = os.path.join(static_dir, "VD_0000.xml")
        if not os.path.exists(static_xml_file):
            logger.warning(f"VD靜態資料不存在: {static_xml_file}")
            return pd.DataFrame()
        
        # 如果使用資料庫，則直接從資料庫中讀取
        if self.use_db:
            try:
                query = f"""
                SELECT * FROM vd_data 
                WHERE date = '{date}'
                ORDER BY update_time
                """
                conn = self.get_db_connection()
                df = pd.read_sql(query, conn)
                self.close_db_connection(conn)
                logger.info(f"從資料庫讀取 {len(df)} 筆VD資料")
                return df
            except Exception as e:
                logger.error(f"從資料庫讀取VD資料時出錯: {e}")
                # 如果從資料庫讀取失敗，則嘗試從文件中讀取
                
        # 從檔案讀取所有VD資料
        all_dfs = []
        for hour in range(24):
            hour_str = f"{hour:02d}"
            for minute in range(0, 60):
                minute_str = f"{minute:02d}"
                xml_file = os.path.join(vd_dir, f"VDLive_{hour_str}{minute_str}.xml")
                
                if os.path.exists(xml_file):
                    df = self.process_vd_xml_to_df(xml_file)
                    if not df.empty:
                        # 過濾目標路段
                        filtered_df = self.filter_target_sections_vd(df, static_xml_file)
                        if not filtered_df.empty:
                            all_dfs.append(filtered_df)
        
        # 合併所有DataFrame
        if all_dfs:
            result_df = pd.concat(all_dfs, ignore_index=True)
            return result_df
        else:
            logger.warning(f"沒有找到可用的VD資料: {date}")
            return pd.DataFrame()
    
    def get_data_for_analysis(self, start_date, end_date, data_types=None, weekend_only=True, is_temp=False):
        """取得並分析指定日期範圍內的資料
        
        Args:
            start_date: 開始日期格式YYYYMMDD
            end_date: 結束日期格式YYYYMMDD
            data_types: 要下載的資料類型列表
            weekend_only: 是否只下載週末資料
            is_temp: 是否使用臨時目錄
            
        Returns:
            分析結果
        """
        # 獲取日期列表
        if weekend_only:
            dates = self.get_weekend_dates(start_date, end_date)
        else:
            dates = self.get_non_holiday_weekdays(start_date, end_date)
        
        # 下載所有資料
        self.download_multiple_dates(dates, data_types, is_temp=is_temp)
        
        # 分析資料
        results = []
        for date in dates:
            vd_df = self.analyze_vd_data(date)
            weather_df = self.extract_weather_data(date)
            
            results.append({
                'date': date,
                'vd_data': vd_df,
                'weather_data': weather_df
            })
        
        return results

    def process_etag_file_to_db(self, xml_file, date, hour, minute):
        """解析eTag XML檔案並存入資料庫
        
        Args:
            xml_file: XML檔案路徑
            date: 日期格式YYYYMMDD
            hour: 小時
            minute: 分鐘
            
        Returns:
            插入的記錄數
        """
        if not self.use_db:
            return 0
            
        try:
            # 解析XML
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 提取命名空間
            namespace = None
            if '}' in root.tag:
                namespace = root.tag.split('}')[0].strip('{')
            
            # 調整XPath以處理命名空間
            namespaces = {'ns': namespace} if namespace else {}
            
            # 尋找更新時間
            update_time_elem = root.find(".//UpdateTime") or root.find(".//ns:UpdateTime", namespaces)
            update_time_str = update_time_elem.text if update_time_elem is not None else datetime.now().isoformat()
            
            try:
                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
            except ValueError:
                # 嘗試另一種格式
                update_time = datetime.strptime(update_time_str, "%Y-%m-%dT%H:%M:%S%z")
            
            # 尋找ETagPairLive元素
            etag_pairs = root.findall(".//ETagPairLive", namespaces) or root.findall(".//ns:ETagPairLive", namespaces)
            
            # 準備批量插入的數據
            etag_data_list = []
            
            # 遍歷所有eTag配對
            for etag_pair in etag_pairs:
                # 提取配對資料
                get_text = lambda path: (etag_pair.find(path, namespaces) or etag_pair.find(f"./ns:{path.split('/')[-1]}", namespaces)).text if (etag_pair.find(path, namespaces) or etag_pair.find(f"./ns:{path.split('/')[-1]}", namespaces)) is not None else None
                
                pair_id = get_text("./PairID") or "unknown"
                start_point = get_text("./StartETagID") or "unknown"
                end_point = get_text("./EndETagID") or "unknown"
                road_id = get_text("./RouteID") or "unknown"
                direction = get_text("./Direction") or "unknown"
                travel_time = int(get_text("./TravelTime") or 0)
                speed = float(get_text("./Speed") or 0)
                count = int(get_text("./Count") or 0)
                
                # 將配對資料加入列表
                etag_data = {
                    'update_time': update_time,
                    'pair_id': pair_id,
                    'start_point': start_point,
                    'end_point': end_point,
                    'road_id': road_id,
                    'direction': direction,
                    'travel_time': travel_time,
                    'speed': speed,
                    'count': count,
                    'date': date,
                    'hour': hour,
                    'minute': minute
                }
                etag_data_list.append(etag_data)
            
            # 批量插入到資料庫
            if etag_data_list:
                try:
                    # 準備SQL語句
                    insert_query = """
                    INSERT INTO etag_travel_time
                    (update_time, pair_id, start_point, end_point, road_id, direction, travel_time, speed, count, date, hour, minute)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    # 準備參數列表
                    params = [
                        (
                            data['update_time'],
                            data['pair_id'],
                            data['start_point'],
                            data['end_point'],
                            data['road_id'],
                            data['direction'],
                            data['travel_time'],
                            data['speed'],
                            data['count'],
                            data['date'],
                            data['hour'],
                            data['minute']
                        )
                        for data in etag_data_list
                    ]
                    
                    # 執行批量插入
                    conn = self.get_db_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_query, params)
                    conn.commit()
                    
                    logger.info(f"成功將 {len(etag_data_list)} 筆eTag資料存入資料庫")
                    return len(etag_data_list)
                except Exception as e:
                    logger.error(f"存儲eTag資料到資料庫時出錯: {e}")
                    conn.rollback()
                finally:
                    cursor.close()
                    self.close_db_connection(conn)
            
            return 0
            
        except Exception as e:
            logger.error(f"解析eTag XML時出錯: {e}")
            return 0
    
    def process_news_file_to_db(self, xml_file, date):
        """解析事件資料XML檔案並存入資料庫
        
        Args:
            xml_file: XML檔案路徑
            date: 日期格式YYYYMMDD
            
        Returns:
            插入的記錄數
        """
        if not self.use_db:
            return 0
            
        try:
            # 解析XML
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 提取命名空間
            namespace = None
            if '}' in root.tag:
                namespace = root.tag.split('}')[0].strip('{')
            
            # 調整XPath以處理命名空間
            namespaces = {'ns': namespace} if namespace else {}
            
            # 尋找更新時間
            update_time_elem = root.find(".//UpdateTime") or root.find(".//ns:UpdateTime", namespaces)
            update_time_str = update_time_elem.text if update_time_elem is not None else datetime.now().isoformat()
            
            try:
                update_time = datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))
            except ValueError:
                # 嘗試另一種格式
                update_time = datetime.strptime(update_time_str, "%Y-%m-%dT%H:%M:%S%z")
            
            # 尋找事件元素
            events = root.findall(".//Incident", namespaces) or root.findall(".//ns:Incident", namespaces)
            
            # 準備批量插入的數據
            event_data_list = []
            
            # 遍歷所有事件
            for event in events:
                # 提取事件資料
                get_text = lambda path: (event.find(path, namespaces) or event.find(f"./ns:{path.split('/')[-1]}", namespaces)).text if (event.find(path, namespaces) or event.find(f"./ns:{path.split('/')[-1]}", namespaces)) is not None else None
                
                event_id = get_text("./IncidentID") or "unknown"
                event_type = get_text("./Type") or "unknown"
                event_detail = get_text("./Comment") or ""
                road_id = get_text("./RoadID") or "unknown"
                direction = get_text("./Direction") or "unknown"
                location_mile = float(get_text("./LocationMile") or 0)
                
                # 處理時間
                start_time_str = get_text("./StartTime")
                end_time_str = get_text("./EndTime")
                
                start_time = None
                end_time = None
                
                if start_time_str:
                    try:
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S%z")
                        except ValueError:
                            pass
                
                if end_time_str:
                    try:
                        end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M:%S%z")
                        except ValueError:
                            pass
                
                # 將事件資料加入列表
                event_data = {
                    'update_time': update_time,
                    'event_id': event_id,
                    'event_type': event_type,
                    'event_detail': event_detail,
                    'road_id': road_id,
                    'direction': direction,
                    'location_mile': location_mile,
                    'start_time': start_time,
                    'end_time': end_time,
                    'date': date
                }
                event_data_list.append(event_data)
            
            # 批量插入到資料庫
            if event_data_list:
                try:
                    # 準備SQL語句
                    insert_query = """
                    INSERT INTO traffic_events
                    (update_time, event_id, event_type, event_detail, road_id, direction, location_mile, start_time, end_time, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    update_time = VALUES(update_time),
                    event_type = VALUES(event_type),
                    event_detail = VALUES(event_detail),
                    end_time = VALUES(end_time)
                    """
                    
                    # 準備參數列表
                    params = [
                        (
                            data['update_time'],
                            data['event_id'],
                            data['event_type'],
                            data['event_detail'],
                            data['road_id'],
                            data['direction'],
                            data['location_mile'],
                            data['start_time'],
                            data['end_time'],
                            data['date']
                        )
                        for data in event_data_list
                    ]
                    
                    # 執行批量插入
                    conn = self.get_db_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_query, params)
                    conn.commit()
                    
                    logger.info(f"成功將 {len(event_data_list)} 筆事件資料存入資料庫")
                    return len(event_data_list)
                except Exception as e:
                    logger.error(f"存儲事件資料到資料庫時出錯: {e}")
                    conn.rollback()
                finally:
                    cursor.close()
                    self.close_db_connection(conn)
            
            return 0
            
        except Exception as e:
            logger.error(f"解析事件XML時出錯: {e}")
            return 0
    
    def process_weather_file_to_db(self, json_file, date):
        """解析氣象資料JSON檔案並存入資料庫
        
        Args:
            json_file: JSON檔案路徑
            date: 日期格式YYYYMMDD
            
        Returns:
            插入的記錄數
        """
        if not self.use_db:
            return 0
            
        try:
            # 讀取JSON
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 提取氣象站資料
            weather_data_list = []
            
            if 'records' in data and 'location' in data['records']:
                for location in data['records']['location']:
                    lat = float(location['lat'])
                    lon = float(location['lon'])
                    location_name = location['locationName']
                    
                    # 提取各項觀測資料
                    weather_elements = {}
                    for elem in location['weatherElement']:
                        elem_name = elem['elementName']
                        elem_value = elem['elementValue']
                        weather_elements[elem_name] = elem_value
                    
                    # 提取時間戳
                    timestamp_str = location.get('time', {}).get('obsTime')
                    if not timestamp_str:
                        # 從檔名提取時間
                        filename = os.path.basename(json_file)
                        if filename.startswith("Weather_") and "_" in filename:
                            time_part = filename.split("_")[1] + "_" + filename.split("_")[2].split(".")[0]
                            timestamp_str = f"{time_part[:4]}-{time_part[4:6]}-{time_part[6:8]}T{time_part[9:11]}:00:00"
                        else:
                            # 使用當前時間作為後備
                            timestamp_str = datetime.now().isoformat()
                    
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except ValueError:
                        # 嘗試另一種格式
                        try:
                            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
                        except ValueError:
                            # 使用當前時間作為後備
                            timestamp = datetime.now()
                    
                    # 僅保留關鍵資料
                    def safe_float(value, default='nan'):
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return float(default)
                    
                    weather_record = {
                        'timestamp': timestamp,
                        'location_name': location_name,
                        'lat': lat,
                        'lon': lon,
                        'temp': safe_float(weather_elements.get('TEMP', 'nan')),  # 溫度
                        'humd': safe_float(weather_elements.get('HUMD', 'nan')),  # 濕度
                        'pres': safe_float(weather_elements.get('PRES', 'nan')),  # 氣壓
                        'wdsd': safe_float(weather_elements.get('WDSD', 'nan')),  # 風速
                        'rain': safe_float(weather_elements.get('RAIN', 'nan')),  # 降雨量
                        'h_24r': safe_float(weather_elements.get('H_24R', 'nan')),  # 24小時累積雨量
                        'visb': safe_float(weather_elements.get('VISB', 'nan')),   # 能見度
                        'date': date
                    }
                    
                    weather_data_list.append(weather_record)
            
            # 批量插入到資料庫
            if weather_data_list:
                try:
                    # 準備SQL語句
                    insert_query = """
                    INSERT INTO weather_data
                    (timestamp, location_name, lat, lon, temp, humd, pres, wdsd, rain, h_24r, visb, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    # 準備參數列表
                    params = [
                        (
                            data['timestamp'],
                            data['location_name'],
                            data['lat'],
                            data['lon'],
                            data['temp'],
                            data['humd'],
                            data['pres'],
                            data['wdsd'],
                            data['rain'],
                            data['h_24r'],
                            data['visb'],
                            data['date']
                        )
                        for data in weather_data_list
                    ]
                    
                    # 執行批量插入
                    conn = self.get_db_connection()
                    cursor = conn.cursor()
                    cursor.executemany(insert_query, params)
                    conn.commit()
                    
                    logger.info(f"成功將 {len(weather_data_list)} 筆氣象資料存入資料庫")
                    return len(weather_data_list)
                except Exception as e:
                    logger.error(f"存儲氣象資料到資料庫時出錯: {e}")
                    conn.rollback()
                finally:
                    cursor.close()
                    self.close_db_connection(conn)
            
            return 0
            
        except Exception as e:
            logger.error(f"解析氣象JSON時出錯: {e}")
            return 0
    
    def download_etag_data(self, date, hour, min_interval=5, save_dir=None, is_temp=False):
        """下載特定日期和小時的eTag配對資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為5)
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "ETag", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每5分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/ETag/{date}/ETagPairLive_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"ETagPairLive_{time_str}.xml")
            
            result = self.download_file(url, save_path, is_temp)
            if result:
                files.append(result)
                # 如果使用資料庫，則解析並存儲到資料庫
                if self.use_db and result:
                    try:
                        self.process_etag_file_to_db(result, date, int(hour), minute)
                    except Exception as e:
                        logger.error(f"解析eTag文件到資料庫時出錯: {e}")
                    
        return files
    
    def download_news_data(self, date, hour, min_interval=1, save_dir=None, is_temp=False):
        """下載特定日期和小時的事件資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "News", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/News/{date}/News_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"News_{time_str}.xml")
            
            result = self.download_file(url, save_path, is_temp)
            if result:
                files.append(result)
                # 如果使用資料庫，則解析並存儲到資料庫
                if self.use_db and result:
                    try:
                        self.process_news_file_to_db(result, date)
                    except Exception as e:
                        logger.error(f"解析News文件到資料庫時出錯: {e}")
                
        return files

    def download_traffic_data(self, date, hour, min_interval=1, save_dir=None, is_temp=False):
        """下載特定日期和小時的路況資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "Traffic", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/Section/{date}/LiveTraffic_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"LiveTraffic_{time_str}.xml")
            
            result = self.download_file(url, save_path, is_temp)
            if result:
                files.append(result)
                    
        return files
    
    def download_station_based_data(self, data_type, date, save_dir=None, is_temp=False):
        """下載基於測站的統計資料(M03A, M04A, M05A等)
        
        Args:
            data_type: 資料類型 (M03A, M04A, M05A...)
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, data_type, date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對每個小時
        for hour in range(24):
            # 每個5分鐘資料
            for minute in range(0, 60, 5):
                time_str = f"{hour:02d}{minute:02d}00"
                url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                
                result = self.download_file(url, save_path, is_temp)
                if result:
                    files.append(result)
                    
        return files
    
    def download_route_data(self, data_type, date, save_dir=None, is_temp=False):
        """下載旅次資料(M06A, M07A, M08A)
        
        Args:
            data_type: 資料類型 (M06A, M07A, M08A)
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, data_type, date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對每個小時下載一次數據
        for hour in range(24):
            # M06A和M07A是每小時更新，M08A是每5分鐘
            if data_type in ['M06A', 'M07A']:
                time_str = f"{hour:02d}0000"
                url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                
                result = self.download_file(url, save_path, is_temp)
                if result:
                    files.append(result)
            else:  # M08A
                for minute in range(0, 60, 5):
                    time_str = f"{hour:02d}{minute:02d}00"
                    url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                    save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                    
                    result = self.download_file(url, save_path, is_temp)
                    if result:
                        files.append(result)
                    
        return files
    
    def download_static_data(self, date, save_dir=None, is_temp=False):
        """下載靜態資料(路段資訊、VD位置、eTag位置等)
        
        Args:
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑字典
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "Static", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        static_files = {}
        
        # VD靜態資訊
        vd_url = f"{self.base_url}/history/motc20/VD/{date}/VD_0000.xml.gz"
        vd_path = os.path.join(save_dir, "VD_0000.xml")
        result = self.download_file(vd_url, vd_path, is_temp)
        if result:
            static_files['vd'] = result
        
        # eTag靜態資訊
        etag_url = f"{self.base_url}/history/motc20/ETag/{date}/ETag_0000.xml.gz"
        etag_path = os.path.join(save_dir, "ETag_0000.xml")
        result = self.download_file(etag_url, etag_path, is_temp)
        if result:
            static_files['etag'] = result
            
        # eTag配對路徑靜態資訊
        etag_pair_url = f"{self.base_url}/history/motc20/ETag/{date}/ETagPair_0000.xml.gz"
        etag_pair_path = os.path.join(save_dir, "ETagPair_0000.xml")
        result = self.download_file(etag_pair_url, etag_pair_path, is_temp)
        if result:
            static_files['etag_pair'] = result
        
        # 路段基本資訊
        section_url = f"{self.base_url}/history/motc20/Section/{date}/Section_0000.xml.gz"
        section_path = os.path.join(save_dir, "Section_0000.xml")
        result = self.download_file(section_url, section_path, is_temp)
        if result:
            static_files['section'] = result
        
        # 路段線型圖資資訊
        section_shape_url = f"{self.base_url}/history/motc20/Section/{date}/SectionShape_0000.xml.gz"
        section_shape_path = os.path.join(save_dir, "SectionShape_0000.xml")
        result = self.download_file(section_url, section_path, is_temp)
        if result:
            static_files['section_shape'] = result
        
        # 壅塞水準資訊
        congestion_url = f"{self.base_url}/history/motc20/Section/{date}/CongestionLevel_0000.xml.gz"
        congestion_path = os.path.join(save_dir, "CongestionLevel_0000.xml")
        result = self.download_file(congestion_url, congestion_path, is_temp)
        if result:
            static_files['congestion'] = result
        
        # CCTV靜態資訊
        cctv_url = f"{self.base_url}/history/motc20/CCTV/{date}/CCTV_0000.xml.gz"
        cctv_path = os.path.join(save_dir, "CCTV_0000.xml")
        result = self.download_file(cctv_url, cctv_path, is_temp)
        if result:
            static_files['cctv'] = result
            
        return static_files
    
    def download_jilian_etag_data(self, date, save_dir=None, is_temp=False):
        """下載宜蘭地區(坪林)eTag配對旅行時間資料
        
        Args:
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "Jilian_ETag", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 每5分鐘一筆資料
        for hour in range(24):
            for minute in range(0, 60, 5):
                time_str = f"{hour:02d}{minute:02d}"
                url = f"{self.base_url}/history/TDCS_PINGLIN/M04/{date}/R14C_M04{date}{time_str}.csv"
                save_path = os.path.join(save_dir, f"R14C_M04{date}{time_str}.csv")
                
                result = self.download_file(url, save_path, is_temp)
                if result:
                    files.append(result)
                    
        return files
    
    def download_weather_data(self, date, save_dir=None, is_temp=False):
        """下載氣象資料
        
        Args:
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            is_temp: 是否存儲在臨時目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if not self.cwa_api_key:
            logger.warning("未設置氣象署API授權碼，無法下載氣象資料")
            return []
            
        if save_dir is None:
            save_dir = os.path.join(self.base_dir if not is_temp else self.temp_dir, "Weather", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 轉換日期格式
        date_obj = datetime.strptime(date, "%Y%m%d")
        date_str = date_obj.strftime("%Y-%m-%d")
        
        # 下載自動氣象站資料 (每小時)
        for hour in range(24):
            time_str = f"{hour:02d}:00:00"
            data_id = "O-A0001-001"  # 自動氣象站資料-每小時
            
            # 構建API URL
            url = f"{self.cwa_base_url}/{data_id}?Authorization={self.cwa_api_key}&format=JSON&timeFrom={date_str}T{time_str}&timeTo={date_str}T{time_str}"
            save_path = os.path.join(save_dir, f"Weather_{date}_{hour:02d}00.json")
            
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    with open(save_path, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    files.append(save_path)
                    logger.info(f"成功下載氣象資料: {save_path}")
                    
                    # 如果使用資料庫，則解析並存儲到資料庫
                    if self.use_db:
                        try:
                            self.process_weather_file_to_db(save_path, date)
                        except Exception as e:
                            logger.error(f"解析氣象資料到資料庫時出錯: {e}")
                else:
                    logger.error(f"下載氣象資料失敗: {response.status_code} {response.text}")
            except Exception as e:
                logger.error(f"下載氣象資料出錯: {e}")
        
        # 下載天氣預報資料 (每天)
        data_id = "F-D0047-001"  # 一般天氣預報-臺灣全島
        
        # 構建API URL
        url = f"{self.cwa_base_url}/{data_id}?Authorization={self.cwa_api_key}&format=JSON&timeFrom={date_str}T00:00:00&timeTo={date_str}T23:59:59"
        save_path = os.path.join(save_dir, f"Weather_Forecast_{date}.json")
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                files.append(save_path)
                logger.info(f"成功下載天氣預報資料: {save_path}")
            else:
                logger.error(f"下載天氣預報資料失敗: {response.status_code} {response.text}")
        except Exception as e:
            logger.error(f"下載天氣預報資料出錯: {e}")
            
        # 下載雨量資料 (每小時)
        data_id = "O-A0002-001"  # 自動雨量站資料-每小時
        
        # 構建API URL
        url = f"{self.cwa_base_url}/{data_id}?Authorization={self.cwa_api_key}&format=JSON&timeFrom={date_str}T00:00:00&timeTo={date_str}T23:59:59"
        save_path = os.path.join(save_dir, f"Rainfall_{date}.json")
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                files.append(save_path)
                logger.info(f"成功下載雨量資料: {save_path}")
            else:
                logger.error(f"下載雨量資料失敗: {response.status_code} {response.text}")
        except Exception as e:
            logger.error(f"下載雨量資料出錯: {e}")
            
        return files

# 主程式
if __name__ == "__main__":
    # 資料庫配置
    db_config = {
        'host': 'localhost',
        'database': 'highway_traffic',
        'user': 'root',
        'password': 'Tim0986985588='
    }
    
    # 設定API授權碼 - 請替換為您的授權碼
    cwa_api_key = "CWA-8D2C708D-67EF-401C-8701-4F54C2AB0C04"  # 您的中央氣象署API授權碼
    
    collector = FreewayDataCollector(
        base_dir="highway_data",
        cwa_api_key=cwa_api_key,
        db_config=db_config,
        use_db=True,
        temp_dir="temp_data"
    )
    
    # 設定下載參數
    start_date = "20240101"  # 113年1月1日
    end_date = "20240331"    # 113年3月31日
    
    # 設定要下載的資料類型
    data_types = [
        "VD", "ETag", "News", "Traffic", "Static", 
        "M03A", "M05A", "M06A", "M08A", "Jilian_ETag", "Weather"
    ]
    
    # 1. 下載假日資料（主要資料集）
    weekend_dates = collector.get_weekend_dates(start_date, end_date)
    logger.info(f"將下載 {len(weekend_dates)} 天的假日資料")
    weekend_results = collector.download_multiple_dates(weekend_dates, data_types, is_temp=True)
    
    # 2. 選擇性下載部分平日資料（對照資料集）
    # 每週選擇星期三作為代表性工作日
    weekday_dates = []
    current = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    while current <= end:
        # 星期三(2)
        if current.weekday() == 2:
            weekday_dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    logger.info(f"將下載 {len(weekday_dates)} 天的平日資料")
    weekday_results = collector.download_multiple_dates(weekday_dates, data_types, is_temp=True)
    
    logger.info(f"資料下載完成，共 {len(weekend_dates) + len(weekday_dates)} 天")