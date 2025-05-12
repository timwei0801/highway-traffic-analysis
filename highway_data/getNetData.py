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

class FreewayDataCollector:
    """國道交通資料收集器"""
    
    def __init__(self, base_dir="highway_data"):
        """初始化收集器
        
        Args:
            base_dir: 資料儲存的根目錄
        """
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
        
        # 主要研究路段設定
        self.target_sections = {
            "國5南港系統-頭城": {
                "route_id": "國5",
                "start_name": "南港系統",
                "end_name": "頭城"
            },
            "國1楊梅-新竹": {
                "route_id": "國1",
                "start_name": "楊梅",
                "end_name": "新竹"
            }
        }

        # 資料來源基礎URL - 修正為正確的URL
        self.base_url = "https://tisvcloud.freeway.gov.tw"
        
        # 設置請求標頭
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
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
    
    def download_file(self, url, save_path=None):
        """下載資料檔案，帶進度顯示和錯誤處理"""
        try:
            # 檢查並修正URL中的錯誤部分
            if "history-list.php/history" in url:
                url = url.replace("history-list.php/history", "history")
            
            # 顯示當前下載的文件
            print(f"正在下載: {url}")
            
            # 嘗試下載
            response = requests.get(url, headers=self.headers, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            # 確保文件夾存在
            if save_path:
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
                        print(f"解壓縮失敗: {gz_error}，將直接使用原始檔案")
                        # 直接使用原始回應內容
                        with open(save_path, 'wb') as f_out:
                            f_out.write(response.content)
                        
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    
                    return save_path
                except Exception as e:
                    print(f"處理檔案時出錯: {e}")
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
            print(f"下載請求失敗 {url}: {e}")
            return None
        except Exception as e:
            print(f"下載處理失敗 {url}: {e}")
            return None
    
    def download_vd_data(self, date, hour, min_interval=1, save_dir=None):
        """下載特定日期和小時的VD資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "VD", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每個分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/VD/{date}/VDLive_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"VDLive_{time_str}.xml")
            
            result = self.download_file(url, save_path)
            if result:
                files.append(result)
                
        return files
    
    def download_etag_data(self, date, hour, min_interval=5, save_dir=None):
        """下載特定日期和小時的eTag配對資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為5)
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "ETag", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每5分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/ETag/{date}/ETagPairLive_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"ETagPairLive_{time_str}.xml")
            
            result = self.download_file(url, save_path)
            if result:
                files.append(result)
                
        return files
    
    def download_news_data(self, date, hour, min_interval=1, save_dir=None):
        """下載特定日期和小時的事件資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "News", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/News/{date}/News_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"News_{time_str}.xml")
            
            result = self.download_file(url, save_path)
            if result:
                files.append(result)
                
        return files

    def download_traffic_data(self, date, hour, min_interval=1, save_dir=None):
        """下載特定日期和小時的路況資料
        
        Args:
            date: 日期格式YYYYMMDD
            hour: 小時格式HH (00-23)
            min_interval: 分鐘間隔 (通常為1)
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "Traffic", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對於指定小時的每分鐘
        for minute in range(0, 60, min_interval):
            time_str = f"{hour}{minute:02d}"
            url = f"{self.base_url}/history/motc20/Section/{date}/LiveTraffic_{time_str}.xml.gz"
            save_path = os.path.join(save_dir, f"LiveTraffic_{time_str}.xml")
            
            result = self.download_file(url, save_path)
            if result:
                files.append(result)
                
        return files
    
    def download_station_based_data(self, data_type, date, save_dir=None):
        """下載基於測站的統計資料(M03A, M04A, M05A等)
        
        Args:
            data_type: 資料類型 (M03A, M04A, M05A...)
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, data_type, date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對每個小時
        for hour in range(24):
            # 每個5分鐘資料
            for minute in range(0, 60, 5):
                time_str = f"{hour:02d}{minute:02d}00"
                url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                
                result = self.download_file(url, save_path)
                if result:
                    files.append(result)
                    
        return files
    
    def download_route_data(self, data_type, date, save_dir=None):
        """下載旅次資料(M06A, M07A, M08A)
        
        Args:
            data_type: 資料類型 (M06A, M07A, M08A)
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, data_type, date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 對每個小時下載一次數據
        for hour in range(24):
            # M06A和M07A是每小時更新，M08A是每5分鐘
            if data_type in ['M06A', 'M07A']:
                time_str = f"{hour:02d}0000"
                url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                
                result = self.download_file(url, save_path)
                if result:
                    files.append(result)
            else:  # M08A
                for minute in range(0, 60, 5):
                    time_str = f"{hour:02d}{minute:02d}00"
                    url = f"{self.base_url}/history/TDCS/{data_type}/{date}/{hour:02d}/TDCS_{data_type}_{date}_{time_str}.csv"
                    save_path = os.path.join(save_dir, f"TDCS_{data_type}_{date}_{time_str}.csv")
                    
                    result = self.download_file(url, save_path)
                    if result:
                        files.append(result)
                    
        return files
    
    def download_static_data(self, date, save_dir=None):
        """下載靜態資料(路段資訊、VD位置、eTag位置等)
        
        Args:
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑字典
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "Static", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        static_files = {}
        
        # VD靜態資訊
        vd_url = f"{self.base_url}/history/motc20/VD/{date}/VD_0000.xml.gz"
        vd_path = os.path.join(save_dir, "VD_0000.xml")
        result = self.download_file(vd_url, vd_path)
        if result:
            static_files['vd'] = result
        
        # eTag靜態資訊
        etag_url = f"{self.base_url}/history/motc20/ETag/{date}/ETag_0000.xml.gz"
        etag_path = os.path.join(save_dir, "ETag_0000.xml")
        result = self.download_file(etag_url, etag_path)
        if result:
            static_files['etag'] = result
            
        # eTag配對路徑靜態資訊
        etag_pair_url = f"{self.base_url}/history/motc20/ETag/{date}/ETagPair_0000.xml.gz"
        etag_pair_path = os.path.join(save_dir, "ETagPair_0000.xml")
        result = self.download_file(etag_pair_url, etag_pair_path)
        if result:
            static_files['etag_pair'] = result
        
        # 路段基本資訊
        section_url = f"{self.base_url}/history/motc20/Section/{date}/Section_0000.xml.gz"
        section_path = os.path.join(save_dir, "Section_0000.xml")
        result = self.download_file(section_url, section_path)
        if result:
            static_files['section'] = result
        
        # 路段線型圖資資訊
        section_shape_url = f"{self.base_url}/history/motc20/Section/{date}/SectionShape_0000.xml.gz"
        section_shape_path = os.path.join(save_dir, "SectionShape_0000.xml")
        result = self.download_file(section_shape_url, section_shape_path)
        if result:
            static_files['section_shape'] = result
        
        # 壅塞水準資訊
        congestion_url = f"{self.base_url}/history/motc20/Section/{date}/CongestionLevel_0000.xml.gz"
        congestion_path = os.path.join(save_dir, "CongestionLevel_0000.xml")
        result = self.download_file(congestion_url, congestion_path)
        if result:
            static_files['congestion'] = result
            
        return static_files
    
    def download_jilian_etag_data(self, date, save_dir=None):
        """下載宜蘭地區(坪林)eTag配對旅行時間資料
        
        Args:
            date: 日期格式YYYYMMDD
            save_dir: 儲存目錄
            
        Returns:
            下載的檔案路徑列表
        """
        if save_dir is None:
            save_dir = os.path.join(self.base_dir, "Jilian_ETag", date)
        
        os.makedirs(save_dir, exist_ok=True)
        
        files = []
        
        # 每5分鐘一筆資料
        for hour in range(24):
            for minute in range(0, 60, 5):
                time_str = f"{hour:02d}{minute:02d}"
                url = f"{self.base_url}/history/TDCS_PINGLIN/M04/{date}/R14C_M04{date}{time_str}.csv"
                save_path = os.path.join(save_dir, f"R14C_M04{date}{time_str}.csv")
                
                result = self.download_file(url, save_path)
                if result:
                    files.append(result)
                    
        return files
    
    def download_all_data_for_date(self, date, data_types=None):
        """下載指定日期的所有類型資料，帶進度顯示"""
        if data_types is None:
            # 企劃書需要的所有資料類型
            data_types = [
                "VD", "ETag", "News", "Traffic", "Static", 
                "M03A", "M04A", "M05A", "M06A", "M08A", "Jilian_ETag"
            ]
        
        total_files = 0
        print(f"===== 開始下載 {date} 的資料 =====")
        
        # 靜態資料只需下載一次
        if "Static" in data_types:
            print(f"[{date}] 下載靜態資料中...")
            static_files = self.download_static_data(date)
            total_files += len(static_files)
            print(f"[{date}] 靜態資料下載完成: {len(static_files)} 個檔案")
        
        # 動態資料需要按小時下載
        for hour in tqdm(range(24), desc=f"[{date}] 處理24小時資料", ncols=100):
            hour_str = f"{hour:02d}"
            
            if "VD" in data_types:
                print(f"[{date}] 下載 {hour_str}時 VD 資料中...")
                vd_files = self.download_vd_data(date, hour_str)
                total_files += len(vd_files)
                print(f"[{date}] {hour_str}時 VD 資料下載完成: {len(vd_files)} 個檔案")
            
            if "ETag" in data_types:
                print(f"[{date}] 下載 {hour_str}時 ETag 資料中...")
                etag_files = self.download_etag_data(date, hour_str)
                total_files += len(etag_files)
                print(f"[{date}] {hour_str}時 ETag 資料下載完成: {len(etag_files)} 個檔案")
            
            if "News" in data_types:
                print(f"[{date}] 下載 {hour_str}時 事件資料中...")
                news_files = self.download_news_data(date, hour_str)
                total_files += len(news_files)
                print(f"[{date}] {hour_str}時 事件資料下載完成: {len(news_files)} 個檔案")
            
            if "Traffic" in data_types:
                print(f"[{date}] 下載 {hour_str}時 路況資料中...")
                traffic_files = self.download_traffic_data(date, hour_str)
                total_files += len(traffic_files)
                print(f"[{date}] {hour_str}時 路況資料下載完成: {len(traffic_files)} 個檔案")
        
        # 下載站點資料
        for data_type in ["M03A", "M04A", "M05A"]:
            if data_type in data_types:
                print(f"[{date}] 下載 {data_type} 資料中...")
                station_files = self.download_station_based_data(data_type, date)
                total_files += len(station_files)
                print(f"[{date}] {data_type} 資料下載完成: {len(station_files)} 個檔案")
        
        # 下載旅次資料
        for data_type in ["M06A", "M08A"]:
            if data_type in data_types:
                print(f"[{date}] 下載 {data_type} 資料中...")
                route_files = self.download_route_data(data_type, date)
                total_files += len(route_files)
                print(f"[{date}] {data_type} 資料下載完成: {len(route_files)} 個檔案")
        
        # 下載宜蘭地區eTag資料
        if "Jilian_ETag" in data_types:
            print(f"[{date}] 下載宜蘭地區eTag資料中...")
            jilian_files = self.download_jilian_etag_data(date)
            total_files += len(jilian_files)
            print(f"[{date}] 宜蘭地區eTag資料下載完成: {len(jilian_files)} 個檔案")
        
        print(f"===== {date} 的資料下載完成，共 {total_files} 個檔案 =====")
        return total_files
    
    def download_multiple_dates(self, dates, data_types=None, max_workers=4):
        """下載多個日期的資料，帶更完善的進度顯示"""
        total_files = 0
        
        print(f"開始下載 {len(dates)} 天的資料，使用 {max_workers} 個並行處理線程")
        print("-" * 60)
        
        # 先顯示將要下載的日期
        print("計劃下載以下日期的資料:")
        for i, date in enumerate(dates):
            print(f"{i+1}. {date[:4]}/{date[4:6]}/{date[6:]}")
        print("-" * 60)
        
        # 使用並行處理加速下載
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {
                executor.submit(self.download_all_data_for_date, date, data_types): date
                for date in dates
            }
            
            # 使用tqdm顯示總體進度
            for future in tqdm(concurrent.futures.as_completed(future_to_date), 
                            total=len(dates), desc="總體下載進度", ncols=100):
                date = future_to_date[future]
                try:
                    files = future.result()
                    total_files += files
                    print(f"完成日期 {date} 的資料下載，共 {files} 個檔案")
                except Exception as e:
                    print(f"下載 {date} 資料時出錯: {e}")
        
        print("=" * 60)
        print(f"所有資料下載完成，共 {total_files} 個檔案")
        print("=" * 60)
        
        return total_files
    
    def process_vd_xml_to_df(self, xml_file):
        """解析VD XML檔案轉為DataFrame
        
        類似您原本的函式，但進行了一些優化
        """
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
            print(f"解析XML時出錯: {e}")
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
                route_id_elem = vd.find("./RouteID", namespaces) or vd.find("./ns:RouteID", namespaces)
                
                if vd_id_elem is not None and route_id_elem is not None:
                    vd_id = vd_id_elem.text
                    route_id = route_id_elem.text
                    vd_to_section[vd_id] = route_id
            
            # 根據目標路段過濾數據
            target_routes = set()
            for section in self.target_sections.values():
                target_routes.add(section['route_id'])
            
            # 只保留目標路段的VD數據
            filtered_df = df[df['vd_id'].isin([vd_id for vd_id, route in vd_to_section.items() if route in target_routes])]
            
            return filtered_df
            
        except Exception as e:
            print(f"過濾目標路段時出錯: {e}")
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
            print(f"VD資料或靜態資料資料夾不存在: {date}")
            return pd.DataFrame()
        
        # 檢查靜態資料是否存在
        static_xml_file = os.path.join(static_dir, "VD_0000.xml")
        if not os.path.exists(static_xml_file):
            print(f"VD靜態資料不存在: {static_xml_file}")
            return pd.DataFrame()
        
        # 讀取所有VD資料
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
            print(f"沒有找到可用的VD資料: {date}")
            return pd.DataFrame()

    def get_data_for_analysis(self, start_date, end_date, data_types=None, weekend_only=True):
        """取得並分析指定日期範圍內的資料
        
        Args:
            start_date: 開始日期格式YYYYMMDD
            end_date: 結束日期格式YYYYMMDD
            data_types: 要下載的資料類型列表
            weekend_only: 是否只下載週末資料
            
        Returns:
            分析結果
        """
        # 獲取日期列表
        if weekend_only:
            dates = self.get_weekend_dates(start_date, end_date)
        else:
            dates = self.get_non_holiday_weekdays(start_date, end_date)
        
        # 下載所有資料
        self.download_multiple_dates(dates, data_types)
        
        # 分析資料
        results = []
        for date in dates:
            df = self.analyze_vd_data(date)
            if not df.empty:
                results.append({
                    'date': date,
                    'data': df
                })
        
        return results

# 主程式
if __name__ == "__main__":
    collector = FreewayDataCollector(base_dir="highway_data")
    
    # 設定下載參數 - 修改為 2025年1月1日到4月30日
    start_date = "20250101"  # 114年1月1日
    end_date = "20250430"    # 114年4月30日
    
    # 設定要下載的資料類型 - 只保留 ETag 相關資料
    data_types = [
        "ETag", "Static"  # Static 是必要的，因為它包含 ETag 靜態資訊
    ]
    
    # 產生時間範圍內的所有日期（不分平日假日）
    all_dates = []
    current = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    while current <= end:
        all_dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    print(f"將下載 {len(all_dates)} 天的 ETag 資料")
    
    # 下載所有日期的資料
    all_results = collector.download_multiple_dates(all_dates, data_types)
    
    print(f"資料下載完成，共 {len(all_dates)} 天")