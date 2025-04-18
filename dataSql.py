import os
import xml.etree.ElementTree as ET
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, DateTime, Index

class HighwayDataImporter:
    """高速公路資料匯入工具"""
    
    def __init__(self, db_config):
        """初始化資料匯入工具"""
        self.db_config = db_config
        self.connection = None
        self.engine = None
        self.debug_mode = False
        # 定義常用的命名空間
        self.namespaces = {
            'ns': 'http://traffic.transportdata.tw/standard/traffic/schema/'
        }
    
    def set_debug_mode(self, enabled=True):
        """設置除錯模式"""
        self.debug_mode = enabled
        print(f"除錯模式已{'啟用' if enabled else '停用'}")
    
    def connect_to_database(self):
        """建立資料庫連線"""
        try:
            connection_string = f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            self.connection = self.engine.connect()
            print(f"成功連接到 MySQL 數據庫 {self.db_config['database']}")
            return True
        except Exception as e:
            print(f"資料庫連線錯誤: {e}")
            return False
    
    def close_connection(self):
        """關閉資料庫連線"""
        if self.connection:
            self.connection.close()
            self.engine.dispose()
            print("資料庫連線已關閉")
            return True
        return False
    
    def create_tables_if_not_exist(self):
        """建立必要的資料表"""
        try:
            metadata = MetaData()
            
            # 定義 VD 資料表
            vd_table = Table(
                'vd_data', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('update_time', DateTime),
                Column('vd_id', String(50)),
                Column('lane_id', String(10)),
                Column('speed', Float),
                Column('occupancy', Float),
                Column('volume', Integer),
                Column('date_key', Date),
                Column('hour_key', Integer),
                Column('minute_key', Integer)
            )
            Index('idx_vd', vd_table.c.vd_id, vd_table.c.date_key, vd_table.c.hour_key, vd_table.c.minute_key)
            
            # 定義 ETag 資料表
            etag_table = Table(
                'etag_data', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('update_time', DateTime),
                Column('from_station', String(50)),
                Column('to_station', String(50)),
                Column('travel_time', Float),
                Column('date_key', Date),
                Column('hour_key', Integer),
                Column('minute_key', Integer)
            )
            Index('idx_etag', etag_table.c.from_station, etag_table.c.to_station, etag_table.c.date_key, etag_table.c.hour_key, etag_table.c.minute_key)
            
            # 建立資料表
            metadata.create_all(self.engine)
            print("資料表檢查/建立完成")
            return True
        except Exception as e:
            print(f"建立資料表錯誤: {e}")
            return False
    
    def remove_namespace(self, tree):
        """移除 XML 元素的命名空間前綴 (用於 VD 資料)"""
        root = tree.getroot()
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        return root
    
    def parse_vd_xml(self, xml_file, date_key):
        """解析VD XML檔案並返回資料列表"""
        try:
            tree = ET.parse(xml_file)
            # 移除命名空間
            root = self.remove_namespace(tree)
            
            # 提取更新時間
            update_time_elem = root.find(".//UpdateTime")
            update_time = update_time_elem.text if update_time_elem is not None else None
            
            # 提取小時和分鐘
            filename = os.path.basename(xml_file)
            if "VDLive_" in filename and len(filename) >= 12:
                time_part = filename.split("_")[1][:4]
                hour_key = int(time_part[:2])
                minute_key = int(time_part[2:4])
            else:
                # 如果檔名不符合預期格式，從更新時間提取
                if update_time and len(update_time) >= 16:
                    hour_key = int(update_time[11:13])
                    minute_key = int(update_time[14:16])
                else:
                    return []
            
            # 找出所有VD元素
            data_list = []
            vd_elements = root.findall(".//VDLive") or root.findall(".//VD")
            
            if self.debug_mode:
                print(f"XML 檔案 {os.path.basename(xml_file)} 找到 {len(vd_elements)} 個 VD 元素")
            
            for vd in vd_elements:
                vd_id_elem = vd.find("./VDID")
                if vd_id_elem is None:
                    continue
                
                vd_id = vd_id_elem.text
                
                # 處理每個車道
                lanes = vd.findall(".//Lane")
                
                if self.debug_mode and lanes:
                    print(f"VD {vd_id} 找到 {len(lanes)} 個車道")
                
                for lane in lanes:
                    lane_id_elem = lane.find("./LaneID")
                    speed_elem = lane.find("./Speed")
                    occupancy_elem = lane.find("./Occupancy")
                    volume_elem = lane.find("./Volume")
                    
                    if None in [lane_id_elem, speed_elem, occupancy_elem]:
                        continue
                    
                    lane_id = lane_id_elem.text
                    speed = float(speed_elem.text) if speed_elem.text else 0
                    occupancy = float(occupancy_elem.text) if occupancy_elem.text else 0
                    volume = int(volume_elem.text) if volume_elem is not None and volume_elem.text else 0
                    
                    data_list.append({
                        'update_time': update_time,
                        'vd_id': vd_id,
                        'lane_id': lane_id,
                        'speed': speed,
                        'occupancy': occupancy,
                        'volume': volume,
                        'date_key': date_key,
                        'hour_key': hour_key,
                        'minute_key': minute_key
                    })
            
            if self.debug_mode:
                print(f"從 {os.path.basename(xml_file)} 解析出 {len(data_list)} 筆資料")
            
            return data_list
        except Exception as e:
            print(f"解析VD XML出錯 ({xml_file}): {e}")
            return []
    
    def parse_etag_xml(self, xml_file, date_key):
        """解析ETag XML檔案並返回資料列表"""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 使用命名空間進行查詢
            update_time_elem = root.find(".//ns:UpdateTime", self.namespaces)
            update_time = update_time_elem.text if update_time_elem is not None else None
            
            # 提取小時和分鐘
            filename = os.path.basename(xml_file)
            if "ETagPairLive_" in filename and len(filename) >= 17:
                time_part = filename.split("_")[1][:4]
                hour_key = int(time_part[:2])
                minute_key = int(time_part[2:4])
            else:
                # 如果檔名不符合預期格式，從更新時間提取
                if update_time and len(update_time) >= 16:
                    hour_key = int(update_time[11:13])
                    minute_key = int(update_time[14:16])
                else:
                    return []
            
            # 找出所有ETagPair元素
            data_list = []
            pair_elements = root.findall(".//ns:ETagPairLive", self.namespaces)
            
            if self.debug_mode:
                print(f"XML 檔案 {os.path.basename(xml_file)} 找到 {len(pair_elements)} 個 ETagPair 元素")
            
            for pair in pair_elements:
                # 從 ETagPairID 中解析出 FromStation 和 ToStation
                pair_id_elem = pair.find("./ns:ETagPairID", self.namespaces)
                if pair_id_elem is None or not pair_id_elem.text or "-" not in pair_id_elem.text:
                    continue
                    
                # 解析 "FromStation-ToStation" 格式
                station_parts = pair_id_elem.text.split("-")
                if len(station_parts) != 2:
                    continue
                    
                from_station = station_parts[0]
                to_station = station_parts[1]
                
                # 尋找旅行時間資訊
                travel_time_elem = pair.find("./ns:TravelTime", self.namespaces)
                if travel_time_elem is not None and travel_time_elem.text:
                    travel_time = float(travel_time_elem.text)
                else:
                    # 嘗試從其他欄位計算，或使用默認值
                    space_mean_speed_elem = pair.find("./ns:SpaceMeanSpeed", self.namespaces)
                    # 這裡可以添加更多計算邏輯...
                    travel_time = 0  # 默認值
                
                data_list.append({
                    'update_time': update_time,
                    'from_station': from_station,
                    'to_station': to_station,
                    'travel_time': travel_time,
                    'date_key': date_key,
                    'hour_key': hour_key,
                    'minute_key': minute_key
                })
            
            if self.debug_mode:
                print(f"從 {os.path.basename(xml_file)} 解析出 {len(data_list)} 筆資料")
                if data_list:
                    print("第一筆資料樣本:")
                    print(data_list[0])
            
            return data_list
        except Exception as e:
            print(f"解析ETag XML出錯 ({xml_file}): {e}")
            return []
    
    def debug_xml_structure(self, xml_file):
        """診斷 XML 結構以協助調試"""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # 顯示原始結構
            print(f"\n--- 原始 XML 結構診斷: {os.path.basename(xml_file)} ---")
            print(f"根元素: {root.tag}")
            
            # 尋找所有元素標籤
            tags = set()
            for elem in root.iter():
                tags.add(elem.tag)
            
            print(f"發現的元素標籤: {', '.join(tags)}")
            
            # 檢測是否為 VD 或 ETag 檔案
            is_vd = "VDLive" in os.path.basename(xml_file)
            is_etag = "ETagPairLive" in os.path.basename(xml_file)
            
            if is_vd:
                # 對 VD 檔案使用移除命名空間的方法
                root = self.remove_namespace(tree)
                vd_elements = root.findall(".//VDLive") or root.findall(".//VD")
                etag_elements = []
                
                print(f"找到 VD 元素數量: {len(vd_elements)}")
                print(f"找到 ETag 元素數量: 0")
                
                # 如果有 VD 元素，顯示第一個的結構
                if vd_elements:
                    first_vd = vd_elements[0]
                    print("\n第一個 VD 元素的結構:")
                    for child in first_vd:
                        print(f"  {child.tag}: {child.text if child.text and child.text.strip() else '無'}")
                    
                    # 檢查是否有車道資訊
                    lanes = first_vd.findall(".//Lane")
                    print(f"  找到 Lane 元素數量: {len(lanes)}")
                    
                    if lanes:
                        first_lane = lanes[0]
                        print("  第一個車道的結構:")
                        for child in first_lane:
                            print(f"    {child.tag}: {child.text if child.text and child.text.strip() else '無'}")
            
            elif is_etag:
                # 對 ETag 檔案使用命名空間方法
                vd_elements = []
                etag_elements = root.findall(".//ns:ETagPairLive", self.namespaces)
                
                print(f"找到 VD 元素數量: 0")
                print(f"找到 ETag 元素數量: {len(etag_elements)}")
                
                # 如果有 ETag 元素，顯示第一個的結構
                if etag_elements:
                    first_etag = etag_elements[0]
                    print("\n第一個 ETag 元素的結構:")
                    for child in first_etag:
                        child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                        print(f"  {child_tag}: {child.text if child.text and child.text.strip() else '無'}")
                    
                    # 檢查具體欄位
                    pair_id_elem = first_etag.find('./ns:ETagPairID', self.namespaces)
                    travel_time_elem = first_etag.find('./ns:TravelTime', self.namespaces)
                    
                    print("\n重要欄位檢查:")
                    print(f"  ETagPairID: {pair_id_elem.text if pair_id_elem is not None else '無'}")
                    
                    if pair_id_elem is not None and "-" in pair_id_elem.text:
                        station_parts = pair_id_elem.text.split("-")
                        print(f"  解析的 FromStation: {station_parts[0]}")
                        print(f"  解析的 ToStation: {station_parts[1]}")
                    else:
                        print("  無法解析 FromStation 和 ToStation")
                        
                    print(f"  TravelTime: {travel_time_elem.text if travel_time_elem is not None else '無'}")
            
            print("--- 診斷結束 ---\n")
            return True
        except Exception as e:
            print(f"解析 XML 結構出錯 ({xml_file}): {e}")
            return False
    
    def bulk_insert_vd_data(self, data_list):
        """批量插入VD資料到資料庫"""
        if not data_list:
            return 0
        
        try:
            # 將資料轉換為 DataFrame 再插入
            df = pd.DataFrame(data_list)
            
            # 處理日期時間格式
            if 'update_time' in df.columns and df['update_time'].notna().any():
                df['update_time'] = pd.to_datetime(df['update_time'])
            
            # 將 date_key 轉換為 datetime.date 物件
            if 'date_key' in df.columns:
                df['date_key'] = pd.to_datetime(df['date_key']).dt.date
            
            # 使用 pandas to_sql 方法批量插入
            df.to_sql('vd_data', self.engine, if_exists='append', index=False)
            
            return len(data_list)
        except Exception as e:
            print(f"批量插入VD資料出錯: {e}")
            # 輸出第一筆資料做為參考
            if data_list and self.debug_mode:
                print("第一筆資料樣本:")
                print(data_list[0])
            return 0
    
    def bulk_insert_etag_data(self, data_list):
        """批量插入ETag資料到資料庫"""
        if not data_list:
            return 0
        
        try:
            # 將資料轉換為 DataFrame 再插入
            df = pd.DataFrame(data_list)
            
            # 處理日期時間格式
            if 'update_time' in df.columns and df['update_time'].notna().any():
                df['update_time'] = pd.to_datetime(df['update_time'])
            
            # 將 date_key 轉換為 datetime.date 物件
            if 'date_key' in df.columns:
                df['date_key'] = pd.to_datetime(df['date_key']).dt.date
            
            # 使用 pandas to_sql 方法批量插入
            df.to_sql('etag_data', self.engine, if_exists='append', index=False)
            
            return len(data_list)
        except Exception as e:
            print(f"批量插入ETag資料出錯: {e}")
            # 輸出第一筆資料做為參考
            if data_list and self.debug_mode:
                print("第一筆資料樣本:")
                print(data_list[0])
            return 0
    
    def import_vd_data(self, base_dir):
        """匯入VD資料"""
        vd_dir = os.path.join(base_dir, "VD")
        if not os.path.exists(vd_dir):
            print(f"VD資料目錄不存在: {vd_dir}")
            return 0
        
        total_rows = 0
        processed_files = 0
        
        # 遍歷所有日期目錄
        date_folders = [f for f in os.listdir(vd_dir) if os.path.isdir(os.path.join(vd_dir, f))]
        for date_folder in tqdm(date_folders, desc="處理VD日期"):
            date_path = os.path.join(vd_dir, date_folder)
            
            # 轉換日期格式 YYYYMMDD -> YYYY-MM-DD
            date_key = f"{date_folder[:4]}-{date_folder[4:6]}-{date_folder[6:8]}"
            
            # 處理該日期下的所有XML檔案
            xml_files = [f for f in os.listdir(date_path) if f.endswith('.xml')]
            
            if self.debug_mode and not xml_files:
                print(f"警告: {date_path} 下沒有 XML 檔案")
                continue
            
            # 如果在除錯模式下，只處理少量檔案
            if self.debug_mode:
                xml_files = xml_files[:5]  # 只處理前5個檔案
                print(f"除錯模式: 只處理 {date_folder} 的前 {len(xml_files)} 個檔案")
            
            for xml_file in tqdm(xml_files, desc=f"處理 {date_folder} 的VD資料", leave=False):
                file_path = os.path.join(date_path, xml_file)
                processed_files += 1
                
                # 解析XML
                data_list = self.parse_vd_xml(file_path, date_key)
                
                # 批量插入資料
                if data_list:
                    rows_inserted = self.bulk_insert_vd_data(data_list)
                    total_rows += rows_inserted
                elif self.debug_mode:
                    print(f"警告: {file_path} 未生成任何資料")
                    self.debug_xml_structure(file_path)
        
        print(f"處理了 {processed_files} 個 VD 檔案")
        return total_rows
    
    def import_etag_data(self, base_dir):
        """匯入ETag資料"""
        etag_dir = os.path.join(base_dir, "ETag")
        if not os.path.exists(etag_dir):
            print(f"ETag資料目錄不存在: {etag_dir}")
            return 0
        
        total_rows = 0
        processed_files = 0
        
        # 遍歷所有日期目錄
        date_folders = [f for f in os.listdir(etag_dir) if os.path.isdir(os.path.join(etag_dir, f))]
        for date_folder in tqdm(date_folders, desc="處理ETag日期"):
            date_path = os.path.join(etag_dir, date_folder)
            
            # 轉換日期格式 YYYYMMDD -> YYYY-MM-DD
            date_key = f"{date_folder[:4]}-{date_folder[4:6]}-{date_folder[6:8]}"
            
            # 處理該日期下的所有XML檔案
            xml_files = [f for f in os.listdir(date_path) if f.endswith('.xml')]
            
            if self.debug_mode and not xml_files:
                print(f"警告: {date_path} 下沒有 XML 檔案")
                continue
            
            # 如果在除錯模式下，只處理少量檔案
            if self.debug_mode:
                xml_files = xml_files[:5]  # 只處理前5個檔案
                print(f"除錯模式: 只處理 {date_folder} 的前 {len(xml_files)} 個檔案")
            
            for xml_file in tqdm(xml_files, desc=f"處理 {date_folder} 的ETag資料", leave=False):
                file_path = os.path.join(date_path, xml_file)
                processed_files += 1
                
                # 解析XML
                data_list = self.parse_etag_xml(file_path, date_key)
                
                # 批量插入資料
                if data_list:
                    rows_inserted = self.bulk_insert_etag_data(data_list)
                    total_rows += rows_inserted
                elif self.debug_mode:
                    print(f"警告: {file_path} 未生成任何資料")
                    self.debug_xml_structure(file_path)
        
        print(f"處理了 {processed_files} 個 ETag 檔案")
        return total_rows
    
    def import_all_data(self, base_dir):
        """匯入所有資料"""
        if not self.connect_to_database():
            print("無法連接到資料庫，匯入中止")
            return False
        
        try:
            # 先確保資料表存在
            if not self.create_tables_if_not_exist():
                print("建立資料表失敗，匯入中止")
                return False
            
            # 先檢查範例 XML 結構
            if self.debug_mode:
                print("檢查範例 XML 檔案結構...")
                self.check_sample_xml_files(base_dir)
            
            print("開始匯入VD資料...")
            vd_rows = self.import_vd_data(base_dir)
            print(f"共匯入 {vd_rows} 筆VD資料")
            
            print("開始匯入ETag資料...")
            etag_rows = self.import_etag_data(base_dir)
            print(f"共匯入 {etag_rows} 筆ETag資料")
            
            print(f"資料匯入完成，總共匯入 {vd_rows + etag_rows} 筆資料")
            return True
        except Exception as e:
            print(f"匯入資料時發生錯誤: {e}")
            return False
        finally:
            self.close_connection()
            
    def check_sample_xml_files(self, base_dir):
        """檢查範例 XML 檔案結構"""
        # 檢查 VD 範例
        vd_dir = os.path.join(base_dir, "VD")
        if os.path.exists(vd_dir):
            date_folders = [f for f in os.listdir(vd_dir) if os.path.isdir(os.path.join(vd_dir, f))]
            if date_folders:
                date_path = os.path.join(vd_dir, date_folders[0])
                xml_files = [f for f in os.listdir(date_path) if f.endswith('.xml')]
                if xml_files:
                    print("VD 檔案範例結構:")
                    self.debug_xml_structure(os.path.join(date_path, xml_files[0]))
                    
        # 檢查 ETag 範例
        etag_dir = os.path.join(base_dir, "ETag")
        if os.path.exists(etag_dir):
            date_folders = [f for f in os.listdir(etag_dir) if os.path.isdir(os.path.join(etag_dir, f))]
            if date_folders:
                date_path = os.path.join(etag_dir, date_folders[0])
                xml_files = [f for f in os.listdir(date_path) if f.endswith('.xml')]
                if xml_files:
                    print("ETag 檔案範例結構:")
                    self.debug_xml_structure(os.path.join(date_path, xml_files[0]))

if __name__ == "__main__":
    # 資料庫連線設定
    db_config = {
        'host': 'localhost',
        'database': 'highway_traffic',
        'user': 'root',
        'password': 'Tim0986985588='
    }
    
    # 資料目錄
    data_dir = "/Users/weiqihong/Library/Mobile Documents/com~apple~CloudDocs/碩士班/碩一下/國道分析/highway_data"
    
    # 讓使用者確認或修改路徑
    print(f"資料將從 {data_dir} 匯入")
    user_path = input("按 Enter 使用此路徑，或輸入新路徑: ")
    if user_path.strip():
        data_dir = user_path
    
    # 問是否啟用除錯模式
    debug_mode = input("是否啟用除錯模式? (y/n): ").lower().startswith('y')
    
    if not os.path.exists(data_dir):
        print(f"錯誤: 資料目錄 {data_dir} 不存在!")
    else:
        # 建立匯入工具並執行匯入
        importer = HighwayDataImporter(db_config)
        if debug_mode:
            importer.set_debug_mode(True)
        importer.import_all_data(data_dir)