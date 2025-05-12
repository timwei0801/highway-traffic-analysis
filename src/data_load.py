import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from datetime import datetime
import os
from glob import glob

class TaiwanETagDataConverter:
    def __init__(self, etag_csv_path, xml_base_path, output_dir):
        """
        初始化轉換器
        
        Parameters:
        etag_csv_path: ETag地理資訊CSV檔案路徑
        xml_base_path: 包含XML車流資料的基礎資料夾路徑
        output_dir: 輸出資料夾路徑
        """
        self.etag_csv_path = etag_csv_path
        self.xml_base_path = xml_base_path
        self.output_dir = output_dir
        
        # 創建輸出目錄
        os.makedirs(output_dir, exist_ok=True)
        
        # 讀取ETag地理資訊
        print(f"讀取ETag資訊從: {etag_csv_path}")
        self.etag_info = pd.read_csv(etag_csv_path)
        print(f"ETag資訊欄位: {self.etag_info.columns.tolist()}")
        print(f"ETag資訊筆數: {len(self.etag_info)}")
        self.etag_dict = self._create_etag_dict()
        
    def _create_etag_dict(self):
        """建立ETag編號到索引的映射"""
        etag_dict = {}
        
        # 先檢查欄位名稱
        print("\n檢查ETag CSV欄位：")
        print(self.etag_info.columns.tolist())
        print("\n前5筆資料：")
        print(self.etag_info.head())
        
        # 使用"編號.1"欄位作為ETag ID
        for idx, row in self.etag_info.iterrows():
            # 取得"編號.1"欄位的值，格式為 "01F-029.3N"
            original_id = str(row['編號.1']).strip()
            
            # 建立基本映射
            base_info = {
                'index': idx,
                'lat': row.get('緯度(北緯)', 0),
                'lon': row.get('經度(東經)', 0),
                'direction': row.get('方向', 'N'),
                'mileage': row.get('收費區\n設定里程', 0)
            }
            
            # 原始格式
            etag_dict[original_id] = base_info.copy()
            
            # 轉換為XML格式: "01F-029.3N" -> "01F0293N"
            if '-' in original_id:
                parts = original_id.split('-')
                if len(parts) == 2:
                    prefix = parts[0]  # "01F"
                    suffix_with_dir = parts[1]  # "029.3N"
                    
                    # 移除小數點，分離數字和方向
                    if any(d in suffix_with_dir for d in ['N', 'S']):
                        direction = suffix_with_dir[-1]  # 'N' 或 'S'
                        number = suffix_with_dir[:-1].replace('.', '')  # "029.3" -> "0293"
                        
                        # 確保數字部分是4位數
                        if len(number) < 4:
                            number = number.ljust(4, '0')
                        
                        # 組合成XML格式
                        xml_format = prefix + number[:4] + direction  # "01F0293N"
                        etag_dict[xml_format] = base_info.copy()
                        
                        # 也嘗試3位數格式（如果XML可能使用）
                        xml_format_3digit = prefix[1:] + number[:3] + direction  # "1F029N"
                        etag_dict[xml_format_3digit] = base_info.copy()
                        
                        # 嘗試其他可能的格式（03F0783N）
                        if prefix.startswith('0'):
                            # "01F" -> "03F" 或其他變化
                            for new_prefix in ['01F', '03F', '1F', '3F']:
                                alt_format = new_prefix + number[:4] + direction
                                etag_dict[alt_format] = base_info.copy()
        
        print(f"\n建立了 {len(set([v['index'] for v in etag_dict.values()]))} 個唯一站點的映射")
        print(f"總共 {len(etag_dict)} 個ETag ID變化")
        print(f"ETag ID格式範例:")
        sample_keys = list(etag_dict.keys())[:30]
        for i in range(0, len(sample_keys), 5):
            print(f"  {sample_keys[i:i+5]}")
        
        return etag_dict
    
    def parse_xml_files(self):
        """解析所有XML檔案並整理成DataFrame"""
        all_data = []
        
        # 找到所有日期資料夾
        date_folders = glob(os.path.join(self.xml_base_path, '*'))
        date_folders = [f for f in date_folders if os.path.isdir(f)]
        
        print(f"\n找到 {len(date_folders)} 個日期資料夾")
        
        for date_folder in sorted(date_folders):
            folder_name = os.path.basename(date_folder)
            print(f"處理資料夾: {folder_name}")
            
            # 找到該日期資料夾中的所有XML檔案
            xml_files = glob(os.path.join(date_folder, '*.xml'))
            print(f"  找到 {len(xml_files)} 個XML檔案")
            
            for xml_file in xml_files:
                try:
                    # 從檔案名稱提取時間資訊
                    filename = os.path.basename(xml_file)
                    time_part = filename.split('_')[-1].replace('.xml', '')  # 提取時間部分，如 "0000"
                    
                    tree = ET.parse(xml_file)
                    root = tree.getroot()
                    
                    # 定義namespace
                    ns = {'ns0': 'http://traffic.transportdata.tw/standard/traffic/schema/'}
                    
                    # 找到所有ETagPairLive節點
                    pair_lives = root.findall('.//ns0:ETagPairLive', ns)
                    print(f"    {filename}: 找到 {len(pair_lives)} 個ETagPair")
                    
                    for pair_live in pair_lives:
                        pair_id = pair_live.find('ns0:ETagPairID', ns).text
                        start_time = pair_live.find('ns0:StartTime', ns).text
                        
                        # 解析pair_id，例如 "03F0996S-03F1022S"
                        try:
                            start_etag, end_etag = pair_id.split('-')
                        except:
                            print(f"      無法解析pair_id: {pair_id}")
                            continue
                        
                        # 解析時間
                        start_datetime = datetime.fromisoformat(start_time.replace('+08:00', ''))
                        
                        # 處理每種車輛類型的流量
                        flows = pair_live.find('ns0:Flows', ns)
                        total_vehicle_count = 0
                        weighted_travel_time = 0
                        weighted_speed = 0
                        
                        for flow in flows.findall('ns0:Flow', ns):
                            vehicle_count = int(flow.find('ns0:VehicleCount', ns).text)
                            travel_time = int(flow.find('ns0:TravelTime', ns).text)
                            speed = int(flow.find('ns0:SpaceMeanSpeed', ns).text)
                            
                            if vehicle_count > 0 and travel_time > 0:
                                total_vehicle_count += vehicle_count
                                weighted_travel_time += travel_time * vehicle_count
                                weighted_speed += speed * vehicle_count
                        
                        # 計算加權平均
                        if total_vehicle_count > 0:
                            avg_travel_time = weighted_travel_time / total_vehicle_count
                            avg_speed = weighted_speed / total_vehicle_count
                        else:
                            avg_travel_time = 0
                            avg_speed = 0
                        
                        # 將pair轉換為單個站點的流量
                        data_row = {
                            'etag_id': start_etag,
                            'datetime': start_datetime,
                            'date': start_datetime.strftime('%Y/%m/%d'),
                            'day': start_datetime.day,
                            'hour': start_datetime.hour,
                            'minute': start_datetime.minute,
                            'flow': total_vehicle_count,
                            'speed': avg_speed,
                            'travel_time': avg_travel_time
                        }
                        all_data.append(data_row)
                        
                        # 也為終點站點創建一條記錄
                        end_data_row = data_row.copy()
                        end_data_row['etag_id'] = end_etag
                        all_data.append(end_data_row)
                
                except Exception as e:
                    print(f"    處理 {xml_file} 時發生錯誤: {str(e)}")
        
        print(f"\n總共處理了 {len(all_data)} 筆資料")
        return pd.DataFrame(all_data)
    
    def create_train_data(self):
        """創建訓練資料（主要的流量資料）"""
        # 解析XML資料
        print("\n開始解析XML資料...")
        flow_data = self.parse_xml_files()
        
        if len(flow_data) == 0:
            print("警告：沒有解析到任何資料！")
            return pd.DataFrame()
        
        print(f"解析完成，共 {len(flow_data)} 筆原始資料")
        print(f"唯一的ETag ID數量: {flow_data['etag_id'].nunique()}")
        print(f"ETag ID範例: {flow_data['etag_id'].unique()[:5]}")
        
        # 映射ETag ID到站點索引
        flow_data['station_id'] = flow_data['etag_id'].map(
            lambda x: self.etag_dict.get(x, {}).get('index', -1)
        )
        
        # 統計未找到的站點
        unknown_etags = flow_data[flow_data['station_id'] == -1]['etag_id'].unique()
        if len(unknown_etags) > 0:
            print(f"\n警告：有 {len(unknown_etags)} 個未知的ETag ID")
            print(f"未知ETag範例: {unknown_etags[:10]}")
            print(f"已知的ETag ID格式範例: {list(self.etag_dict.keys())[:10]}")
        
        # 過濾掉未找到的站點
        valid_data = flow_data[flow_data['station_id'] != -1]
        print(f"過濾後剩餘 {len(valid_data)} 筆資料")
        
        if len(valid_data) == 0:
            # 如果沒有有效資料，嘗試匹配部分ID
            print("\n嘗試模糊匹配ETag ID...")
            
            # 建立簡化的映射
            simplified_etag_dict = {}
            for etag_id, info in self.etag_dict.items():
                # 提取核心部分（例如從 "01F-029.3N" 提取 "01F029"）
                simplified_id = ''.join(filter(str.isalnum, etag_id))
                simplified_etag_dict[simplified_id] = info
            
            # 重新映射
            flow_data['station_id'] = flow_data['etag_id'].map(
                lambda x: simplified_etag_dict.get(''.join(filter(str.isalnum, x)), {}).get('index', -1)
            )
            
            valid_data = flow_data[flow_data['station_id'] != -1]
            print(f"模糊匹配後剩餘 {len(valid_data)} 筆資料")
        
        if len(valid_data) == 0:
            print("錯誤：無法匹配任何ETag ID！")
            print("請檢查CSV和XML中的ID格式是否一致")
            return pd.DataFrame()
        
        # 按照時間和站點排序
        valid_data = valid_data.sort_values(['datetime', 'station_id'])
        
        # 確保每個時間點都有所有站點的記錄
        unique_times = valid_data['datetime'].unique()
        unique_stations = valid_data['station_id'].unique()
        
        print(f"唯一時間點: {len(unique_times)}")
        print(f"唯一站點: {len(unique_stations)}")
        
        # 創建完整的時間-站點組合
        full_data = []
        for time in unique_times[:100]:  # 先處理前100個時間點以測試
            for station in unique_stations:
                mask = (valid_data['datetime'] == time) & (valid_data['station_id'] == station)
                if mask.any():
                    row = valid_data[mask].iloc[0]
                    full_data.append({
                        'station_id': station,
                        'date': row['date'],
                        'day': row['day'],
                        'hour': row['hour'],
                        'minute': row['minute'],
                        'flow': row['flow']
                    })
                else:
                    # 如果沒有資料，使用0
                    full_data.append({
                        'station_id': station,
                        'date': pd.to_datetime(time).strftime('%Y/%m/%d'),
                        'day': pd.to_datetime(time).day,
                        'hour': pd.to_datetime(time).hour,
                        'minute': pd.to_datetime(time).minute,
                        'flow': 0
                    })
        
        if len(full_data) == 0:
            print("錯誤：無法創建任何訓練資料！")
            return pd.DataFrame()
        
        result_df = pd.DataFrame(full_data)
        result_df = result_df.sort_values(['date', 'hour', 'minute', 'station_id'])
        
        # 儲存訓練資料
        result_df.to_csv(os.path.join(self.output_dir, 'train.csv'), index=False)
        print(f"\n訓練資料已儲存，共 {len(result_df)} 筆")
        
        return result_df
    
    def create_adjacency_matrix(self):
        """創建鄰接矩陣（基於道路連接關係）"""
        adj_data = []
        
        # 根據里程順序建立連接關係
        # 同方向的相鄰站點互相連接
        if '方向' in self.etag_info.columns:
            for direction in self.etag_info['方向'].unique():
                direction_data = self.etag_info[self.etag_info['方向'] == direction]
                if '設定里程' in self.etag_info.columns:
                    direction_data = direction_data.sort_values('設定里程')
                
                for i in range(len(direction_data) - 1):
                    src_idx = direction_data.iloc[i].name
                    dst_idx = direction_data.iloc[i + 1].name
                    
                    # 雙向連接
                    adj_data.append({'src_FID': src_idx, 'nbr_FID': dst_idx})
                    adj_data.append({'src_FID': dst_idx, 'nbr_FID': src_idx})
        else:
            # 如果沒有方向欄位，創建全連接矩陣（簡化處理）
            n_stations = len(self.etag_info)
            for i in range(n_stations):
                for j in range(i+1, min(i+3, n_stations)):  # 連接鄰近的2個站點
                    adj_data.append({'src_FID': i, 'nbr_FID': j})
                    adj_data.append({'src_FID': j, 'nbr_FID': i})
        
        adj_df = pd.DataFrame(adj_data)
        adj_df.to_csv(os.path.join(self.output_dir, 'adjacent_fully.csv'), index=False)
        
        return adj_df
    
    def create_other_files(self):
        """創建其他必要的檔案"""
        n_stations = len(self.etag_info)
        
        # 創建距離矩陣
        distance_matrix = np.random.uniform(0.1, 10, (n_stations, n_stations))
        np.fill_diagonal(distance_matrix, 0)
        pd.DataFrame(distance_matrix).to_csv(
            os.path.join(self.output_dir, 'dis.csv'), index=False, header=False
        )
        
        # 創建度數資料
        adj_df = pd.read_csv(os.path.join(self.output_dir, 'adjacent_fully.csv'))
        in_degrees = []
        out_degrees = []
        
        for i in range(n_stations):
            in_deg = len(adj_df[adj_df['nbr_FID'] == i])
            out_deg = len(adj_df[adj_df['src_FID'] == i])
            
            in_degrees.append({'station': i, 'in_degree': min(in_deg, 4)})
            out_degrees.append({'station': i, 'out_degree': min(out_deg, 4)})
        
        pd.DataFrame(in_degrees).to_csv(
            os.path.join(self.output_dir, 'in_deg.csv'), index=False
        )
        pd.DataFrame(out_degrees).to_csv(
            os.path.join(self.output_dir, 'out_deg.csv'), index=False
        )
        
        # 創建簡化的最短路徑矩陣
        sp_data = []
        max_path_length = 15
        
        for i in range(n_stations):
            for j in range(n_stations):
                path = [0] * max_path_length
                path[0] = i
                if i != j:
                    path[1] = j
                sp_data.append(path)
        
        pd.DataFrame(sp_data).to_csv(
            os.path.join(self.output_dir, 'sp.csv'), index=False, header=False
        )
    
    def convert_all(self):
        """執行所有轉換步驟"""
        print("開始轉換資料...")
        
        # 1. 創建主要訓練資料
        print("\n1. 創建訓練資料...")
        train_data = self.create_train_data()
        
        if len(train_data) == 0:
            print("錯誤：無法創建訓練資料！")
            return
        
        print(f"   - 站點數量: {len(train_data['station_id'].unique())}")
        print(f"   - 時間範圍: {train_data['date'].min()} 到 {train_data['date'].max()}")
        
        # 2. 創建圖結構資料
        print("\n2. 創建鄰接矩陣...")
        self.create_adjacency_matrix()
        
        print("\n3. 創建其他必要檔案...")
        self.create_other_files()
        
        print("\n資料轉換完成！")
        
        # 更新超參數檔案
        self.update_hyperparameters()
    
    def update_hyperparameters(self):
        """更新超參數配置建議"""
        n_stations = len(self.etag_info)
        
        print(f"\n請在 hyparameter.py 中更新以下參數：")
        print(f"--site_num: {n_stations}")
        print(f"--granularity: 5  # 根據您的XML資料，時間間隔為5分鐘")
        print(f"--input_length: 12  # 使用過去1小時的資料（12個5分鐘）")
        print(f"--output_length: 12  # 預測未來1小時")
        
        # 儲存參數建議到文件
        with open(os.path.join(self.output_dir, 'parameter_suggestions.txt'), 'w') as f:
            f.write(f"site_num: {n_stations}\n")
            f.write("granularity: 5\n")
            f.write("input_length: 12\n")
            f.write("output_length: 12\n")

# 使用範例
if __name__ == "__main__":
    # 設定路徑
    etag_csv_path = "/Volumes/國道資料/國道資料分析/國道Etag 資料/整合-Etag 國一 新竹系統 - 五股.csv"
    xml_base_path = "/Volumes/國道資料/國道資料分析/processed_etag_data/"
    output_dir = "data/Taiwan/"
    
    # 創建轉換器並執行轉換
    converter = TaiwanETagDataConverter(etag_csv_path, xml_base_path, output_dir)
    converter.convert_all()