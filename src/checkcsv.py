import pandas as pd
import xml.etree.ElementTree as ET
from glob import glob
import os

def diagnose_etag_format():
    """診斷ETag格式問題"""
    
    # 1. 檢查CSV檔案
    print("=== 檢查CSV檔案 ===")
    csv_path = "/Volumes/國道資料/國道資料分析/國道Etag 資料/整合-Etag 國一 新竹系統 - 五股.csv"
    
    # 嘗試不同的編碼
    for encoding in ['utf-8', 'big5', 'cp950']:
        try:
            df = pd.read_csv(csv_path, encoding=encoding)
            print(f"成功使用 {encoding} 編碼讀取CSV")
            break
        except:
            print(f"無法使用 {encoding} 編碼")
    
    print(f"\nCSV欄位: {df.columns.tolist()}")
    print(f"\n前10筆資料:")
    print(df.head(10))
    
    # 檢查編號欄位
    if '編號' in df.columns:
        print(f"\n編號欄位的唯一值（前20個）:")
        print(df['編號'].unique()[:20])
    
    # 2. 檢查XML檔案
    print("\n\n=== 檢查XML檔案 ===")
    xml_base_path = "/Volumes/國道資料/國道資料分析/processed_etag_data/"
    
    # 找到第一個XML檔案
    date_folders = glob(os.path.join(xml_base_path, '*'))
    date_folders = [f for f in date_folders if os.path.isdir(f)]
    
    if date_folders:
        first_date = sorted(date_folders)[0]
        xml_files = glob(os.path.join(first_date, '*.xml'))
        
        if xml_files:
            first_xml = xml_files[0]
            print(f"檢查檔案: {first_xml}")
            
            tree = ET.parse(first_xml)
            root = tree.getroot()
            ns = {'ns0': 'http://traffic.transportdata.tw/standard/traffic/schema/'}
            
            # 找到前10個ETagPairID
            pair_ids = []
            for pair_live in root.findall('.//ns0:ETagPairLive', ns)[:10]:
                pair_id = pair_live.find('ns0:ETagPairID', ns).text
                pair_ids.append(pair_id)
            
            print(f"\nETagPairID範例（前10個）:")
            for pid in pair_ids:
                print(pid)
            
            # 分析ID格式
            if pair_ids:
                print(f"\n分析第一個ID: {pair_ids[0]}")
                parts = pair_ids[0].split('-')
                print(f"起始站: {parts[0]}")
                print(f"結束站: {parts[1]}")
    
    # 3. 建議映射方式
    print("\n\n=== 建議的映射方式 ===")
    if '編號' in df.columns:
        csv_id_sample = str(df['編號'].iloc[0])
        print(f"CSV中的ID格式: {csv_id_sample}")
        
        if pair_ids:
            xml_id_sample = pair_ids[0].split('-')[0]
            print(f"XML中的ID格式: {xml_id_sample}")
            
            # 嘗試找出轉換規則
            print("\n可能的轉換規則:")
            # 規則1: 移除破折號和小數點
            csv_simple = csv_id_sample.replace('-', '').replace('.', '')
            print(f"1. CSV簡化: {csv_id_sample} -> {csv_simple}")
            
            # 規則2: 加上方向
            if '方向' in df.columns:
                direction = df['方向'].iloc[0]
                csv_with_dir = csv_simple + direction
                print(f"2. 加上方向: {csv_simple} + {direction} -> {csv_with_dir}")
            
            # 規則3: 特定格式轉換
            if '-' in csv_id_sample:
                parts = csv_id_sample.split('-')
                if len(parts) == 2:
                    prefix = parts[0]
                    suffix = parts[1].replace('.', '')
                    if len(suffix) < 4:
                        suffix = suffix.zfill(4)
                    csv_formatted = prefix + suffix
                    print(f"3. 格式化: {csv_id_sample} -> {csv_formatted}")
    
    return df

# 執行診斷
if __name__ == "__main__":
    diagnose_etag_format()