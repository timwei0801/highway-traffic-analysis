import xml.etree.ElementTree as ET
import re
import os

def extract_etag_data(xml_file_path, yangmei_to_hsinchu_output=None, hsinchu_to_yangmei_output=None):
    """
    從XML檔案中擷取國道三號土城到新竹系統雙向的ETag資料
    - 土城到新竹系統方向：起點里程數約44.7公里，終點里程數約102.2公里
    - 新竹系統到土城方向：起點里程數約102.2公里，終點里程數約44.7公里
    """
    # 解析XML檔案
    namespaces = {
        '': 'http://traffic.transportdata.tw/standard/traffic/schema/',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
    
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    
    # 使用正確的命名空間來找到所有ETagPairLive元素
    etag_pair_lives = root.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairLive')
    
    # 儲存符合條件的資料
    yangmei_to_hsinchu_data = []  # 土城到新竹系統方向
    hsinchu_to_yangmei_data = []  # 新竹系統到土城方向
    
    # 正則表達式用於檢查ETagPairID是否符合國道一號的格式
    # 01F表示國道一號，後面的數字是里程數
    etag_pattern = re.compile(r'01F(\d{4})([NS])-01F(\d{4})([NS])')
    
    for etag_pair_live in etag_pair_lives:
        # 取得ETagPairID元素
        etag_pair_id_elem = etag_pair_live.find('.//{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairID')
        
        if etag_pair_id_elem is not None:
            etag_pair_id = etag_pair_id_elem.text
            match = etag_pattern.match(etag_pair_id)
            
            if match:
                # 取得兩個里程數和方向
                start_km = int(match.group(1))
                start_dir = match.group(2)
                end_km = int(match.group(3))
                end_dir = match.group(4)
                
                # 確認里程數是否在楊梅到新竹的範圍內（447到1022）
                if (447 <= start_km <= 1022 and 447 <= end_km <= 1022):
                    # 確定行駛方向
                    etag_data = ET.tostring(etag_pair_live, encoding='unicode')
                    
                    # 楊梅到新竹方向（里程數增加）
                    if start_km < end_km:
                        yangmei_to_hsinchu_data.append(etag_data)
                    
                    # 新竹到楊梅方向（里程數減少）
                    elif start_km > end_km:
                        hsinchu_to_yangmei_data.append(etag_data)
    
    # 輸出土城到新竹系統方向的資料
    if yangmei_to_hsinchu_output:
        with open(yangmei_to_hsinchu_output, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<ETagPairLiveList xmlns="http://traffic.transportdata.tw/standard/traffic/schema/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n')
            for data in yangmei_to_hsinchu_data:
                f.write(data)
                f.write('\n')
            f.write('</ETagPairLiveList>')
        print(f"已將土城到新竹系統方向的ETag資料輸出至 {yangmei_to_hsinchu_output}")
    
    # 輸出新竹系統到土城方向的資料
    if hsinchu_to_yangmei_output:
        with open(hsinchu_to_yangmei_output, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<ETagPairLiveList xmlns="http://traffic.transportdata.tw/standard/traffic/schema/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n')
            for data in hsinchu_to_yangmei_data:
                f.write(data)
                f.write('\n')
            f.write('</ETagPairLiveList>')
        print(f"已將新竹系統到土城方向的ETag資料輸出至 {hsinchu_to_yangmei_output}")
    
    return {
        "yangmei_to_hsinchu": yangmei_to_hsinchu_data,
        "hsinchu_to_yangmei": hsinchu_to_yangmei_data
    }

if __name__ == "__main__":
    # 創建輸出目錄
    output_dir = "processed_etag_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # 設定基礎路徑（改為相對路徑）
    base_dir = "highway_data"  # 不要加斜線開頭
    
    # 處理檔案計數
    processed_count = 0
    error_count = 0
    
    for mon in range(4, 5):
        # 根據月份決定天數
        if mon == 1 or mon == 3:
            last_day = 31  # 1月和3月有31天
        elif mon == 2:
            last_day = 29  # 2月有29天（假設是閏年）
        else:
            last_day = 30  # 其他月份
        
        # 每隔5天取一個日期
        for day in range(1, last_day + 1, 1):
            date_str = f"2025{mon:02d}{day:02d}"
            
            # 為每個日期創建輸出目錄
            date_output_dir = os.path.join(output_dir, date_str)
            os.makedirs(date_output_dir, exist_ok=True)
            
            # 處理每個時間點的檔案
            for hour in range(24):
                for minute in range(0, 60, 5):
                    # 構建檔案路徑
                    file_time = f"{hour:02d}{minute:02d}"
                    xml_file_path = os.path.join(base_dir, "ETag", date_str, f"ETagPairLive_{file_time}.xml")
                    
                    # 構建輸出檔案路徑
                    yangmei_output = os.path.join(date_output_dir, f"國三_土城到新竹系統_{file_time}.xml")
                    hsinchu_output = os.path.join(date_output_dir, f"國三_新竹系統到土城_{file_time}.xml")
                    
                    # 檢查檔案是否存在
                    if os.path.exists(xml_file_path):
                        try:
                            # 處理檔案
                            result = extract_etag_data(xml_file_path, yangmei_output, hsinchu_output)
                            processed_count += 1
                            
                            # 顯示進度
                            if processed_count % 10 == 0:
                                print(f"已處理 {processed_count} 個檔案")
                        except Exception as e:
                            print(f"處理檔案 {xml_file_path} 時發生錯誤: {str(e)}")
                            error_count += 1
                    else:
                        print(f"檔案不存在，跳過: {xml_file_path}")
                        error_count += 1
    
    print(f"處理完成！成功處理 {processed_count} 個檔案，跳過 {error_count} 個檔案")