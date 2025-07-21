
# 目錄結構

<pre>
你的程式資料夾/  
│  
├── main.py           ← 主程式（每10分鐘檢查）  
├── processor.py      ← a程式（處理CSV轉檔）  
├── 下載資料/  
│   └── *.csv           手動下載的資料皆放置於此。
|                       檢查該資料夾有csv檔案，將自動轉入資料庫中。
|   schema/             schema資料夾，定義資料表的欄位格式
|   schema.py           根據schema資料夾，向資料庫建立資料表。
└── 已處理資料夾/      ← 當資料轉換至資料庫後，原始資料將放置於此。
                        
</pre>

## 主程式
import time  
import os  
from processor import process_csv_files  

WATCH_FOLDER = '下載資料'  
PROCESSED_FOLDER = '已處理資料夾'  
CHECK_INTERVAL = 600  # 10分鐘（以秒計）  
<pre>
def has_csv_files(folder):
    return any(file.endswith('.csv') for file in os.listdir(folder))

def main():
    while True:
        if has_csv_files(WATCH_FOLDER):
            print("檢測到 CSV 檔案，開始處理...")
            process_csv_files(WATCH_FOLDER, PROCESSED_FOLDER)
        else:
            print("未檢測到 CSV 檔案，結束程序。")
            break
        print(f"等待 {CHECK_INTERVAL // 60} 分鐘後再次檢查...")
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    main()
</pre>
## 轉檔
<pre>
import os
import pandas as pd
import shutil

def process_csv_files(input_folder, output_folder):
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            input_path = os.path.join(input_folder, file_name)
            output_path = os.path.join(output_folder, file_name)

            try:
                # 示例轉檔處理（這裡只加了一欄資料做轉換）
                df = pd.read_csv(input_path)
                df['處理狀態'] = '已轉檔'
                df.to_csv(output_path, index=False)
                print(f"已處理檔案：{file_name}")

                # 處理完畢後刪除原始檔（或你可以選擇移動/備份）
                os.remove(input_path)

            except Exception as e:
                print(f"處理 {file_name} 發生錯誤：{e}")
</pre>



<pre>
你的程式資料夾/  
│  
├── main.py           ← 主程式（每10分鐘檢查）  
├── processor.py      ← a程式（處理CSV轉檔）  
├── a資料夾/  
│   └── *.csv  
└── 以處理資料夾/
</pre>