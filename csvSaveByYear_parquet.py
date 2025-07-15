import pandas as pd
import os

# 設定檔案路徑與欄位名稱
input_csv_path = r'air_quality.csv'
output_dir = r'air_quality_parquet_sep'
date_column = 'date'
year_column = 'year'

# 設定是否包含欄位名稱的變數
first = True

# 讀取 CSV，避免 dtype 警告
df = pd.read_csv(input_csv_path, low_memory=False)

# 檢查 'date' 欄位是否存在
if date_column not in df.columns:
    raise ValueError(f"資料中沒有 '{date_column}' 欄位")

# 將 'date' 欄位轉換為 datetime 格式（處理混合格式）
df[date_column] = pd.to_datetime(df[date_column], format='mixed', errors='coerce')

# 移除轉換失敗的日期
df = df.dropna(subset=[date_column])

# 取年份，並加入 'year' 欄位
df[year_column] = df[date_column].dt.year

# 建立資料夾（如果資料夾已存在，則不會重複創建）
os.makedirs(output_dir, exist_ok=True)

# 依年份分檔並儲存為 Parquet 格式
for year, group in df.groupby(year_column):
    # 使用完整路徑來儲存檔案
    output_filename = os.path.join(output_dir, f'air_quality_{year}.parquet')
    group.to_parquet(output_filename, index=False, engine='pyarrow')  # 使用 pyarrow 引擎
    print(f'✅ 已儲存：{output_filename}')
