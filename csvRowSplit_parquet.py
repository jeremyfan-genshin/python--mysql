import pandas as pd

# 設定檔案路徑與名稱
input_csv_path = r'air_quality_2016.csv'
output_prefix = 'air_quality_2016_part'

# 設定每個檔案的切割大小
chunk_size = 1000

# 讀取原始 CSV
df = pd.read_csv(input_csv_path, low_memory=False)

# 計算總資料筆數
total_rows = len(df)

# 切分資料並儲存
first = True  # 控制是否寫入欄位名稱（header）

# 切分成三個檔案
df_part1 = df.iloc[:chunk_size]  # 第一份 1000 筆
df_part2 = df.iloc[chunk_size:chunk_size*2]  # 第二份 1000 筆
df_part3 = df.iloc[chunk_size*2:]  # 第三份 剩餘的資料

# 儲存三個檔案為 Parquet 格式
df_part1.to_parquet(f'{output_prefix}_1.parquet', index=False, engine='pyarrow')
df_part2.to_parquet(f'{output_prefix}_2.parquet', index=False, engine='pyarrow')
df_part3.to_parquet(f'{output_prefix}_3.parquet', index=False, engine='pyarrow')

print("✅ 完成切分並儲存為三個 Parquet 檔案")
