# import pandas as pd

# # 讀取 CSV
# csv_path = r'air_quality.csv'
# df = pd.read_csv(csv_path)

# # 儲存為 Parquet
# parquet_path = r'air_quality.parquet'
# df.to_parquet(parquet_path, index=False, engine='pyarrow')

# print(f'✅ 已轉換並儲存為 Parquet：{parquet_path}')


import pandas as pd

# 設定檔案路徑與名稱
input_csv_path = r'air_quality.csv'
output_parquet_path = r'air_quality.parquet'


# 讀取 CSV，並解決 DtypeWarning
df = pd.read_csv(input_csv_path, low_memory=False)

# 強制將數值欄位轉換為浮點數，這裡假設 'so2' 是需要轉換的欄位
df['so2'] = pd.to_numeric(df['so2'], errors='coerce')  # errors='coerce' 會將無法轉換的值變為 NaN

# 儲存為 Parquet
df.to_parquet(output_parquet_path, index=False, engine='pyarrow')

print(f'✅ 已轉換並儲存為 Parquet：{output_parquet_path}')
