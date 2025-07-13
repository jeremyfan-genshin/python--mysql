import pandas as pd

# 讀取 CSV
csv_path = r'E:\jeremyOnly\projects\J-Power-Code-to-Insight\codeTest\air_quality_2016.csv'
df = pd.read_csv(csv_path)

# 儲存為 Parquet
parquet_path = r'E:\jeremyOnly\projects\J-Power-Code-to-Insight\codeTest\air_quality2016.parquet'
df.to_parquet(parquet_path, index=False, engine='pyarrow')

print(f'✅ 已轉換並儲存為 Parquet：{parquet_path}')
