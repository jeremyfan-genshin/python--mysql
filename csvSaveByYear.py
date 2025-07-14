import pandas as pd

# # 指定 dtype，避免 pandas 自動猜錯
# dtype_spec = {
#     'longitude': 'float64',
#     'latitude': 'float64',
#     # 你也可以加上其他欄位的型別，例如：
#     # 'siteid': 'int', 'aqi': 'float64'
# }


# # 讀檔，避免 dtype 警告
# df = pd.read_csv(r'D:\PowerBI進階\projects\airquality\air_quality.csv', 
#                     low_memory=False,
#                     dtype=dtype_spec)
df = pd.read_csv(r'D:\PowerBI進階\projects\airquality\air_quality.csv', 
                    low_memory=False)

# 檢查欄位
if 'date' not in df.columns:
    raise ValueError("資料中沒有 'date' 欄位")

# 將 date 欄轉成 datetime（處理混合格式）
df['date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce')

# 移除轉換失敗的日期
df = df.dropna(subset=['date'])

# 取年份
df['year'] = df['date'].dt.year

# 控制是否輸出欄位名稱
first = True

# 依年份分檔
for year, group in df.groupby('year'):
    filename = fr'D:\PowerBI進階\projects\airquality\air_quality_{year}.csv'
    group.to_csv(filename, index=False, header=first)
    print(f'✅ 已儲存：{filename}')
    first = False  # 後續檔案不寫入欄位名稱

