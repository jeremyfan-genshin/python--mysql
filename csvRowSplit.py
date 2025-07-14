import pandas as pd

# 讀取原始 CSV
df = pd.read_csv(r'D:\PowerBI進階\projects\airquality\air_quality_2016.csv', low_memory=False)

# 切分成三份
df_part1 = df.iloc[:1000]
df_part2 = df.iloc[1000:2000]
df_part3 = df.iloc[2000:]

# 儲存三個檔案，只有第一個含有欄位名稱（header）
df_part1.to_csv(r'D:\PowerBI進階\projects\airquality\air_quality_2016_part1.csv', index=False, header=True)
df_part2.to_csv(r'D:\PowerBI進階\projects\airquality\air_quality_2016_part2.csv', index=False, header=False)
df_part3.to_csv(r'D:\PowerBI進階\projects\airquality\air_quality_2016_part3.csv', index=False, header=False)

print("✅ 完成切分並儲存為三個檔案")
