import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time

def add_lat_lng_from_address(parquet_path, address_col, save_path=None):
    # 讀取 parquet
    df = pd.read_parquet(parquet_path)
    if address_col not in df.columns:
        print(f"❌ 找不到欄位: {address_col}")
        return

    # 設定 geolocator（使用 Nominatim）
    geolocator = Nominatim(user_agent="tw-address-locator")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # 建立新欄位
    lats = []
    lngs = []

    for i, addr in enumerate(df[address_col]):
        if pd.isna(addr):
            lats.append(None)
            lngs.append(None)
            continue
        try:
            location = geocode(addr)
            if location:
                lats.append(location.latitude)
                lngs.append(location.longitude)
                print(f"latitude:{location.latitude}")
                print(f"latitude:{location.longitude}")
            else:
                lats.append(None)
                lngs.append(None)
        except Exception as e:
            print(f"⚠️ 第{i}筆地址查詢失敗：{addr}，錯誤：{e}")
            lats.append(None)
            lngs.append(None)
        time.sleep(1)  # 防止過度查詢

    # 寫入新欄位
    df['latitude'] = lats
    df['longitude'] = lngs

    # 儲存
    if save_path is None:
        save_path = parquet_path.replace(".parquet", "_with_latlng.parquet")

    df.to_parquet(save_path, index=False)
    print(f"✅ 儲存含經緯度檔案至：{save_path}")
    return df



add_lat_lng_from_address(r"e:\jeremyOnly\projects\python--mysql\python--mysql\house\data_csv\113q1_fixed.parquet", address_col="address")
