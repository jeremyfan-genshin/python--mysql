import pandas as pd
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# 老舊地名轉換對照表
OLD_TO_NEW_CITY = {
    '台北縣': '新北市',
    '台中縣': '臺中市',
    '台南縣': '臺南市',
    '高雄縣': '高雄市',
    '台北市': '臺北市',
    '台中市': '臺中市',
    '台南市': '臺南市',
}

def normalize_address(addr):
    addr = addr.strip()

    # 替換老舊縣市名稱
    for old, new in OLD_TO_NEW_CITY.items():
        addr = addr.replace(old, new)

    # 全形數字轉半形
    addr = addr.translate(str.maketrans('０１２３４５６７８９', '0123456789'))

    # 移除不該有的空白（但先不處理號前空白）
    addr = re.sub(r'(\d+)\s+(?=[巷弄樓])', r'\1', addr)     # 巷/弄/樓前不能留空白
    addr = re.sub(r'([段路街道])\s+', r'\1', addr)           # 段/路/街/道後不能留空白

    # 先修復可能被錯誤斷開的「門牌號」數字（如 2 1號 → 21號）
    addr = re.sub(r'(\d)\s+(\d+號)', r'\1\2', addr)

    # 確保門牌號前有空白（例：496號 → 八段 496號）
    addr = re.sub(r'(?<=[^\s])(\d+號)', r' \1', addr)

    return addr




if __name__ == '__main__':

    df = pd.DataFrame({
        '地址': [
            '台中市沙鹿區中清路八段 496號',
            '台中市沙鹿區福成路130巷 21號',
            '台中縣霧峰區新生路 7號',
            '台北縣板橋區文化路一段 188號',
            '台南市東區東門路一段 2號'
        ]
    })

    # 正規化地址
    df['地址'] = df['地址'].apply(normalize_address)

    # 初始化地理編碼器
    geolocator = Nominatim(user_agent="my_geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    # 地理編碼
    df['location'] = df['地址'].apply(geocode)
    df['latitude'] = df['location'].apply(lambda loc: loc.latitude if loc else None)
    df['longitude'] = df['location'].apply(lambda loc: loc.longitude if loc else None)
    df.drop(columns=['location'], inplace=True)

    print(df)
