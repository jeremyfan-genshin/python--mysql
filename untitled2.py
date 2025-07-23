from geopy.geocoders import Nominatim
import time

geolocator = Nominatim(user_agent="my_geocoder_app")  # 用英文命名

address = "台中市太平區樹孝路 2號"
location = geolocator.geocode(address)

if location:
    print(f"地址：{address}")
    print(f"緯度：{location.latitude}")
    print(f"經度：{location.longitude}")
else:
    print("查無此地址")
