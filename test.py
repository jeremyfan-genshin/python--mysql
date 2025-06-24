import mysql.connector

#
print("建立資料庫連線中")
mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="dbuser",
    password="1234"

)
print("透過連線取得 cursor物件")