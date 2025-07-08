import csv
import os
import mysql.connector
from mysql.connector import Error

# MySQL 資料庫連線設定

db_config = {
    "host":"127.0.0.1",
    "user":"dbuser2",
    "password":"1234",
    "autocommit":True,  # 自動提交事務
    "port":3306,  # 如果需要指定端口
    "charset":'utf8mb4',  # 如果需要指定字符集
    "database":"housing"
}

# CSV 設定
csv_file = "housing_data.csv"
buffer = []
BUFFER_LIMIT = 100

# 建立表格（若不存在）
def init_db():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS housing (
                id INT AUTO_INCREMENT PRIMARY KEY,
                address VARCHAR(255),
                price INT,
                area FLOAT
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
    except Error as e:
        print("❌ 初始化資料庫失敗：", e)

# 寫入 CSV（單筆）
def write_to_csv(data, file_path):
    file_exists = os.path.isfile(file_path)
    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["address", "price", "area"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

# 批次寫入 MySQL（交易控制）
# def write_to_mysql_transaction(data_batch):
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor()
#         conn.start_transaction()
#         for item in data_batch:
#             cursor.execute(
#                 "INSERT INTO housing (address, price, area) VALUES (%s, %s, %s)",
#                 (item["address"], item["price"], item["area"])
#             )
#         conn.commit()
#         print(f"✅ 成功寫入 {len(data_batch)} 筆資料")
#     except Error as e:
#         conn.rollback()
#         print("❌ 資料寫入失敗，已回滾")
#         print("錯誤：", e)
#     finally:
#         cursor.close()
#         conn.close()

def write_to_mysql_transaction(data_batch):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        conn.start_transaction()

        params = [(item["address"], item["price"], item["area"]) for item in data_batch]
        cursor.executemany(
            "INSERT INTO housing (address, price, area) VALUES (%s, %s, %s)",
            params
        )

        conn.commit()
        print(f"✅ 成功寫入 {len(data_batch)} 筆資料")
    except Error as e:
        conn.rollback()
        print("❌ 資料寫入失敗，已回滾")
        print("錯誤：", e)
    finally:
        cursor.close()
        conn.close()


# 新資料進來時的處理邏輯
def process_new_data(new_data):
    write_to_csv(new_data, csv_file)
    buffer.append(new_data)
    if len(buffer) >= BUFFER_LIMIT:
        write_to_mysql_transaction(buffer.copy())
        buffer.clear()

# if __name__ == '__main__':
# 初始化資料表
init_db()

# 模擬資料進來（可替換成實際爬蟲）
for i in range(250):  # 假設有 250 筆資料
    mock_data = {
        "address": f"台北市大安區仁愛路{i}號",
        "price": 1000 + i * 5,
        "area": 30 + (i % 10)
    }
    process_new_data(mock_data)
