import csv
import os
import mysql.connector

# 資料庫連線設定
db_config = {
    'host': '127.0.0.1',
    'user': 'dbuser2',
    'password': '1234',
    'database': 'testdb',
    'autocommit': True
}

# 模擬爬取一筆 user 資料
def fetch_user_data():
    from faker import Faker
    fake = Faker()
    return {
        'name': fake.name(),
        'email': fake.email()
    }

# 寫入一筆資料到 user.csv
def cache_user_to_csv(user_data, csv_file='user.csv'):
    file_exists = os.path.isfile(csv_file)
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'email'])
        if not file_exists:
            writer.writeheader()
        writer.writerow(user_data)

# 讀取 CSV 並寫入資料庫
def write_csv_to_db(csv_file='user.csv'):
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    sql = "INSERT INTO users2 (name, email) VALUES (%s, %s)"
    data = [(row['name'], row['email']) for row in rows]

    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    # 清空 CSV 檔
    open(csv_file, 'w').close()

# 主程序（模擬爬 1 筆後做處理）
def main():
    user_data = fetch_user_data()
    cache_user_to_csv(user_data)

    # 檢查是否滿 10 筆
    with open('user.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
        if len(lines) > 10:  # 含標題列
            write_csv_to_db()

if __name__ == '__main__':
    main()
