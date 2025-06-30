import mysql.connector

connect = mysql.connector.connect(
    host="127.0.0.1",
    user="dbuser2",
    password="1234",
    autocommit=True,  # 自動提交事務
    # database="house2",
    port=3306,  # 如果需要指定端口
    charset='utf8mb4',  # 如果需要指定字符集
    # connection_timeout=1  # 如果需要指定連接超時時間

    # pool_name="mypool", # 連接池名稱
    # pool_size=5,  # 連接池大小
    # pool_reset_session=True  # 每次從連接池獲取連接時重置會話
    # ssl_ca='path/to/ca.pem',  # 如果需要SSL連接
    # ssl_cert='path/to/client-cert.pem',  # 如果需要SSL連接
    # ssl_key='path/to/client-key.pem'  # 如果需要SSL連接
)

# 建立資料庫
cursor = connect.cursor()
cursor.execute("create database if not exists house2")
cursor.execute("use house2")

# 建立資料表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS `users2` (
    `id` int NOT NULL AUTO_INCREMENT,
    `name` varchar(100) DEFAULT NULL,
    `email` varchar(100) DEFAULT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;               
''')

# 寫入一筆資料
sql = "INSERT INTO users2 (name, email) VALUES (%s, %s)"
val = ("王小明", "xiaoming@example.com")
cursor.execute(sql, val)
connect.commit()  # 提交事務
    


cursor.execute("SELECT * FROM users2")
results = cursor.fetchall()
for row in results:
    print(row)
cursor.close()
connect.close()


    