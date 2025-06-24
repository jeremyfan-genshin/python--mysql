import mysql.connector

# cnx = mysql.connector.connect = (

cnx=mysql.connector.connect(    
    host="127.0.0.1",
    port="3306",
    user="dbuser",
    password="1234",
    database="prize"
)

dbcursor = cnx.cursor()

insertSQL = "insert into lottery(n1, n2, n3, n4, n5, n6) values(%s,%s,%s,%s,%s,%s)"
parserdata = (11,12,13,14,15,16)

# dbcursor.execute(insertSQL, parserdata)
# cnx.commit()

# dbcursor.close()
# cnx.close()



dbcursor.execute("select * from lottery")

for pid, n1, n2, n3, n4, n5, n6 in dbcursor:
    print(pid, n1, n2, n3, n4, n5, n6)




# import mysql.connector


# cnx=mysql.connector.connect(
#     host="127.0.0.1",
#     port=3306,
#     user="root",
#     password="1234",
#     database="prize"
# )

# dbcursor=cnx.cursor()

# insertSQL="insert into lottery (n1,n2,n3,n4,n5,n6) values (%s,%s,%s,%s,%s,%s)"

# parserdata=(1,2,4,8,10,13)
# dbcursor.execute(insertSQL, parserdata)
# print("完成交易 確定要寫入資料庫")
# cnx.commit()

# dbcursor.close()
# cnx.close()