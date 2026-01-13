import pymysql

connection = pymysql.connect(
    host='localhost',
    user='pavitra',
    password='root',
    port=3306
)

print("MySQL connection successful!✌️")
