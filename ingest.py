import requests
import ast
# import psycopg2
from add_data import DBConnect

conn, cur = DBConnect()
cur.execute("USE stream_test;")

with requests.get("http://127.0.0.1:5000/large_datastore/100", stream=1) as data:

    # conn = psycopg2.connect(dbname="stream_test", user='postgres', password='postgres')

    # cur = conn.cursor()
    sql = "INSERT INTO transaction (txid, uid, amount) VALUES (%s, %s, %s)"

    buffer = ""
    for chuck in data.iter_content(chunk_size=1):
        if chuck.endswith(b"\n"):
            d = str(buffer)
            print(d)
            
            cur.execute(sql, (d.split(',')[0].strip('('), d.split(',')[1], d.split(',')[2].strip(')')))
            conn.commit()
            buffer = ""
        else:
            buffer += chuck.decode()

# df = db_execute_fetch(tablename='transaction')
# print(df.head())