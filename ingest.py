import requests
# import psycopg2
from add_data import createTables, createDB, DBConnect, db_execute_fetch


createDB(dbName='strem_test')
createTables(dbName='strem_test')

conn, cur = DBConnect()


with requests.get("http://127.0.0.1:5000/large_datastore/5", stream=1) as data:

    # conn = psycopg2.connect(dbname="stream_test", user='postgres', password='postgres')

    # cur = conn.cursor()
    sql = "INSERT INTO transactions (txid, uid, amount) VALUES (%s, %s, %s)"

    buffer = ""
    for chuck in data.iter_content(chunk_size=1):
        if chuck.endswith(b"\n"):
            d = str(buffer)
            print(d)
            cur.execute("USE stream_test")
            cur.execute(sql, (d[0], d[1], d[2]))
            conn.commit()
            buffer = ""
        else:
            buffer += chuck.decode()

df = db_execute_fetch(tablename='transaction')
print(df.head())