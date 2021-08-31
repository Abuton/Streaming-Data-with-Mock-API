import requests
from add_data import DBConnect

conn, cur = DBConnect(dbName='streamingDB')
cur.execute("USE streamingDB;")

with requests.get("http://127.0.0.1:5000/large_datastore/10000", stream=1) as data:

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