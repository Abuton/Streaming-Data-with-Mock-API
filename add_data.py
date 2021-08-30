import pandas as pd
import mysql.connector as mysql

def DBConnect(dbName=None):
    """

    Parameters
    ----------
    dbName :
        Default value = None)

    Returns
    -------

    """
    conn = mysql.connect(host='localhost', user='root',
                         database=dbName, buffered=True)
    cur = conn.cursor()
    return conn, cur

def createDB(dbName:str)->None:
    """
    The function helps to create a mySQL database

    Parameters
    ----------
    dbName : the name of the database to be createdd
        str:

    Returns
    -------

    """
    conn, cur = DBConnect()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbName} CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_unicode_ci;")
    conn.commit()
    cur.close()

def createTables(dbName:str)->None:
    """

    Parameters
    ----------
    dbName : the name of the database to create the table into
        str:        

    Returns
    -------

    """
    conn, cur = DBConnect(dbName)
    sqlFile = 'transaction.sql'
    fd = open(sqlFile, 'r')
    readSqlFile = fd.read()
    fd.close()

    sqlCommands = readSqlFile.split(';')

    for command in sqlCommands:
        try:
            res = cur.execute(command)
        except Exception as ex:
            print("Command skipped: ", command)
            print(ex)
    conn.commit()
    cur.close()

    return 

def read_csv(filepath:str):
    df = pd.read_csv(filepath=filepath)
    return df

def insert_to_transaction_table(dbName:str, df:pd.DataFrame, table_name:str)->None:
    """

    Parameters
    ----------
    dbName :
        str:
    df :
        pd.DataFrame:
    table_name :
        str:        

    Returns
    -------

    """
    conn, cur = DBConnect(dbName)

    df = read_csv(df)

    for _, row in df.iterrows():
        sqlQuery = f"""INSERT INTO {table_name} (txid, uid, amount)
             VALUES( %s, %s, %s)"""
        data = (row[0], row[1], row[2],)

        try:
            # Execute the SQL command
            cur.execute(sqlQuery, data)
            # Commit your changes in the database
            conn.commit()
            print("Data Inserted Successfully")
        except Exception as e:
            conn.rollback()
            print("Error: ", e)
    return

def db_execute_fetch(*args, many=False, tablename='', rdf=True, **kwargs)->pd.DataFrame:
    """

    Parameters
    ----------
    *args :
        
    many :
         (Default value = False)
    tablename :
         (Default value = '')
    rdf :
         (Default value = True)
    **kwargs :
        

    Returns
    -------

    """
    connection, cursor1 = DBConnect(**kwargs)
    if many:
        cursor1.executemany(*args)
    else:
        cursor1.execute(*args)

    # get column names
    field_names = [i[0] for i in cursor1.description]

    # get column values
    res = cursor1.fetchall()

    # get row count and show info
    nrow = cursor1.rowcount
    if tablename:
        print(f"{nrow} recrods fetched from {tablename} table")

    cursor1.close()
    connection.close()

    # return result
    if rdf:
        return pd.DataFrame(res, columns=field_names)
    else:
        return res


if __name__ == "__main__":
    createDB(dbName='stream_test')
    createTables(dbName='stream_test')

#     df = pd.read_csv('data/*.csv')

#     insert_to_transaction_table(dbName='stream_test', df=df, table_name='transaction')
