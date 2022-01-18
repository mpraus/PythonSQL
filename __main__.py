import sched, time

from connection import Connection

SERVER = 'WIN-Q0KFG511D92'
DB = 'Fashion4You'
s = sched.scheduler(time.time, time.sleep)

def get_connection():
    return Connection(SERVER, DB)

def execute_sql(sql, connection):
    result = connection.query(sql)
    conn.conn.commit()
    if result:
        print(result)
    s.enter(5, 1, execute_sql, (sql, connection,))

if __name__ == '__main__':
    conn = get_connection()
    conn.open_connection()
    sql = """
        INSERT INTO Bestellungen
        VALUES (2, %timestamp, 2, 2, 1, 1);
        """.format(timestamp=time.ctime())
    conn.query(sql)
    # conn.conn.commit()
    # conn.close_connection()
    s.enter(5, 1, execute_sql, (sql, conn,))
    s.run()
