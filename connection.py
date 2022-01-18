import pyodbc

class Connection:

    def __init__(self, server, db):
        self.server = server
        self.db = db

    def open_connection(self):
        self.conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                          "Server=" + self.server + ";"
                          "Database=" + self.db + ";"
                          "Trusted_Connection=yes;")
    def query(self, query):
        return self.conn.execute(query)

    def close_connection(self):
        if self.conn:
            self.conn.close()
