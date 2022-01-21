import sched, time, random, datetime
from datetime import date

from connection import Connection

SERVER = "WIN-Q0KFG511D92"
DB = "Fashion4You"
s = sched.scheduler(time.time, time.sleep)


def get_connection():
    return Connection(SERVER, DB)


def execute_sql(sql, connection):
    result = connection.query(sql)
    conn.conn.commit()
    if result:
        print(result)
    s.enter(
        5,
        1,
        execute_sql,
        (
            sql,
            connection,
        ),
    )


def get_max_value(connection, row, table):
    return connection.query(
        """
        SELECT MAX({row})
        FROM {table};
        """.format(
            row=row, table=table
        )
    ).fetchval()


def get_random_value(connection, row, table):
    return connection.query(
        """
    SELECT TOP 1 {row}
    FROM {table}
    ORDER BY NEWID();
    """.format(
            row=row, table=table
        )
    ).fetchval()


def get_value(connection, row, table, condition):
    return connection.query(
        """
    SELECT MAX({row})
    FROM {table}
    WHERE {condition};
    """.format(
            row=row, table=table, condition=condition
        )
    ).fetchval()


def insert_bestellung(conn):
    timestamp = date.today()
    bestell_nr = int(get_max_value(conn, "BestellungNr", "Bestellungen") + 1)
    kunden_nr = get_random_value(conn, "KundenNr", "Kunden")
    personal_nr = get_random_value(conn, "PersonalNr", "Personal")
    spediteur_nr = get_random_value(conn, "SpediteurNr", "Spediteure")
    fracht = random.uniform(1.0, 100.0)

    sql_bestellung = """
        INSERT INTO Bestellungen
        VALUES ({bestell_nr}, {timestamp}, {kunden_nr}, {personal_nr}, {spediteur_nr}, {fracht});
        """.format(
        timestamp=timestamp,
        bestell_nr=bestell_nr,
        kunden_nr=kunden_nr,
        personal_nr=personal_nr,
        spediteur_nr=spediteur_nr,
        fracht=fracht,
    )

    produkt_nr = get_random_value(conn, "ProduktNr", "Produkte")
    menge = int(random.uniform(0, 100))
    sql_bestelldaten = """
        INSERT INTO Bestelldaten
        VALUES ({bestell_nr}, {beste_zeile_nr}, {produkt_nr}, {menge}, {preis}, {rabatt});
        """.format(
        bestell_nr=bestell_nr,
        beste_zeile_nr=1,
        produkt_nr=produkt_nr,
        menge=menge,
        preis=get_value(
            conn, "Listenspreis", "Produkte", "ProduktNr=" + str(produkt_nr)
        )
        * menge,
        rabatt=round(random.uniform(0.0, 1.0), 2),
    )

    sql_lieferung = """
        INSERT INTO Lieferungen
        VALUES ({bestell_nr}, {beste_zeile_nr}, {spediteur_nr}, {kunden_nr}, {produkt_nr}, {mitarbeiter_nr}, {lieferdatum});
    """.format(
        bestell_nr=bestell_nr,
        beste_zeile_nr=1,
        spediteur_nr=spediteur_nr,
        kunden_nr=kunden_nr,
        produkt_nr=produkt_nr,
        mitarbeiter_nr=personal_nr,
        lieferdatum=timestamp + datetime.timedelta(int(random.uniform(1, 14))),
    )
    print(sql_bestellung)
    print(sql_bestelldaten)
    print(sql_lieferung)
    conn.query(sql_bestellung)
    conn.query(sql_bestelldaten)
    conn.query(sql_lieferung)
    conn.conn.commit()


if __name__ == "__main__":
    conn = get_connection()
    print("date ", date.today())
    conn.open_connection()

    insert_bestellung(conn)
    print()
    # conn.conn.commit()
    # conn.close_connection()
    # s.enter(
    #     5,
    #     1,
    #     execute_sql,
    #     (
    #         sql,
    #         conn,
    #     ),
    # )
    s.run()
