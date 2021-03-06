import random
import schedule
import datetime
from pick import pick

from connection import Connection

SERVER = "WIN-Q0KFG511D92"
DB_CURRENT = ["Fashion4You"]
DB_PUMP = ["Fashion4You Current"]
INS_TIME = 3  # Time (in seconds) between inserts in pump mode


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


def insert_personal(
    nachname,
    vorname,
    position,
    anrede,
    geburtsdatum,
    einstellung,
    telefon,
    durchwahl,
    bemerkung,
    vorgesetzter,
):
    personal_nr = get_max_value(conn, "PersonalNr", "Personal")
    query = """INSERT INTO Personal
    VALUES ({personal_nr}, {nachname}, {vorname}, {position}, {andrede},
        {geburtsdatum}, {einstellung}, {telefon}, {durchwahl}, {bemerkung},
        {vorgesetzter})""".format(
        personal_nr=personal_nr,
        nachname=nachname,
        vorname=vorname,
        position=position,
        anrede=anrede,
        geburtsdatum=geburtsdatum,
        einstellung=einstellung,
        telefon=telefon,
        durchwahl=durchwahl,
        bemerkung=bemerkung,
        vorgesetzter=vorgesetzter,
    )
    conn.query(query)
    conn.conn.commit()


def insert_kunde(
    frima,
    kontaktname,
    strasse,
    regionnr,
    ort,
    bundesstaat,
    plz,
    laenderkennz,
    land,
    telefon,
    fax,
):
    kunden_nr = get_max_value(conn, "KundenNr", "Kunden")
    query = """INSERT INTO Kunden
    VALUES ({kunden_nr}, {firma}, {kontaktname}, {strasse}, {regionnr}, {ort},
    {bundesstaat}, {plz}, {laenderkennz}, {land}, {telefon}, {fax},
    {land})""".format(
        kunden_nr=kunden_nr,
        firma=frima,
        kontaktname=kontaktname,
        strasse=strasse,
        regionnr=regionnr,
        ort=ort,
        bundesstaat=bundesstaat,
        plz=plz,
        laenderkennz=laenderkennz,
        land=land,
        telefon=telefon,
        fax=fax,
    )
    conn.query(query)
    conn.conn.commit()


def insert_spediteur(spediteur):
    spediteur_nr = get_max_value(conn, "SpediteurNr", "Spediteure")
    query = """INSERT INTO Spediteure
    VALUES ({spediteur_nr}, {spediteur})""".format(
        spediteur_nr=spediteur_nr, spediteur=spediteur
    )
    conn.query(query)
    conn.conn.commit()


def insert_bestellung(conn, timestamp="SYSDATETIME()"):
    timestamp = timestamp
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
        lieferdatum="DATEADD(day, %d, SYSDATETIME())" % int(random.uniform(1, 14)),
    )
    conn.query(sql_bestellung)
    conn.query(sql_bestelldaten)
    conn.query(sql_lieferung)
    conn.conn.commit()
    print(sql_bestellung)
    print("Inserted Bestellung")


def update(conn):
    while get_max_value(conn, "Bestelldatum", "Bestellungen") < datetime.datetime.now():
        max_date = get_max_value(conn, "Bestelldatum", "Bestellungen")
        timestamp = "DATEADD(minute, {interval}, (SELECT MAX(Bestelldatum) FROM Bestellungen))".format(
            interval=random.uniform(60.0, 240.0)
        )
        insert_bestellung(conn, timestamp)


if __name__ == "__main__":
    try:
        frage_modus = "Welcher Modus?"
        option_modus = ["Pumpe", "Update"]
        modus, _ = pick(option_modus, frage_modus)

        frage_datenbank = "Welche Datenbank?"

        if modus == "Update":
            datenbank, _ = pick(DB_CURRENT, frage_datenbank)
            conn = Connection(SERVER, datenbank)
            conn.open_connection()
            update(conn)
            conn.close_connection()
        elif modus == "Pumpe":
            print("Start of script")
            datenbank, _ = pick(DB_PUMP, frage_datenbank)
            conn = Connection(SERVER, datenbank)
            conn.open_connection()
            schedule.every(INS_TIME).seconds.do(insert_bestellung, conn)
            while True:
                schedule.run_pending()
                time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        schedule.clear()
        print("Keyboard Interrupt")
        print("End of script")
