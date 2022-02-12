# PythonSQL

Skript um mit der Beispieldatenbank "Fashion For You" eine Verbindung aufzubauen und SQL Queries auszuführen.

## Ausführung

Es müssen zuerst die Dependencies installiert werden mit `pip install -r reuqirements.txt`

Dabei wird pyodbc verwendet, welches Microsoft Visual C++ Dependencies enthält. Sind diese nicht installiert, können sie [hier](https://docs.microsoft.com/en-US/cpp/windows/latest-supported-vc-redist?view=msvc-170) runtergeladen werden.

Im Anschluss kann das Skript mit `python __main__.py` ausgeführt werden.

## Anpassungen in `__main__.py`

In Zeile 6 kann/muss `SERVER` auf den Servername gesetzt werden und in Zeile 7 `DB` auf den Datenbankname gesetzt werden.

Standardmäßig wird dann alle 60 Sekunden eine Bestellung hinzugefügt und alle relevanten Tabellen ergänzt. Um die Frequenz zu ändern, kann in Zeile 110 ver
