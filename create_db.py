import sqlite3

# Your provided connection
connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock (
    id iNTEGER PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stock_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        FOREIGN KEY(stock_id) REFERENCES stock(id),
        UNIQUE(stock_id, date)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_data (
        timestamp TEXT NOT NULL,
        close REAL,
        high REAL,
        low REAL,
        trade_count INTEGER,
        open REAL,
        volume INTEGER,
        vwap REAL,
        symbol TEXT NOT NULL,
        FOREIGN KEY(symbol) REFERENCES stock(symbol),
        PRIMARY KEY (timestamp, symbol)
    )
""")

connection.commit()
