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
        date DATE NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
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

cursor.execute("""
                CREATE TABLE IF NOT EXISTS strategy(
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )
                """)

cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_strategy(
                    stock_id INTEGER NOT NULL,
                    strategy_id INTEGER NOT NULL,
                    FOREIGN KEY(stock_id) REFERENCES stock (id),
                    FOREIGN KEY(strategy_id) REFERENCES strategy (id),
                    PRIMARY KEY (stock_id, strategy_id)
                )
                """)

# Insert strategies if they do not already exist
strategies = ['opening_range_breakout', 'opening_range_breakdown']

cursor.executemany("""
                    INSERT OR IGNORE INTO strategy (name) VALUES (?)
                    """, [(strategy,) for strategy in strategies])

connection.commit()

# Query to join stock and stock_strategy on id, selecting where strategy_id = 1
cursor.execute("""
    SELECT stock.*
    FROM stock
    JOIN stock_strategy ON stock.id = stock_strategy.stock_id
    WHERE stock_strategy.strategy_id = 1
""")

    
connection.commit()
