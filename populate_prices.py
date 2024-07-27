from dotenv import load_dotenv
import os
import sqlite3
import alpaca_trade_api as tradeapi
import pandas as pd
from datetime import datetime
import time

# Load environment variables from the .env file
load_dotenv()

connection = sqlite3.connect('app.db')

# Set row factory to sqlite3.Row to access columns by name
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""SELECT id, symbol, name FROM stock""")
rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']
api = tradeapi.REST(os.getenv('ALPACA_KEY'), os.getenv('ALPACA_SECRET'), base_url='https://paper-api.alpaca.markets', api_version='v2')

timeframe = tradeapi.TimeFrame.Day
start_date = pd.Timestamp('2022-11-01', tz='America/New_York').isoformat()
end_date = pd.Timestamp('2022-12-01', tz='America/New_York').isoformat()
chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]
    bars = api.get_bars(symbol_chunk, timeframe, start=start_date, end=end_date).df
    
    records_to_insert = []
    for idx, row in bars.iterrows():
        #cursor.execute("SELECT id FROM stock WHERE symbol = ?", (row['symbol'],))
        #stock_id = cursor.fetchone()[0]
        stock_id = stock_dict[row['symbol']]
        records_to_insert.append((stock_id, idx.strftime('%Y-%m-%d %H:%M:%S'), row['open'], row['high'], row['low'], row['close'], row['volume']))

    # Bulk insert into stock_price
    cursor.executemany("""
        INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_id, date) DO NOTHING
        """, records_to_insert)

    # Commit the changes to the stock_price table
    connection.commit()
    print(f"Inserting records from {i} to {i + chunk_size}")
    time.sleep(3)

