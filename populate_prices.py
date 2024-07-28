from dotenv import load_dotenv
import os
import sqlite3
import alpaca_trade_api as tradeapi
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import time
import sys

# ----> Input correct year <------
year=2020
print("year: ", year)
start_date = datetime(year, 1, 1)
if year == datetime.today().year:
    end_date = datetime.today()
else:
    end_date = datetime(year, 12, 31)

# Load environment variables from the .env file
load_dotenv()

connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""SELECT id, symbol, name FROM stock""")
rows = cursor.fetchall()

# Create lists for cleaned symbols and removed symbols
cleaned_symbols = [row for row in rows if '/' not in row['symbol']]
removed_symbols = [row['symbol'] for row in rows if '/' in row['symbol']]

#removed not stock simbol, like BTC/USD
print("Removed Symbols:", removed_symbols)

symbols = []
stock_dict = {}
for row in cleaned_symbols:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']
api = tradeapi.REST(os.getenv('ALPACA_KEY'), os.getenv('ALPACA_SECRET'), base_url='https://paper-api.alpaca.markets', api_version='v2')

#for debug
#sys.exit()
timeframe = tradeapi.TimeFrame.Day
start_date_tz = pd.Timestamp(start_date, tz='America/New_York').isoformat()
end_date_tz = pd.Timestamp(end_date, tz='America/New_York').isoformat()
chunk_size = 200

i = 0
while i < len(symbols):
    symbol_chunk = symbols[i:i+chunk_size]
    try:
        bars = api.get_bars(symbol_chunk, timeframe, start=start_date_tz, end=end_date_tz)
        records_to_insert = []
        for bar in bars:
            records_to_insert.append((stock_dict[bar.S], bar.t.date(), float(bar.o), float(bar.h), float(bar.l), float(bar.c), int(bar.v) ))

        cursor.executemany("""
            INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_id, date) DO NOTHING
            """, records_to_insert)

        connection.commit()
        print(f"Inserting records from {i} to {i + chunk_size}")
        i += chunk_size
    except tradeapi.rest.APIError as e:
        print(f"Error fetching bars for symbols {symbol_chunk}: {e}")
        # Identify and remove the invalid symbol
        invalid_symbol = str(e).split(':')[-1].strip()
        if invalid_symbol in symbols:
            print(f"Removing invalid symbol: {invalid_symbol}")
            symbols.remove(invalid_symbol)
        else:
            i += chunk_size
    time.sleep(4)

