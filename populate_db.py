from dotenv import load_dotenv
import os
import sqlite3
import alpaca_trade_api as tradeapi

# Load environment variables from the .env file
load_dotenv()

connection = sqlite3.connect('app.db')

cursor = connection.cursor()
cursor.execute("""SELECT symbol, name FROM stock""")
rows = cursor.fetchall()
symbols = [row[0] for row in rows]

api = tradeapi.REST(os.getenv('ALPACA_KEY'), os.getenv('ALPACA_SECRET'), base_url='https://paper-api.alpaca.markets')
assets = api.list_assets()
for asset in assets:
    try:
        if asset.status == 'active' and asset.tradable and asset.symbol not in symbols:
            print(f"added a new stock {asset.symbol}  {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, name, exchange) VALUES (?, ?, ?)",(asset.symbol, asset.name, asset.exchange))
    except Exception as e:
        print(asset.symbol)
        print(e)

connection.commit()
