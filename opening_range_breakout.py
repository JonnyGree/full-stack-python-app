from dotenv import load_dotenv
import os
import sqlite3
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta, timezone
import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.requests import StockBarsRequest
import pytz
import smtplib, ssl

# Create a secure SSL context
context = ssl.create_default_context()

# Load environment variables from the .env file
load_dotenv()

connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""
               SELECT id from strategy where name='opening_range_breakout' 
               """)

strategy_id = cursor.fetchone()['id']

cursor.execute("""
               SELECT symbol, name 
               FROM stock 
               JOIN stock_strategy on stock_strategy.stock_id = stock.id
               WHERE stock_strategy.strategy_id = ?
               """, (strategy_id,))
stocks = cursor.fetchall()

symbols = [stock['symbol'] for stock in stocks]
print(symbols)
    
api = tradeapi.REST(os.getenv('ALPACA_KEY'), os.getenv('ALPACA_SECRET'), base_url='https://paper-api.alpaca.markets', api_version='v2')

# Get the current time in UTC
end_time_utc = datetime.now(pytz.utc) - timedelta(minutes=16)
print(end_time_utc)

new_york_tz = pytz.timezone('America/New_York')
end_time_ny = end_time_utc.astimezone(new_york_tz)

# Define the start time as 9:30 AM Eastern Time today
start_time_ny = end_time_ny.replace(hour=9, minute=30, second=0, microsecond=0)
print("New York Time start:", start_time_ny)
print("New York Time end:", end_time_ny)

# Convert to ISO 8601 format
start_time_ny_iso = start_time_ny.isoformat()
end_time_ny_iso = end_time_ny.isoformat()

current_date = datetime.now(pytz.utc).date().strftime('%Y-%m-%d')
start_minute_bar = f"{current_date} 09:30:00-04:00" 
end_minute_bar = f"{current_date} 09:45:00-04:00"
orders = api.list_orders(status='all', after=f"{current_date}T13:30:00Z")
existing_order_symbols = [order.symbol for order in orders]
print(existing_order_symbols)

messages = []

for symbol in symbols:
    minute_bars = api.get_bars(symbol, '1Min', start=start_time_ny_iso, end=end_time_ny_iso).df

    # Convert the index of the DataFrame to the 'America/New_York' timezone
    minute_bars.index = minute_bars.index.tz_convert('America/New_York')
    
    # print(symbol)
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar) 
    opening_range_bars = minute_bars.loc[opening_range_mask]
    # print (opening_range_bars)
    opening_range_low = opening_range_bars['low'].min() 
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low
    
    # print(opening_range_low)
    # print(opening_range_high)
    # print(opening_range)
    
    
    after_opening_range_mask = minute_bars.index >= end_minute_bar 
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]
    # print(after_opening_range_bars)
    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars['close'] >opening_range_high]
    
    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:
            limit_price = after_opening_range_breakout.iloc[0]['close']
            
            messages.append(f"placing order for symbol {symbol} at {limit_price}, limit {round(limit_price+opening_range, 2)}, stop {round(limit_price-opening_range, 2)}\n\n{after_opening_range_breakout.iloc[0]}\n\n")
            print(f"placing order for symbol {symbol} at {limit_price}, close_abhove {opening_range_high} at {after_opening_range_breakout.iloc[0]}")
            
            
        #     try:
        #         order = api.submit_order(
        #             symbol=symbol,
        #             side='buy',
        #             type='limit',
        #             qty='10',
        #             time_in_force='day',
        #             order_class='bracket',
        #             limit_price=limit_price,
        #             take_profit=dict(
        #                 limit_price=round(limit_price, 2) +  round(opening_range, 2),
        #             ),
        #             stop_loss=dict(
        #                 stop_price=round(limit_price, 2) -  round(opening_range, 2),
        #             )
        #         )
        #         print("Order submitted:", order)
        #     except tradeapi.rest.APIError as e:
        #         print("API error:", e)
        
        # else:
        #     print(f"already an order for {symbol}, skipping")
        
print(messages)

EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

with smtplib.SMTP('smtp.gmail.com:587') as server:
    try:
        server.ehlo()
        server.starttls()
        server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        
        email_message=f"Subject: Trade Notifications for {current_date}\n\n"
        email_message += "\n\n".join(messages)
        
        server.sendmail(str(os.getenv('EMAIL_ADDRESS')),str(os.getenv('EMAIL_ADDRESS')),email_message)         
        server.close()
        print('successfully sent the mail')
    except:
        print("failed to send mail")


