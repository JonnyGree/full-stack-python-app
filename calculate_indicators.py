import sqlite3
from datetime import date
import numpy as np
import tulipy as ti
import matplotlib.pyplot as plt

connection = sqlite3.connect('app.db')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()


symbol = 'MSFT'
cursor.execute("""SELECT * FROM stock WHERE symbol = ?""", (symbol,))

row = cursor.fetchone()
print("stock id", row['id'])
    
cursor.execute("""
                SELECT strftime('%Y-%m-%d', date) as Date, Open, High, Low, Close, Volume 
                FROM stock_price 
                WHERE stock_id = ? 
                ORDER BY Date ASC
                """, (row['id'], ))
prices = cursor.fetchall()

recent_closes = [row['close'] for row in prices]
print(recent_closes[-1])

if len(prices)>=50:
    sma_20 = ti.sma(np.array(recent_closes), period=20)
    sma_50 = ti.sma(np.array(recent_closes), period=50)
    rsi_14 = ti.rsi(np.array(recent_closes), period=14)
    
print(sma_20[-1])
print(sma_50[-1])
print(rsi_14[-1])


