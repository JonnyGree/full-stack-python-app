import sqlite3
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory='templates')

@app.get("/")
def index(request: Request):
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row
    
    cursor = connection.cursor()
    cursor.execute("""SELECT id, symbol, name FROM stock ORDER BY symbol""")
    
    rows = cursor.fetchall()
    
    return templates.TemplateResponse("index.html", { "request": request, "stocks": rows})

@app.get("/stock/{symbol}")
def index(request: Request, symbol):
    connection = sqlite3.connect('app.db')
    connection.row_factory = sqlite3.Row
    
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM stock WHERE symbol = ?""", (symbol,))
    
    row = cursor.fetchone()
    print(row['id'])
    
    cursor.execute("""
                    SELECT strftime('%Y-%m-%d', date) as Date, Open, High, Low, Close, Volume 
                    FROM stock_price 
                    WHERE stock_id = ? 
                    ORDER BY Date DESC
                   """, (row['id'], ))
    prices = cursor.fetchall()
    
    print(prices[0]['high'])
    return templates.TemplateResponse("stock_detail.html", { "request": request, "stock": row, "bars": prices})