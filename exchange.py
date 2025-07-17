from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
import random
from datetime import datetime

app = Flask(__name__)
CORS(app)


open_prices = {}
last_prices = {}
initial_volume = {}

def get_companies_from_db():
    conn = psycopg2.connect(
        dbname="airflow", user="airflow", password="airflow", host="postgres", port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT id, symbol, name FROM companies;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def simulate_market_data(companies):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    result = []

    for company_id, symbol, name in companies:
        if symbol not in open_prices:
            start_price = round(random.uniform(10, 500), 2)
            open_prices[symbol] = start_price
            last_prices[symbol] = start_price  
            initial_volume[symbol] = random.randint(10000, 5000000)
            
        last_price = last_prices[symbol]
        pct_change = random.uniform(-0.001, 0.001)  
        current_price = round(last_price * (1 + pct_change), 2)
        last_prices[symbol] = current_price  


        open_price = open_prices[symbol]
        pct_change_display = round((current_price - open_price) / open_price * 100, 2)

        volume = round(initial_volume[symbol] * (1 + random.uniform(-0.003, 0.003)), 2)

        result.append({
            "id": company_id,
            "symbol": symbol,
            "name": name,
            "open": open_price,
            "current_price": current_price,
            "percent_change": pct_change_display,
            "volume": volume,
            "timestamp": timestamp
        })

    return result

@app.route('/api/market')
def market_data():
    companies = get_companies_from_db()
    data = simulate_market_data(companies)
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
