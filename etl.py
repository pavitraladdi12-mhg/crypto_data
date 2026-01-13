import pandas as pd
import requests
import pymysql
import time
from datetime import datetime

# =====================================
# Database Connection (YOUR STYLE)
# =====================================
connector = pymysql.connect(
    host='localhost',
    user='pavitra',
    password='root',
    port=3306
    
)

print(connector, "Connection Successful ✌🎉")

cursor = connector.cursor()
cursor.execute("USE crypto_market;")
print("✅ Using database: crypto_market")

# =====================================
# API URLs
# =====================================
MARKET_URL = "https://api.coingecko.com/api/v3/coins/markets"
HISTORY_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/history"

# =====================================
# Read CSV
# =====================================
df = pd.read_csv("data/crypto_watch_list.csv")

# =====================================
# Load Dimension Table
# =====================================
for _, row in df.iterrows():
    cursor.execute(
        """
        INSERT IGNORE INTO dim_coin (coin_id, symbol, name, date_added)
        VALUES (%s, %s, %s, %s)
        """,
        (row["Symbol"], row["Symbol"], row["Name"], row["Date_Added"])
    )

connector.commit()
print("✅ dim_coin loaded")

# =====================================
# Load Fact Table
# =====================================
for _, row in df.iterrows():
    coin_id = row["Symbol"]
    date_added = row["Date_Added"]

    print(f"🔄 Processing {coin_id}")

    # ---------- CURRENT MARKET DATA ----------
    response = requests.get(
        MARKET_URL,
        params={"vs_currency": "usd", "ids": coin_id}
    )

    # 🚨 Handle rate limit
    if response.status_code == 429:
        print("⏳ Rate limit hit. Sleeping 60 seconds...")
        time.sleep(60)
        continue

    try:
        market_data = response.json()
    except Exception:
        print(f"⚠ Invalid JSON for {coin_id}. Skipping.")
        continue

    if not isinstance(market_data, list) or len(market_data) == 0:
        print(f"⚠ No market data for {coin_id}")
        continue

    market = market_data[0]
    current_price = market.get("current_price")
    market_cap = market.get("market_cap")
    volume_24h = market.get("total_volume")

    # ---------- HISTORICAL PRICE ----------
    formatted_date = datetime.strptime(
        date_added, "%Y-%m-%d"
    ).strftime("%d-%m-%Y")

    history_response = requests.get(
        HISTORY_URL.format(coin_id=coin_id),
        params={"date": formatted_date}
    )

    if history_response.status_code == 429:
        print("⏳ Rate limit on history API. Sleeping 60 seconds...")
        time.sleep(60)
        continue

    try:
        history_data = history_response.json()
        old_price = history_data["market_data"]["current_price"]["usd"]
        price_change_pct = round(
            ((current_price - old_price) / old_price) * 100, 4
        )
    except Exception:
        print(f"⚠ No historical price for {coin_id}")
        price_change_pct = None

    # ---------- INSERT FACT ----------
    cursor.execute(
        """
        INSERT INTO fact_crypto_prices
        (coin_id, current_price, market_cap, volume_24h, price_change_pct)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (coin_id, current_price, market_cap, volume_24h, price_change_pct)
    )

    connector.commit()
    print(f"✅ Loaded {coin_id}")

    # ---------- SAFE DELAY ----------
    time.sleep(5)

cursor.close()
connector.close()
print("🎉 ETL Pipeline completed successfully")