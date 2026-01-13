import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# ===============================
# Database Connection
# ===============================
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="root",
    port=3306,
    database="crypto_market"
)

# ===============================
# Fetch data
# ===============================
query = """
SELECT 
    d.name,
    f.current_price,
    f.market_cap,
    f.volume_24h,
    f.price_change_pct
FROM fact_crypto_prices f
JOIN dim_coin d ON f.coin_id = d.coin_id
"""

df = pd.read_sql(query, conn)
conn.close()

# ===============================
# Handle NULL values
# ===============================
df = df.fillna(0)

# ===============================
# Create ONE FRAME with GOOD SIZE
# ===============================
fig = plt.figure(figsize=(22, 14))  # 👈 Big frame

# ---------- Graph 1 ----------
ax1 = fig.add_subplot(2, 2, 1)
ax1.bar(df["name"], df["current_price"])
ax1.set_title("Current Price")
ax1.tick_params(axis='x', rotation=90, labelsize=8)

# ---------- Graph 2 ----------
ax2 = fig.add_subplot(2, 2, 2)
ax2.bar(df["name"], df["market_cap"])
ax2.set_title("Market Capitalization")
ax2.tick_params(axis='x', rotation=90, labelsize=8)

# ---------- Graph 3 ----------
ax3 = fig.add_subplot(2, 2, 3)
ax3.bar(df["name"], df["volume_24h"])
ax3.set_title("24h Trading Volume")
ax3.tick_params(axis='x', rotation=90, labelsize=8)

# ---------- Graph 4 ----------
ax4 = fig.add_subplot(2, 2, 4)
ax4.bar(df["name"], df["price_change_pct"])
ax4.set_title("Price Change Percentage")
ax4.tick_params(axis='x', rotation=90, labelsize=8)

# ===============================
# Adjust spacing (KEY FIX)
# ===============================
plt.subplots_adjust(
    left=0.05,
    right=0.98,
    top=0.95,
    bottom=0.25,   # 👈 space for labels
    hspace=0.35,   # 👈 vertical spacing
    wspace=0.25    # 👈 horizontal spacing
)

plt.show()