# ================================
# Database: Crypto Market Pipeline
# ================================

CREATE DATABASE IF NOT EXISTS crypto_market;
USE crypto_market;

# ================================
# Dimension Table: Coin Details
# ================================
CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id VARCHAR(50) PRIMARY KEY,       # CoinGecko coin_id (bitcoin, ethereum, etc.)
    symbol VARCHAR(20) NOT NULL,            # Same as coin_id for simplicity
    name VARCHAR(100) NOT NULL,
    date_added DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

# ================================
# Fact Table: Crypto Prices
# ================================
CREATE TABLE IF NOT EXISTS fact_crypto_prices (
    price_id INT AUTO_INCREMENT PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    current_price DECIMAL(18,8),
    market_cap BIGINT,
    volume_24h BIGINT,
    price_change_pct DECIMAL(10,4),
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_coin
        FOREIGN KEY (coin_id)
        REFERENCES dim_coin(coin_id)
        ON DELETE CASCADE
);