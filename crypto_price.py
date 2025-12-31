import requests
import logging
import sqlite3
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Dictionary mapping crypto symbols to their IDs in CoinGecko API
CRYPTO_ID_MAP = {
    'BTC': 'bitcoin',
    'ETH': 'ethereum',
    'LTC': 'litecoin',
    'XMR': 'monero',
    'DASH': 'dash',
    'BCH': 'bitcoin-cash',
    'ZEC': 'zcash',
    # Add more cryptocurrencies as needed
}

def init_crypto_prices_table():
    """Initialize the crypto_prices table in escrow_bot.db if it doesn't exist."""
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crypto_prices (
                crypto_type TEXT PRIMARY KEY,
                price REAL NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error in init_crypto_prices_table: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def save_price_to_db(crypto_type, price):
    """Save the price of a cryptocurrency to the database."""
    crypto_type = crypto_type.upper()
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO crypto_prices (crypto_type, price, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (crypto_type, price))
        conn.commit()
        logger.info(f"Saved {crypto_type} price: ${price} to database")
    except sqlite3.Error as e:
        logger.error(f"Database error in save_price_to_db: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def get_price_from_db(crypto_type):
    """Retrieve the last known price of a cryptocurrency from the database."""
    crypto_type = crypto_type.upper()
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT price FROM crypto_prices WHERE crypto_type = ?
        ''', (crypto_type,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        return None
    except sqlite3.Error as e:
        logger.error(f"Database error in get_price_from_db: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_crypto_price(crypto_type):
    """
    Get the current price of a cryptocurrency in USD.
    Falls back to the last known price if API fails.
    
    Args:
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        
    Returns:
        float: The current price in USD, or last known price if unavailable
    """
    crypto_type = crypto_type.upper()
    
    if crypto_type not in CRYPTO_ID_MAP:
        logger.error(f"Unsupported cryptocurrency: {crypto_type}")
        return None
    
    crypto_id = CRYPTO_ID_MAP[crypto_type]
    
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={crypto_id}&vs_currencies=usd"
        response = requests.get(url)
        data = response.json()
        
        if crypto_id in data and 'usd' in data[crypto_id]:
            price = data[crypto_id]['usd']
            logger.info(f"Current {crypto_type} price: ${price}")
            save_price_to_db(crypto_type, price)
            return price
        else:
            logger.warning(f"Failed to get price for {crypto_type} from API")
            fallback_price = get_price_from_db(crypto_type)
            if fallback_price is not None:
                logger.info(f"Using last known {crypto_type} price: ${fallback_price}")
                return fallback_price
            return None
    except Exception as e:
        logger.warning(f"Error fetching {crypto_type} price: {str(e)}")
        fallback_price = get_price_from_db(crypto_type)
        if fallback_price is not None:
            logger.info(f"Using last known {crypto_type} price: ${fallback_price}")
            return fallback_price
        logger.error(f"No price available for {crypto_type} and no cached price found")
        return None

def convert_crypto_to_fiat(crypto_amount, crypto_type):
    """
    Convert a cryptocurrency amount to USD.
    
    Args:
        crypto_amount (float): The amount of cryptocurrency
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        
    Returns:
        float: The equivalent amount in USD
    """
    price = get_crypto_price(crypto_type)
    
    if price is None:
        return None
    
    usd_amount = float(crypto_amount) * price
    logger.info(f"{crypto_amount} {crypto_type} = ${usd_amount:.2f}")
    return usd_amount

def convert_fiat_to_crypto(usd_amount, crypto_type):
    """
    Convert a USD amount to cryptocurrency.
    
    Args:
        usd_amount (float): The amount in USD
        crypto_type (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
        
    Returns:
        float: The equivalent amount in the specified cryptocurrency
    """
    price = get_crypto_price(crypto_type)
    
    if price is None or price == 0:
        return None
    
    crypto_amount = float(usd_amount) / price
    logger.info(f"${usd_amount} = {crypto_amount} {crypto_type}")
    return crypto_amount