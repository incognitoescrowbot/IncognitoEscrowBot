import imghdr
import logging
import os
import sys
import sqlite3
import json
import requests
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
import re
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, CallbackContext, ConversationHandler, ContextTypes
)
from telegram.error import BadRequest
from web3 import Web3

try:
    from init_bitcoinlib import suppress_bitcoinlib_warnings, fix_bitcoinlib_database
    suppress_bitcoinlib_warnings()
    fix_bitcoinlib_database()
except Exception as e:
    print(f"Warning: Could not initialize bitcoinlib database fix: {e}")

import bitcoinlib
from bitcoinlib.wallets import Wallet
import uuid
import hashlib
import random
import string
# Import crypto_utils (compatibility layer for the crypto-utils package)
import crypto_utils
from crypto_utils import KeyManager, WalletManager, TransactionManager, ElectrumXClient
from crypto_utils import ADDRESS_TYPE_LEGACY, ADDRESS_TYPE_SEGWIT, ADDRESS_TYPE_NATIVE_SEGWIT
from crypto_price import get_crypto_price, convert_crypto_to_fiat, convert_fiat_to_crypto, init_crypto_prices_table
import btcwalletclient_wif
# Import Telethon for group creation
import asyncio
from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest, InviteToChannelRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError, FloodError
from dotenv import load_dotenv

load_dotenv()

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation states
SELECTING_CRYPTO, ENTERING_AMOUNT, ENTERING_RECIPIENT, CONFIRMING_TRANSACTION = range(4)
DISPUTE_REASON, DISPUTE_EVIDENCE = range(4, 6)
SELECTING_WALLET_TYPE, SELECTING_ADDRESS_TYPE, ENTERING_M, ENTERING_N, ENTERING_PUBLIC_KEYS, CONFIRMING_WALLET = range(6, 12)
SELECTING_WITHDRAW_WALLET, ENTERING_WITHDRAW_AMOUNT, ENTERING_WALLET_ADDRESS = range(12, 15)

# Global variables
app = None
telethon_client = None

# Welcome GIF URL
WELCOME_GIF_URL = os.getenv('WELCOME_GIF_URL', '')


# Database setup
def escape_markdown(text):
    """Escape special characters for Markdown formatting."""
    if text is None:
        return ""

    # Convert to string if it's not already
    text = str(text)

    # Escape special characters that have meaning in Markdown
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')

    return text

def safe_send_message(update, text, parse_mode=None, **kwargs):
    """
    Safely send a message with proper error handling for entity parsing errors.
    Falls back to plain text if entity parsing fails.
    """
    try:
        return update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
    except BadRequest as e:
        if "entity" in str(e).lower() and parse_mode:
            # If entity parsing fails, try sending without parse_mode
            print(f"Entity parsing error: {e}. Sending without formatting.")
            return update.message.reply_text(text, parse_mode=None, **kwargs)
        else:
            # Re-raise other BadRequest errors
            raise

async def safe_send_text(message_method, text, parse_mode=None, **kwargs):
    """
    A more general version of safe_send_message that works with any message sending method.
    Falls back to plain text if entity parsing fails.

    Args:
        message_method: The method to call for sending the message (e.g., update.message.reply_text, query.edit_message_text)
        text: The text to send
        parse_mode: The parse mode to use (ParseMode.MARKDOWN, ParseMode.HTML, etc.)
        **kwargs: Additional arguments to pass to the message method
    """
    try:
        return await message_method(text, parse_mode=parse_mode, **kwargs)
    except BadRequest as e:
        if "entity" in str(e).lower() and parse_mode:
            # If entity parsing fails, try sending without parse_mode
            print(f"Entity parsing error: {e}. Sending without formatting.")
            return await message_method(text, parse_mode=None, **kwargs)
        else:
            # Re-raise other BadRequest errors
            raise


def setup_database():
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        # Users table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS users
                       (
                           user_id
                           INTEGER
                           PRIMARY
                           KEY,
                           username
                           TEXT,
                           first_name
                           TEXT,
                           last_name
                           TEXT,
                           language_code
                           TEXT
                           DEFAULT
                           'en',
                           registration_date
                           TIMESTAMP
                           DEFAULT
                           CURRENT_TIMESTAMP
                       )
                       ''')

        # Wallets table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS wallets
                       (
                           wallet_id
                           TEXT
                           PRIMARY
                           KEY,
                           user_id
                           INTEGER,
                           crypto_type
                           TEXT,
                           address
                           TEXT,
                           private_key
                           TEXT,
                           balance
                           REAL
                           DEFAULT
                           0.0,
                           pending_balance
                           REAL
                           DEFAULT
                           0.0,
                           wallet_type
                           TEXT
                           DEFAULT
                           'single',
                           address_type
                           TEXT
                           DEFAULT
                           'segwit',
                           required_sigs
                           INTEGER
                           DEFAULT
                           1,
                           total_keys
                           INTEGER
                           DEFAULT
                           1,
                           public_keys
                           TEXT,
                           tx_hex
                           TEXT,
                           txid
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           user_id
                       ) REFERENCES users
                       (
                           user_id
                       )
                           )
                       ''')

        # Transactions table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS transactions
                       (
                           transaction_id
                           TEXT
                           PRIMARY
                           KEY,
                           seller_id
                           INTEGER,
                           buyer_id
                           INTEGER,
                           crypto_type
                           TEXT,
                           amount
                           REAL,
                           fee_amount
                           REAL,
                           status
                           TEXT,
                           creation_date
                           TIMESTAMP,
                           completion_date
                           TIMESTAMP,
                           description
                           TEXT,
                           wallet_id
                           TEXT,
                           tx_hex
                           TEXT,
                           txid
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           seller_id
                       ) REFERENCES users
                       (
                           user_id
                       ),
                           FOREIGN KEY
                       (
                           buyer_id
                       ) REFERENCES users
                       (
                           user_id
                       ),
                           FOREIGN KEY
                       (
                           wallet_id
                       ) REFERENCES wallets
                       (
                           wallet_id
                       )
                           )
                       ''')

        # Disputes table
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS disputes
                       (
                           dispute_id
                           TEXT
                           PRIMARY
                           KEY,
                           transaction_id
                           TEXT,
                           initiator_id
                           INTEGER,
                           reason
                           TEXT,
                           evidence
                           TEXT,
                           status
                           TEXT,
                           creation_date
                           TIMESTAMP,
                           resolution_date
                           TIMESTAMP,
                           resolution_notes
                           TEXT,
                           FOREIGN
                           KEY
                       (
                           transaction_id
                       ) REFERENCES transactions
                       (
                           transaction_id
                       ),
                           FOREIGN KEY
                       (
                           initiator_id
                       ) REFERENCES users
                       (
                           user_id
                       )
                           )
                       ''')

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def migrate_wallets_table():
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(wallets)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'pending_balance' not in columns:
            cursor.execute('ALTER TABLE wallets ADD COLUMN pending_balance REAL DEFAULT 0.0')
            conn.commit()
            print("Added pending_balance column to wallets table")
    except sqlite3.Error as e:
        print(f"Database migration error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def migrate_transactions_table():
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(transactions)")
        columns = [column[1] for column in cursor.fetchall()]

        if 'recipient_username' not in columns:
            cursor.execute('ALTER TABLE transactions ADD COLUMN recipient_username TEXT')
            conn.commit()
            print("Added recipient_username column to transactions table")
    except sqlite3.Error as e:
        print(f"Database migration error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# User management functions
def get_or_create_user(user_id, username, first_name, last_name, language_code='en'):
    conn = None
    user = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()

        if not user:
            cursor.execute(
                'INSERT INTO users (user_id, username, first_name, last_name, language_code) VALUES (?, ?, ?, ?, ?)',
                (user_id, username, first_name, last_name, language_code)
            )
            conn.commit()
            # Fetch the user after insertion
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            user = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_or_create_user: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    return user


def process_pending_recipient(user_id, username):
    """
    Process any pending transactions for a recipient when they start the bot.
    Updates transactions and pending balances for recipients who weren't in the database
    when the transaction was initiated.
    """
    if not username:
        return {'success': False, 'transactions_updated': 0}
    
    conn = None
    transactions_updated = 0
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        username_clean = username.lstrip('@')
        
        cursor.execute(
            '''SELECT transaction_id, crypto_type, amount 
               FROM transactions 
               WHERE recipient_username = ? AND seller_id IS NULL''',
            (f"@{username_clean}",)
        )
        pending_transactions = cursor.fetchall()
        
        if not pending_transactions:
            cursor.execute(
                '''SELECT transaction_id, crypto_type, amount 
                   FROM transactions 
                   WHERE recipient_username = ? AND seller_id IS NULL''',
                (username_clean,)
            )
            pending_transactions = cursor.fetchall()
        
        for transaction_id, crypto_type, amount in pending_transactions:
            pending_result = add_to_pending_balance(user_id, crypto_type, amount)
            
            if pending_result['success']:
                cursor.execute(
                    '''UPDATE transactions 
                       SET seller_id = ?, recipient_username = NULL 
                       WHERE transaction_id = ?''',
                    (user_id, transaction_id)
                )
                transactions_updated += 1
                logger.info(f"Updated transaction {transaction_id} with recipient user_id {user_id}")
        
        conn.commit()
        return {'success': True, 'transactions_updated': transactions_updated}
        
    except sqlite3.Error as e:
        logger.error(f"Database error in process_pending_recipient: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'transactions_updated': 0}
    finally:
        if conn:
            conn.close()


async def ensure_user_and_process_pending(update: Update) -> dict:
    """
    Ensure user exists in database and process any pending recipient transactions.
    Should be called at the start of each command handler.
    
    Returns dict with 'transactions_updated' count for notification purposes.
    """
    user = update.effective_user
    get_or_create_user(user.id, user.username, user.first_name, user.last_name, user.language_code)
    
    pending_result = process_pending_recipient(user.id, user.username)
    return pending_result


def get_user_id_from_username(username):
    conn = None
    user_id = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        username_to_search = username.lstrip('@')
        cursor.execute('SELECT user_id FROM users WHERE username = ?', (username_to_search,))
        result = cursor.fetchone()

        if result:
            user_id = result[0]
    except sqlite3.Error as e:
        print(f"Database error in get_user_id_from_username: {e}")
    finally:
        if conn:
            conn.close()

    return user_id


# Wallet management functions
def create_wallet(user_id, crypto_type, wallet_type='single', address_type=ADDRESS_TYPE_SEGWIT, m=1, n=1, public_keys=None):
    """
    Create a wallet for a user

    Args:
        user_id: User ID
        crypto_type: Cryptocurrency type (BTC, ETH, etc.)
        wallet_type: Wallet type ('single' or 'multisig')
        address_type: Address type ('legacy', 'segwit', 'native_segwit')
        m: Number of signatures required (for multisig)
        n: Total number of keys (for multisig)
        public_keys: List of public keys (for multisig)

    Returns:
        Tuple[str, str]: (wallet_id, address)
    """
    wallet_id = str(uuid.uuid4())
    wallet_name = f"user_{user_id}_{crypto_type.lower()}_{wallet_id}"
    address = None
    private_key = None
    public_keys_json = None

    try:
        # Generate wallet based on crypto type and wallet type
        if crypto_type.upper() == 'BTC':
            if wallet_type == 'single':
                # Create single-signature Bitcoin wallet
                wallet_name, address, private_key = WalletManager.create_single_sig_wallet(wallet_name, address_type)
                public_keys_json = None
            elif wallet_type == 'multisig':
                # Create multisig Bitcoin wallet
                wallet_name, address, private_keys = WalletManager.create_multisig_wallet(wallet_name, m, n, public_keys, address_type)
                # Convert private keys to hex strings if they are bytes objects
                if isinstance(private_keys, list):
                    private_keys_hex = [pk.hex() if isinstance(pk, bytes) else pk for pk in private_keys]
                    private_key = json.dumps(private_keys_hex)
                else:
                    private_key = private_keys

                if public_keys is None:
                    # If public keys were generated, get them from the wallet
                    wallet = Wallet(wallet_name)
                    public_keys = [key.public for key in wallet.keys()]

                # Convert public keys to hex strings if they are bytes objects
                if public_keys:
                    public_keys_hex = [pk.hex() if isinstance(pk, bytes) else pk for pk in public_keys]
                    public_keys_json = json.dumps(public_keys_hex)
                else:
                    public_keys_json = None
            else:
                raise ValueError(f"Invalid wallet type: {wallet_type}")
        elif crypto_type.upper() in ['ETH', 'USDT']:
            # Create Ethereum wallet (multisig not supported yet)
            account = Web3().eth.account.create()
            address = account.address
            private_key = account.privateKey.hex()
            public_keys_json = None
            wallet_type = 'single'  # Force single for ETH/USDT
        else:
            # For other cryptocurrencies, implement appropriate wallet creation
            # This is a placeholder
            address = f"{crypto_type}_address_{wallet_id}"
            private_key = f"{crypto_type}_private_key_{wallet_id}"
            public_keys_json = None
            wallet_type = 'single'  # Force single for other cryptos

        conn = None
        try:
            conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
            cursor = conn.cursor()

            cursor.execute(
                '''INSERT INTO wallets
                   (wallet_id, user_id, crypto_type, address, private_key, wallet_type, address_type, required_sigs, total_keys, public_keys, tx_hex, txid)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (wallet_id, user_id, crypto_type.upper(), address, private_key, wallet_type, address_type, m, n, public_keys_json, None, None)
            )

            conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in create_wallet: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

        return wallet_id, address
    except Exception as e:
        print(f"Error creating wallet: {e}")
        return None, None


def get_user_wallets(user_id):
    conn = None
    wallets = []
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('''SELECT wallet_id, crypto_type, address, balance, private_key,
                                 wallet_type, address_type, required_sigs, total_keys, public_keys
                          FROM wallets WHERE user_id = ?''', (user_id,))
        wallets = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in get_user_wallets: {e}")
    finally:
        if conn:
            conn.close()
    return wallets


def get_wallet_balance(wallet_id):
    conn = None
    wallet = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT crypto_type, address, private_key, balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_wallet_balance: {e}")
    finally:
        if conn:
            conn.close()

    if not wallet:
        return None

    crypto_type, address, private_key, balance = wallet

    # In a real implementation, you would query the blockchain for the current balance
    # This is a placeholder
    return balance


def get_btc_balance_from_blockchain(address):
    """
    Fetch BTC balance from blockchain.com API for a given address

    Args:
        address (str): Bitcoin wallet address

    Returns:
        float: Balance in BTC, or None if request fails
    """
    try:
        url = f"https://blockchain.info/q/addressbalance/{address}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            balance_satoshis = int(response.text.strip())
            balance_btc = balance_satoshis / 100000000
            return balance_btc
        else:
            print(f"Blockchain.com API error: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching BTC balance from blockchain: {e}")
        return None


def update_wallet_balance(wallet_id, new_balance):
    """
    Update the stored balance in the database for a wallet

    Args:
        wallet_id (str): Wallet ID
        new_balance (float): New balance value

    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
            (new_balance, wallet_id)
        )

        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Database error updating wallet balance: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def sync_blockchain_balance(wallet_id):
    """
    Fetch current balance from blockchain and sync with database.
    If blockchain balance differs, reconcile the difference.

    Args:
        wallet_id (str): Wallet ID

    Returns:
        dict: {success: bool, old_balance: float, new_blockchain_balance: float, db_balance: float, difference: float}
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT address, balance, crypto_type FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()

        if not wallet:
            return {'success': False, 'error': 'Wallet not found'}

        address, db_balance, crypto_type = wallet

        if crypto_type.upper() != 'BTC':
            return {'success': False, 'error': 'Balance sync only supported for BTC'}

        blockchain_balance = get_btc_balance_from_blockchain(address)

        if blockchain_balance is None:
            return {'success': False, 'error': 'Failed to fetch balance from blockchain'}

        old_balance = db_balance
        difference = blockchain_balance - db_balance

        if difference != 0:
            if difference > 0:
                reconciled_balance = db_balance + difference
            else:
                reconciled_balance = db_balance

            update_wallet_balance(wallet_id, reconciled_balance)

            return {
                'success': True,
                'old_balance': old_balance,
                'new_blockchain_balance': blockchain_balance,
                'db_balance': reconciled_balance,
                'difference': difference,
                'reconciled': True
            }
        else:
            return {
                'success': True,
                'old_balance': old_balance,
                'new_blockchain_balance': blockchain_balance,
                'db_balance': db_balance,
                'difference': 0,
                'reconciled': False
            }
    except sqlite3.Error as e:
        print(f"Database error in sync_blockchain_balance: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def subtract_wallet_balance(wallet_id, amount):
    """
    Subtract an amount from wallet balance when transaction is initiated.

    Args:
        wallet_id (str): Wallet ID
        amount (float): Amount to subtract

    Returns:
        dict: {success: bool, old_balance: float, new_balance: float, error: str (if any)}
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        result = cursor.fetchone()

        if not result:
            return {'success': False, 'error': 'Wallet not found'}

        old_balance = result[0]
        new_balance = old_balance - amount

        if new_balance < 0:
            return {'success': False, 'error': f'Insufficient balance. Required: {amount}, Available: {old_balance}'}

        cursor.execute(
            'UPDATE wallets SET balance = ? WHERE wallet_id = ?',
            (new_balance, wallet_id)
        )

        conn.commit()

        return {'success': True, 'old_balance': old_balance, 'new_balance': new_balance}
    except sqlite3.Error as e:
        print(f"Database error in subtract_wallet_balance: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def add_to_pending_balance(user_id, crypto_type, amount):
    """
    Add an amount to the pending balance of a user's wallet.

    Args:
        user_id (int): User ID
        crypto_type (str): Cryptocurrency type (e.g., 'BTC')
        amount (float): Amount to add to pending balance

    Returns:
        dict: {success: bool, old_pending_balance: float, new_pending_balance: float, wallet_id: str, error: str (if any)}
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT wallet_id, pending_balance FROM wallets WHERE user_id = ? AND crypto_type = ?',
                       (user_id, crypto_type))
        result = cursor.fetchone()

        if not result:
            return {'success': False, 'error': 'Wallet not found for recipient'}

        wallet_id, old_pending_balance = result
        new_pending_balance = old_pending_balance + amount

        cursor.execute(
            'UPDATE wallets SET pending_balance = ? WHERE wallet_id = ?',
            (new_pending_balance, wallet_id)
        )

        conn.commit()

        return {
            'success': True,
            'wallet_id': wallet_id,
            'old_pending_balance': old_pending_balance,
            'new_pending_balance': new_pending_balance
        }
    except sqlite3.Error as e:
        print(f"Database error in add_to_pending_balance: {e}")
        if conn:
            conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


# Transaction management functions
def create_transaction(seller_id, buyer_id, crypto_type, amount, description="", wallet_id=None, tx_hex=None, txid=None, recipient_username=None):
    transaction_id = str(uuid.uuid4())
    fee_amount = amount * 0.05  # 5% fee

    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''INSERT INTO transactions
               (transaction_id, seller_id, buyer_id, crypto_type, amount, fee_amount, status, creation_date, description, wallet_id, tx_hex, txid, recipient_username)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (transaction_id, seller_id, buyer_id, crypto_type, amount, fee_amount, 'PENDING', datetime.now().isoformat(), description, wallet_id, tx_hex, txid, recipient_username)
        )

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in create_transaction: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

    return transaction_id


def get_transaction(transaction_id):
    conn = None
    transaction = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM transactions WHERE transaction_id = ?', (transaction_id,))
        transaction = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in get_transaction: {e}")
    finally:
        if conn:
            conn.close()
    return transaction


def update_transaction_status(transaction_id, status):
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        if status == 'COMPLETED':
            cursor.execute(
                'UPDATE transactions SET status = ?, completion_date = ? WHERE transaction_id = ?',
                (status, datetime.now().isoformat(), transaction_id)
            )
        else:
            cursor.execute(
                'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                (status, transaction_id)
            )

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in update_transaction_status: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def check_and_update_expired_transactions():
    """
    Check for pending transactions older than 24 hours and update their status to EXPIRED.
    Returns the number of transactions that were expired.
    """
    conn = None
    expired_count = 0
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        expiration_time = (datetime.now() - timedelta(hours=24)).isoformat()
        
        cursor.execute(
            '''UPDATE transactions 
               SET status = 'EXPIRED' 
               WHERE status = 'PENDING' 
               AND creation_date < ?''',
            (expiration_time,)
        )
        
        expired_count = cursor.rowcount
        conn.commit()
        
        if expired_count > 0:
            logger.info(f"Expired {expired_count} transaction(s) older than 24 hours")
        
    except sqlite3.Error as e:
        logger.error(f"Database error in check_and_update_expired_transactions: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    
    return expired_count


def is_transaction_expired(transaction):
    """
    Check if a transaction has been pending for more than 24 hours.
    
    Args:
        transaction: Transaction tuple from database
        
    Returns:
        bool: True if transaction is expired, False otherwise
    """
    if not transaction:
        return False
    
    creation_date_str = transaction[7]
    status = transaction[6]
    
    if status != 'PENDING':
        return False
    
    try:
        creation_date = datetime.fromisoformat(creation_date_str)
        time_elapsed = datetime.now() - creation_date
        return time_elapsed > timedelta(hours=24)
    except (ValueError, TypeError) as e:
        logger.error(f"Error parsing transaction date: {e}")
        return False


def get_user_transactions(user_id):
    conn = None
    transactions = []
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT *
               FROM transactions
               WHERE seller_id = ?
                  OR buyer_id = ?
               ORDER BY creation_date DESC''',
            (user_id, user_id)
        )
        transactions = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error in get_user_transactions: {e}")
    finally:
        if conn:
            conn.close()
    return transactions


def get_user_pending_transaction_balance(user_id, crypto_type):
    """
    Calculate the total pending balance from transactions where the user is the recipient (seller).
    
    Args:
        user_id: The user's ID
        crypto_type: The cryptocurrency type (e.g., 'BTC')
    
    Returns:
        The total pending balance for the specified crypto type
    """
    conn = None
    pending_balance = 0.0
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT SUM(amount)
               FROM transactions
               WHERE seller_id = ? AND crypto_type = ? AND status = 'PENDING' ''',
            (user_id, crypto_type)
        )
        result = cursor.fetchone()
        if result and result[0] is not None:
            pending_balance = result[0]
    except sqlite3.Error as e:
        print(f"Database error in get_user_pending_transaction_balance: {e}")
    finally:
        if conn:
            conn.close()
    return pending_balance


def auto_refresh_user_balances(user_id):
    """
    Automatically refresh all wallet balances for a user.
    This function is called before executing any command to ensure balances are up-to-date.
    
    Args:
        user_id (int): The user's Telegram ID
        
    Returns:
        dict: Summary of refresh results
    """
    try:
        wallets = get_user_wallets(user_id)
        refresh_results = {
            'total_wallets': len(wallets),
            'refreshed_count': 0,
            'failed_count': 0,
            'btc_wallets_updated': 0,
            'errors': []
        }
        
        for wallet in wallets:
            wallet_id, crypto_type = wallet[0], wallet[1]
            
            if crypto_type.upper() == 'BTC':
                try:
                    sync_result = sync_blockchain_balance(wallet_id)
                    if sync_result['success']:
                        refresh_results['refreshed_count'] += 1
                        refresh_results['btc_wallets_updated'] += 1
                        logger.info(f"Auto-refreshed BTC wallet {wallet_id} for user {user_id}")
                    else:
                        refresh_results['failed_count'] += 1
                        refresh_results['errors'].append(f"Failed to sync wallet {wallet_id}: {sync_result.get('error', 'Unknown error')}")
                except Exception as e:
                    refresh_results['failed_count'] += 1
                    refresh_results['errors'].append(f"Error syncing wallet {wallet_id}: {str(e)}")
                    logger.error(f"Error auto-refreshing wallet {wallet_id}: {e}")
            else:
                refresh_results['refreshed_count'] += 1
        
        logger.info(f"Auto-refresh completed for user {user_id}: {refresh_results['refreshed_count']}/{refresh_results['total_wallets']} wallets processed")
        return refresh_results
        
    except Exception as e:
        logger.error(f"Error in auto_refresh_user_balances for user {user_id}: {e}")
        return {
            'total_wallets': 0,
            'refreshed_count': 0,
            'failed_count': 1,
            'btc_wallets_updated': 0,
            'errors': [f"General error: {str(e)}"]
        }


def with_auto_balance_refresh(command_func):
    """
    Decorator that automatically refreshes user wallet balances before executing a command.
    
    Args:
        command_func: The command function to wrap
        
    Returns:
        Wrapped function that refreshes balances first
    """
    async def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user = update.effective_user
        if user and user.id:
            try:
                auto_refresh_user_balances(user.id)
            except Exception as e:
                logger.error(f"Error auto-refreshing balances for user {user.id} in command {command_func.__name__}: {e}")
        
        return await command_func(update, context, *args, **kwargs)
    
    wrapper.__name__ = command_func.__name__
    wrapper.__doc__ = command_func.__doc__
    return wrapper


def has_pending_transactions(user_id, crypto_type='BTC'):
    """
    Check if a user has any pending transactions (as buyer or seller).
    
    Args:
        user_id: The user's ID
        crypto_type: The cryptocurrency type (e.g., 'BTC')
    
    Returns:
        bool: True if user has pending transactions, False otherwise
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute(
            '''SELECT COUNT(*)
               FROM transactions
               WHERE (seller_id = ? OR buyer_id = ?) 
                 AND crypto_type = ? 
                 AND status IN ('PENDING', 'DISPUTED')''',
            (user_id, user_id, crypto_type)
        )
        result = cursor.fetchone()
        return result[0] > 0 if result else False
    except sqlite3.Error as e:
        print(f"Database error in has_pending_transactions: {e}")
        return False
    finally:
        if conn:
            conn.close()


# Dispute management functions
def create_dispute(transaction_id, initiator_id, reason, evidence):
    dispute_id = str(uuid.uuid4())

    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''INSERT INTO disputes
               (dispute_id, transaction_id, initiator_id, reason, evidence, status, creation_date)
               VALUES (?, ?, ?, ?, ?, ?, ?)''',
            (dispute_id, transaction_id, initiator_id, reason, evidence, 'OPEN', datetime.now().isoformat())
        )

        # Update transaction status
        cursor.execute(
            'UPDATE transactions SET status = ? WHERE transaction_id = ?',
            ('DISPUTED', transaction_id)
        )

        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error in create_dispute: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

    return dispute_id


def resolve_dispute(dispute_id, resolution, notes):
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute('SELECT transaction_id FROM disputes WHERE dispute_id = ?', (dispute_id,))
        result = cursor.fetchone()
        if not result:
            print(f"Dispute {dispute_id} not found")
            return False

        transaction_id = result[0]
        
        cursor.execute('SELECT buyer_id, seller_id, crypto_type, wallet_id FROM transactions WHERE transaction_id = ?', (transaction_id,))
        transaction = cursor.fetchone()
        if not transaction:
            print(f"Transaction {transaction_id} not found")
            return False
        
        buyer_id, seller_id, crypto_type, wallet_id = transaction

        cursor.execute(
            '''UPDATE disputes
               SET status           = ?,
                   resolution_date  = ?,
                   resolution_notes = ?
               WHERE dispute_id = ?''',
            ('RESOLVED', datetime.now().isoformat(), notes, dispute_id)
        )

        if resolution == 'REFUNDED' and crypto_type == 'BTC':
            refund_result = refund_btc_to_buyer(wallet_id, seller_id)
            
            if refund_result['success']:
                cursor.execute(
                    'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                    (resolution, transaction_id)
                )
                print(f"Dispute resolved: 50% sent to seller ({refund_result['seller_amount']:.8f} BTC), "
                      f"50% sent to fee wallet ({refund_result['fee_amount']:.8f} BTC). "
                      f"Transaction ID: {refund_result['txid']}")
            else:
                print(f"Failed to process refund: {refund_result.get('error', 'Unknown error')}")
                return False
        else:
            cursor.execute(
                'UPDATE transactions SET status = ? WHERE transaction_id = ?',
                (resolution, transaction_id)
            )

        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Database error in resolve_dispute: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"Error in resolve_dispute: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


# Bot command handlers
@with_auto_balance_refresh
async def start(update: Update, context: CallbackContext) -> None:
    user = update.effective_user
    get_or_create_user(user.id, user.username, user.first_name, user.last_name, user.language_code)
    
    check_and_update_expired_transactions()
    
    pending_result = process_pending_recipient(user.id, user.username)
    if pending_result['success'] and pending_result['transactions_updated'] > 0:
        await update.message.reply_text(
            f"✅ {pending_result['transactions_updated']} pending transaction(s) have been linked to your account!\n"
            f"Check /transactions to view them."
        )

    welcome_message = (
        f"Welcome to the Incognito Escrow Bot, {user.first_name}!\n\n"
        "We are your trusted escrow service for secure transactions. "
        "Keep your funds safe and pay other Telegram users seamlessly with confidence.\n\n"
        "_Tap 'Help' button for guidance_\n\n"
        "NEW: Now supporting multisig wallets, SegWit address format, ElectrumX connectivity!"
    )

    # Create a ReplyKeyboardMarkup with the required buttons
    keyboard = [
        [KeyboardButton("My Account"), KeyboardButton("Transaction History")],
        [KeyboardButton("Language"), KeyboardButton("Help")],
        [KeyboardButton("Withdraw Funds")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    if WELCOME_GIF_URL:
        try:
            await update.message.reply_animation(
                animation=WELCOME_GIF_URL,
                caption=welcome_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except BadRequest as e:
            logger.warning(f"Failed to send GIF: {e}. Falling back to text message.")
            await update.message.reply_text(
                welcome_message,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    else:
        await update.message.reply_text(
            welcome_message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )


@with_auto_balance_refresh
async def help_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    help_text = (
        "**Instructions:**\n\n"
        "Step 1)\n"
        "Tap \"My Account\" button\n\n"
        "Step 2)\n"
        "Create a new wallet for escrow\n\n"
        "Step 3)\n"
        "Choose address type for escrow wallet\n\n"
        "Step 4)\n"
        "Deposit funds to your wallet address\n\n"
        "Step 5)\n"
        "Tap \"Start Trade\" button to initiate a transaction with your escrow wallet\n\n"
        "Step 6)\n"
        "Enter USD value of the amount of BTC you want to initiate a transaction for\n\n"
        "Step 7)\n"
        "Enter Telegram username of the recipient\n\n"
        "Step 8)\n"
        "Provide a description for this transaction\n\n"
        "Step 9)\n"
        "Check and confirm transaction details\n\n"
        "Step 10)\n"
        "Once you have received product as described, tap \"Release Funds\" button\n\n"

        "*If Something Goes Wrong:*\n"
        "As a buyer, use /dispute to open a dispute if there's a problem with your transaction. "
        "Our team will review the evidence and make a fair decision within 1-2 business days.\n\n"

        "*Fees:*\n"
        "We charge a 5% fee on all successful transactions."
    )

    await safe_send_message(update, help_text, parse_mode=ParseMode.MARKDOWN)


@with_auto_balance_refresh
async def wallet_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    wallets = get_user_wallets(user.id)

    if not wallets:
        keyboard = [
            [
                InlineKeyboardButton("Bitcoin (BTC)", callback_data='create_wallet_BTC')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "You don't have any wallets yet. Choose a cryptocurrency to create your first wallet:",
            reply_markup=reply_markup
        )
    else:
        wallet_text = "Your wallets:\n\n"
        for wallet in wallets:
            wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
            wallet_type = wallet[5] if len(wallet) > 5 else "single"
            address_type = wallet[6] if len(wallet) > 6 else "segwit"

            # Get USD value of the balance
            usd_balance = convert_crypto_to_fiat(balance, crypto_type)
            usd_value_text = f"(${usd_balance:.2f} USD)" if usd_balance is not None else "(USD value unavailable)"

            # Get blockchain balance for BTC
            blockchain_balance = None
            if crypto_type == 'BTC':
                blockchain_balance = get_btc_balance_from_blockchain(address)
            
            # Get pending transaction balance
            pending_tx_balance = get_user_pending_transaction_balance(user.id, crypto_type)
            pending_usd_balance = convert_crypto_to_fiat(pending_tx_balance, crypto_type)
            pending_usd_value_text = f"(${pending_usd_balance:.2f} USD)" if pending_usd_balance is not None else "(USD value unavailable)"

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            wallet_text += f"*{crypto_type}*\n"
            wallet_text += f"Type: {address_type.capitalize()}\n"
            wallet_text += f"Address: `{escaped_address}`\n"
            
            # Show balance if > 0, otherwise show pending if there are pending transactions
            if balance > 0:
                wallet_text += f"Balance: {balance:.6f} {crypto_type} {usd_value_text}\n"
            elif pending_tx_balance > 0:
                wallet_text += f"Pending: {pending_tx_balance:.6f} {crypto_type} {pending_usd_value_text}\n"

            if wallet_type == "multisig" and len(wallet) > 7:
                m, n = wallet[7], wallet[8]
                wallet_text += f"Signatures required: {m} of {n}\n"

            wallet_text += "\n"

        keyboard = [
            [
                InlineKeyboardButton("Create New Wallet", callback_data='create_new_wallet'),
                InlineKeyboardButton("Refresh Balances", callback_data='refresh_balances')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_send_text(
            update.message.reply_text,
            wallet_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )


async def wallet_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data == 'create_new_wallet':
        keyboard = [
            [
                InlineKeyboardButton("Bitcoin (BTC)", callback_data='create_wallet_BTC')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "Choose a cryptocurrency to create a new wallet:",
            reply_markup=reply_markup
        )
    elif data.startswith('create_wallet_'):
        crypto_type = data.split('_')[-1]

        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                query.edit_message_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            if crypto_type == 'BTC':
                keyboard = [
                    [
                        InlineKeyboardButton("SegWit", callback_data='confirm_wallet_BTC_segwit')
                    ]
                ]

                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(
                    "Create Bitcoin (BTC) wallet with SegWit address type:",
                    reply_markup=reply_markup
                )
            else:
                wallet_id, address = create_wallet(user.id, crypto_type)

                escaped_address = escape_markdown(address)
                await safe_send_text(
                    query.edit_message_text,
                    f"✅ Your new {crypto_type} wallet has been created!\n\n"
                    f"Address: `{escaped_address}`\n\n"
                    f"Use this address to deposit funds into your escrow account.",
                    parse_mode=ParseMode.MARKDOWN
                )
    elif data == 'confirm_wallet_BTC_segwit':
        wallet_id, address = create_wallet(user.id, 'BTC', address_type=ADDRESS_TYPE_SEGWIT)

        escaped_address = escape_markdown(address)
        await safe_send_text(
            query.edit_message_text,
            f"✅ Your new Bitcoin (BTC) wallet with SegWit address has been created!\n\n"
            f"Address: `{escaped_address}`\n\n"
            f"Use this address to deposit funds into your escrow account.",
            parse_mode=ParseMode.MARKDOWN
        )
    elif data.startswith('create_multisig_'):
        crypto_type = data.split('_')[-1]

        # Store crypto type in user data for the conversation
        context.user_data['crypto_type'] = crypto_type

        # Ask for wallet type (address format)
        keyboard = [
            [
                InlineKeyboardButton("Legacy", callback_data=f'address_type_{ADDRESS_TYPE_LEGACY}'),
                InlineKeyboardButton("SegWit", callback_data=f'address_type_{ADDRESS_TYPE_SEGWIT}')
            ],
            [
                InlineKeyboardButton("Native SegWit", callback_data=f'address_type_{ADDRESS_TYPE_NATIVE_SEGWIT}')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"You're creating a {crypto_type} multisig wallet.\n\n"
            f"Choose the address format:",
            reply_markup=reply_markup
        )

        return SELECTING_ADDRESS_TYPE
    elif data.startswith('address_type_'):
        address_type = data.split('_')[-1]

        # Store address type in user data for the conversation
        context.user_data['address_type'] = address_type

        # For BTC multisig wallets, default to 2-of-3
        crypto_type = context.user_data.get('crypto_type', '')
        if crypto_type == 'BTC':
            # Set default values
            context.user_data['m'] = 2
            context.user_data['n'] = 3

            # Ask if user wants to enter public keys or generate new ones
            keyboard = [
                [
                    InlineKeyboardButton("Generate new keys", callback_data='generate_keys'),
                    InlineKeyboardButton("Enter public keys", callback_data='enter_keys')
                ]
            ]

            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"You're creating a 2-of-3 multisig wallet for {crypto_type}.\n\n"
                f"Do you want to generate new keys or enter existing public keys?",
                reply_markup=reply_markup
            )

            return ENTERING_PUBLIC_KEYS
        else:
            # For other cryptocurrencies, ask for m and n values
            await query.edit_message_text(
                f"How many signatures should be required to spend from this wallet? (m in m-of-n)\n\n"
                f"Enter a number between 1 and 15:"
            )

            return ENTERING_M
    elif data == 'refresh_balances':
        wallets = get_user_wallets(user.id)

        wallet_text = "Your wallets (balances updated):\n\n"
        for wallet in wallets:
            wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
            wallet_type = wallet[5] if len(wallet) > 5 else "single"
            address_type = wallet[6] if len(wallet) > 6 else "segwit"

            if crypto_type.upper() == 'BTC':
                sync_result = sync_blockchain_balance(wallet_id)
                if sync_result['success']:
                    balance = sync_result['db_balance']
                    sync_status = ""
                else:
                    sync_status = ""
            else:
                sync_status = ""

            usd_balance = convert_crypto_to_fiat(balance, crypto_type)
            usd_value_text = f"(${usd_balance:.2f} USD)" if usd_balance is not None else "(USD value unavailable)"

            pending_tx_balance = get_user_pending_transaction_balance(user.id, crypto_type)
            pending_usd_balance = convert_crypto_to_fiat(pending_tx_balance, crypto_type)
            pending_usd_value_text = f"(${pending_usd_balance:.2f} USD)" if pending_usd_balance is not None else "(USD value unavailable)"

            escaped_address = escape_markdown(address)
            wallet_text += f"*{crypto_type}*\n"
            wallet_text += f"Type: {address_type.capitalize()}\n"
            wallet_text += f"Address: `{escaped_address}`\n"
            
            # Show balance if > 0, otherwise show pending if there are pending transactions
            if balance > 0:
                wallet_text += f"Balance: {balance:.6f} {crypto_type} {usd_value_text}{sync_status}\n"
            elif pending_tx_balance > 0:
                wallet_text += f"Pending: {pending_tx_balance:.6f} {crypto_type} {pending_usd_value_text}\n"

            if wallet_type == "multisig" and len(wallet) > 7:
                m, n = wallet[7], wallet[8]
                wallet_text += f"Signatures required: {m} of {n}\n"

            wallet_text += "\n"

        keyboard = [
            [
                InlineKeyboardButton("Create New Wallet", callback_data='create_new_wallet'),
                InlineKeyboardButton("Refresh Balances", callback_data='refresh_balances')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_send_text(
            query.edit_message_text,
            wallet_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return


@with_auto_balance_refresh
async def deposit_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    # Clear any existing conversation state to ensure fresh start
    context.user_data.clear()
    
    user = update.effective_user
    wallets = get_user_wallets(user.id)

    if not wallets:
        await update.message.reply_text(
            "You don't have any wallets yet. Please create a wallet first using the /wallet command."
        )
        return ConversationHandler.END

    keyboard = []
    for wallet in wallets:
        wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
        # Get USD value of the balance
        usd_balance = convert_crypto_to_fiat(balance, crypto_type)
        usd_value_text = f"${usd_balance:.2f} USD" if usd_balance is not None else "USD value unavailable"

        keyboard.append(
            [InlineKeyboardButton(f"{crypto_type} ({usd_value_text} available)", callback_data=f"deposit_{crypto_type}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select the cryptocurrency you want to use for this transaction:",
        reply_markup=reply_markup
    )

    return SELECTING_CRYPTO


async def select_crypto(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    crypto_type = query.data.split('_')[1]
    context.user_data['crypto_type'] = crypto_type

    # Get current price of the cryptocurrency in USD
    price = get_crypto_price(crypto_type)
    price_info = f"Current {crypto_type} price: ${price:.2f} USD" if price is not None else "Price information unavailable"

    await query.edit_message_text(
        f"You selected {crypto_type}. {price_info}\n\n"
        f"Please enter the amount in USD you want to deposit (e.g., 100):"
    )

    return ENTERING_AMOUNT


async def enter_amount(update: Update, context: CallbackContext) -> int:
    try:
        usd_amount = float(update.message.text.strip())
        if usd_amount <= 0:
            await update.message.reply_text("Amount must be greater than 0. Please try again:")
            return ENTERING_AMOUNT

        crypto_type = context.user_data['crypto_type']

        # Convert USD amount to cryptocurrency amount
        crypto_amount = convert_fiat_to_crypto(usd_amount, crypto_type)

        if crypto_amount is None:
            await update.message.reply_text(
                "Unable to convert USD to cryptocurrency at this time. Please try again later."
            )
            return ConversationHandler.END

        # Store both USD and crypto amounts
        context.user_data['usd_amount'] = usd_amount
        context.user_data['amount'] = crypto_amount

        # Calculate fees
        usd_fee = usd_amount * 0.05
        usd_total = usd_amount + usd_fee

        crypto_fee = crypto_amount * 0.05
        crypto_total = crypto_amount + crypto_fee

        await update.message.reply_text(
            f"Transaction amount: ${usd_amount:.2f} USD\n"
            f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
            f"Total: ${usd_total:.2f} USD\n\n"
            "Please enter the Telegram username of the recipient (e.g., @username):"
        )

        return ENTERING_RECIPIENT
    except ValueError:
        await update.message.reply_text("Invalid amount. Please enter a valid number:")
        return ENTERING_AMOUNT


async def enter_recipient(update: Update, context: CallbackContext) -> int:
    recipient = update.message.text.strip()

    if not recipient.startswith('@'):
        await update.message.reply_text("Please enter a valid Telegram username starting with @:")
        return ENTERING_RECIPIENT

    context.user_data['recipient'] = recipient

    await update.message.reply_text(
        "Please enter a description for this transaction (e.g., 'Payment for design services'):"
    )

    return CONFIRMING_TRANSACTION


async def confirm_transaction(update: Update, context: CallbackContext) -> int:
    description = update.message.text.strip()
    context.user_data['description'] = description

    crypto_type = context.user_data['crypto_type']
    amount = context.user_data['amount']
    usd_amount = context.user_data['usd_amount']
    recipient = context.user_data['recipient']

    # Calculate fees
    fee = amount * 0.05
    usd_fee = usd_amount * 0.05

    # Calculate totals
    total = amount + fee
    usd_total = usd_amount + usd_fee

    keyboard = [
        [
            InlineKeyboardButton("Confirm", callback_data='confirm_transaction'),
            InlineKeyboardButton("Cancel", callback_data='cancel_transaction')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_send_text(
        update.message.reply_text,
        f"📝 *Transaction Summary*\n\n"
        f"Cryptocurrency: {crypto_type}\n"
        f"Amount: ${usd_amount:.2f} USD\n"
        f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
        f"Total: ${usd_total:.2f} USD\n"
        f"Recipient: {recipient}\n"
        f"Description: {description}\n\n"
        f"Please confirm this transaction:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

    return ConversationHandler.END


async def transaction_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data == 'confirm_transaction':
        crypto_type = context.user_data['crypto_type']
        amount = context.user_data['amount']
        usd_amount = context.user_data['usd_amount']
        recipient = context.user_data['recipient']
        description = context.user_data['description']

        fee = amount * 0.05
        total = amount + fee
        usd_fee = usd_amount * 0.05
        usd_total = usd_amount + usd_fee

        conn = None
        wallet = None
        try:
            conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
            cursor = conn.cursor()
            cursor.execute('SELECT wallet_id, balance FROM wallets WHERE user_id = ? AND crypto_type = ?',
                           (user.id, crypto_type))
            wallet = cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Database error in transaction_callback: {e}")
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: Database error\n\n"
                f"Please try again later.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        finally:
            if conn:
                conn.close()

        if not wallet:
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: No wallet found for {crypto_type}\n\n"
                f"Please create a wallet first.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        wallet_id, current_balance = wallet

        # Sync with blockchain to get actual balance before checking
        sync_result = sync_blockchain_balance(wallet_id)
        if sync_result['success']:
            current_balance = sync_result['new_blockchain_balance']
            print(f"Synced blockchain balance: {current_balance} BTC for wallet {wallet_id}")
        else:
            print(f"Failed to sync blockchain balance: {sync_result.get('error', 'Unknown error')}")
            # Continue with stored balance as fallback
        
        if current_balance < total:
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: Insufficient balance\n\n"
                f"Required: {total:.8f} {crypto_type}\n"
                f"Available: {current_balance:.8f} {crypto_type}\n",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        recipient_user_id = get_user_id_from_username(recipient)

        subtract_result = subtract_wallet_balance(wallet_id, total)
        if not subtract_result['success']:
            error_msg = subtract_result.get('error', 'Unknown error')
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: {error_msg}",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        pending_result = None
        if recipient_user_id:
            pending_result = add_to_pending_balance(recipient_user_id, crypto_type, total)
            if not pending_result['success']:
                add_back = add_to_pending_balance(user.id, crypto_type, total)
                await safe_send_text(
                    query.edit_message_text,
                    f"❌ Transaction failed!\n\n"
                    f"Reason: Failed to update recipient's pending balance\n\n"
                    f"Your balance has been restored.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return

        transaction_id = create_transaction(
            seller_id=recipient_user_id,
            buyer_id=user.id,
            crypto_type=crypto_type,
            amount=total,
            description=description,
            wallet_id=wallet_id,
            tx_hex=None,
            txid=None,
            recipient_username=recipient if not recipient_user_id else None
        )

        if not transaction_id:
            add_back = add_to_pending_balance(user.id, crypto_type, total)
            await safe_send_text(
                query.edit_message_text,
                f"❌ Transaction failed!\n\n"
                f"Reason: Failed to create transaction\n\n"
                f"Your balance has been restored.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        escaped_transaction_id = escape_markdown(transaction_id)

        group_created = False
        group_link = None

        if recipient_user_id:
            try:
                group_title = f"Escrow: {user.first_name or user.username} → {recipient}"
                group = await context.bot.create_supergroup(
                    title=group_title,
                    description=f"Escrow transaction {transaction_id}"
                )
                group_id = group.id

                await context.bot.add_chat_members(
                    chat_id=group_id,
                    user_ids=[user.id, recipient_user_id]
                )

                try:
                    invite_link = await context.bot.create_chat_invite_link(group_id)
                    group_link = invite_link.invite_link
                except Exception:
                    group_info = await context.bot.get_chat(group_id)
                    if group_info.invite_link:
                        group_link = group_info.invite_link

                await context.bot.send_message(
                    chat_id=group_id,
                    text=(
                        f"💰 *Escrow Transaction Created*\n\n"
                        f"*Sender:* {user.first_name or user.username}\n"
                        f"*Recipient:* {recipient}\n\n"
                        f"*Transaction Details:*\n"
                        f"*Cryptocurrency:* {crypto_type}\n"
                        f"*Amount:* {amount:.8f} {crypto_type}\n"
                        f"*USD Value:* ${usd_amount:.2f} USD\n"
                        f"*Escrow fee (5%):* ${usd_fee:.2f} USD\n"
                        f"*Total:* ${usd_total:.2f} USD\n"
                        f"*Transaction ID:* `{escaped_transaction_id}`\n\n"
                        f"*Description:* {description}\n\n"
                        f"⚠️ *Action Required:*\n"
                        f"@{recipient.lstrip('@')}, you need to run /start with the bot and create a {crypto_type} wallet so that the funds can be released to it."
                    ),
                    parse_mode=ParseMode.MARKDOWN
                )

                group_created = True
            except Exception as e:
                print(f"Error creating group or adding members: {e}")

        context.user_data['create_group_data'] = {
            'recipient': recipient,
            'transaction_id': transaction_id,
            'sender_name': user.first_name or user.username,
            'sender_username': user.username,
            'sender_id': user.id,
            'crypto_type': crypto_type,
            'amount': amount,
            'usd_amount': usd_amount,
            'fee': fee,
            'usd_fee': usd_fee,
            'total': total,
            'usd_total': usd_total,
            'description': description,
            'escaped_transaction_id': escaped_transaction_id
        }

        keyboard = []
        if group_created and group_link:
            keyboard.append([InlineKeyboardButton("Open Escrow Group", url=group_link)])
        keyboard.append([InlineKeyboardButton("Create Escrow Group", callback_data='create_escrow_group')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        if crypto_type.upper() == 'BTC':
            sync_result = sync_blockchain_balance(wallet_id)

            balance_info = ""
            if sync_result['success']:
                recipient_info = f"Recipient: {recipient}\n"
                if pending_result:
                    recipient_info += f"Recipient pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
                recipient_notification = "\nAn escrow group has been created with the recipient."

                await safe_send_text(
                    query.edit_message_text,
                    f"✅ Transaction initiated!\n\n"
                    f"Transaction ID: {escaped_transaction_id}\n\n"
                    f"Amount: ${usd_amount:.2f} USD\n"
                    f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                    f"Total: ${usd_total:.2f} USD\n\n"
                    f"{recipient_info}"
                    f"Balance after deduction: {subtract_result['new_balance']:.8f} {crypto_type}{balance_info}{recipient_notification}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
            else:
                recipient_info = f"Recipient: {recipient}\n"
                if pending_result:
                    recipient_info += f"Recipient pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
                recipient_notification = "\nAn escrow group has been created with the recipient."

                await safe_send_text(
                    query.edit_message_text,
                    f"⚠️ Transaction initiated but blockchain sync failed!\n\n"
                    f"Transaction ID: {escaped_transaction_id}\n\n"
                    f"Amount: ${usd_amount:.2f} USD\n"
                    f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                    f"Total: ${usd_total:.2f} USD\n\n"
                    f"{recipient_info}"
                    f"Balance after deduction: {subtract_result['new_balance']:.8f} {crypto_type}{recipient_notification}",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
        else:
            recipient_info = f"Recipient: {recipient}\n"
            if pending_result:
                recipient_info += f"Recipient pending balance: {pending_result['new_pending_balance']:.8f} {crypto_type}\n"
            recipient_notification = "\nAn escrow group has been created with the recipient."

            await safe_send_text(
                query.edit_message_text,
                f"✅ Transaction initiated!\n\n"
                f"Transaction ID: {escaped_transaction_id}\n\n"
                f"Amount: ${usd_amount:.2f} USD\n"
                f"Escrow fee (5%): ${usd_fee:.2f} USD\n"
                f"Total: ${usd_total:.2f} USD\n\n"
                f"{recipient_info}"
                f"Balance after deduction: {subtract_result['new_balance']:.8f} {crypto_type}{recipient_notification}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    elif data == 'cancel_transaction':
        await query.edit_message_text("Transaction cancelled.")


@with_auto_balance_refresh
async def transactions_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    check_and_update_expired_transactions()
    
    user = update.effective_user
    transactions = get_user_transactions(user.id)

    if not transactions:
        await update.message.reply_text("You don't have any transactions yet.")
        return

    transactions_text = "Your transactions:\n\n"
    for transaction in transactions:
        transaction_id = escape_markdown(transaction[0])
        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = escape_markdown(transaction[3])
        amount = transaction[4]
        status = escape_markdown(transaction[6])
        creation_date = escape_markdown(transaction[7])

        role = "Seller" if seller_id == user.id else "Buyer"

        # Get USD value of the amount
        usd_amount = convert_crypto_to_fiat(amount, crypto_type)
        usd_value_text = f"(${usd_amount:.2f} USD)" if usd_amount is not None else "(USD value unavailable)"
        # Escape the USD value text
        usd_value_text = escape_markdown(usd_value_text)

        transactions_text += f"*Transaction ID:* `{transaction_id}`\n"
        transactions_text += f"*Role:* {role}\n"
        transactions_text += f"*Cryptocurrency:* {crypto_type}\n"
        transactions_text += f"*Amount:* {amount} {crypto_type} {usd_value_text}\n"
        transactions_text += f"*Status:* {status}\n"
        transactions_text += f"*Date:* {creation_date}\n\n"

    await safe_send_text(
        update.message.reply_text,
        transactions_text,
        parse_mode=ParseMode.MARKDOWN
    )


@with_auto_balance_refresh
async def withdraw_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user
    
    # Check for pending transactions before allowing withdrawal
    if has_pending_transactions(user.id, 'BTC'):
        await update.message.reply_text(
            "❌ **Withdrawal Blocked**\n\n"
            "You cannot withdraw BTC while you have a pending transaction. "
            "Please wait for pending transaction to complete before attempting to withdraw.\n\n"
            "You can check your transaction status using the /transactions command.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END
    
    wallets = get_user_wallets(user.id)

    if not wallets:
        await update.message.reply_text(
            "You don't have any wallets yet. Please create a wallet first using the /wallet command."
        )
        return ConversationHandler.END

    btc_wallets = [w for w in wallets if w[1] == 'BTC']
    
    if not btc_wallets:
        await update.message.reply_text(
            "You don't have a BTC wallet yet. Please create one using the /wallet command."
        )
        return ConversationHandler.END

    keyboard = []
    for wallet in btc_wallets:
        wallet_id, crypto_type, address, balance = wallet[0], wallet[1], wallet[2], wallet[3]
        usd_balance = convert_crypto_to_fiat(balance, crypto_type)
        usd_value_text = f"${usd_balance:.2f} USD" if usd_balance is not None else "USD value unavailable"

        keyboard.append(
            [InlineKeyboardButton(
                f"{crypto_type}: {balance:.8f} ({usd_value_text})",
                callback_data=f"withdraw_{wallet_id}"
            )]
        )

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select the BTC wallet you want to withdraw from:",
        reply_markup=reply_markup
    )
    
    return SELECTING_WITHDRAW_WALLET


async def select_withdraw_wallet(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()
    
    wallet_id = query.data.replace('withdraw_', '')
    context.user_data['withdraw_wallet_id'] = wallet_id
    
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        cursor.execute('SELECT balance FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
        
        if wallet:
            balance = wallet[0]
            
            # Sync with blockchain to get actual balance
            sync_result = sync_blockchain_balance(wallet_id)
            if sync_result['success']:
                balance = sync_result['new_blockchain_balance']
                print(f"Synced withdraw wallet balance: {balance} BTC for wallet {wallet_id}")
            else:
                print(f"Failed to sync withdraw wallet balance: {sync_result.get('error', 'Unknown error')}")
                # Continue with stored balance as fallback
            
            context.user_data['withdraw_wallet_balance'] = balance
            
            await query.edit_message_text(
                f"Your current balance: {balance:.8f} BTC\n\n"
                f"How much BTC would you like to withdraw?\n"
                f"(Enter amount in BTC)"
            )
        else:
            await query.edit_message_text("Wallet not found.")
            return ConversationHandler.END
            
    except sqlite3.Error as e:
        await query.edit_message_text(f"Database error: {e}")
        return ConversationHandler.END
    finally:
        if conn:
            conn.close()
    
    return ENTERING_WITHDRAW_AMOUNT


async def enter_withdraw_amount(update: Update, context: CallbackContext) -> int:
    try:
        amount = float(update.message.text.strip())
        
        if amount <= 0:
            await update.message.reply_text("Amount must be greater than 0. Please try again:")
            return ENTERING_WITHDRAW_AMOUNT
        
        wallet_balance = context.user_data.get('withdraw_wallet_balance', 0)
        
        # Sync wallet balance with blockchain before checking
        wallet_id = context.user_data.get('withdraw_wallet_id')
        if wallet_id:
            sync_result = sync_blockchain_balance(wallet_id)
            if sync_result['success']:
                wallet_balance = sync_result['new_blockchain_balance']
                context.user_data['withdraw_wallet_balance'] = wallet_balance
                print(f"Synced withdraw wallet balance: {wallet_balance} BTC for wallet {wallet_id}")
            else:
                print(f"Failed to sync withdraw wallet balance: {sync_result.get('error', 'Unknown error')}")
        
        if amount > wallet_balance:
            await update.message.reply_text(
                f"Insufficient balance. Your balance is {wallet_balance:.8f} BTC.\n"
                f"Please enter a valid amount:"
            )
            return ENTERING_WITHDRAW_AMOUNT
        
        context.user_data['withdraw_amount'] = amount
        
        await update.message.reply_text(
            f"Amount to withdraw: {amount:.8f} BTC\n\n"
            f"Please enter the BTC wallet address where you want to receive the funds:"
        )
        
        return ENTERING_WALLET_ADDRESS
        
    except ValueError:
        await update.message.reply_text(
            "Invalid amount. Please enter a number:"
        )
        return ENTERING_WITHDRAW_AMOUNT


async def enter_wallet_address(update: Update, context: CallbackContext) -> int:
    address = update.message.text.strip()
    
    if len(address) < 26 or len(address) > 62:
        await update.message.reply_text(
            "Invalid BTC address format. Please enter a valid BTC address:"
        )
        return ENTERING_WALLET_ADDRESS
    
    context.user_data['withdraw_address'] = address
    
    wallet_id = context.user_data['withdraw_wallet_id']
    amount = context.user_data['withdraw_amount']
    
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key FROM wallets WHERE wallet_id = ?', (wallet_id,))
        wallet = cursor.fetchone()
        
        if not wallet:
            await update.message.reply_text("Wallet not found.")
            return ConversationHandler.END
        
        from_address, private_key = wallet
        
        result = btcwalletclient_wif.send_max_btc_auto(
            wif_private_key=private_key,
            destination_address=address
        )
        
        if result['success']:
            amount_sent = result['amount_sent']
            fee_paid = result['fee']
            
            cursor.execute(
                'UPDATE wallets SET balance = balance - ? WHERE wallet_id = ?',
                (amount_sent, wallet_id)
            )
            conn.commit()
            
            await update.message.reply_text(
                f"✅ Withdrawal successful!\n\n"
                f"Amount sent: {amount_sent:.8f} BTC\n"
                f"Transaction fee: {fee_paid:.8f} BTC\n"
                f"To address: {address}\n"
                f"Transaction ID: {result['txid']}\n\n"
                f"Your funds have been sent!"
            )
        else:
            await update.message.reply_text(
                f"❌ Withdrawal failed: {result.get('error', 'Unknown error')}"
            )
        
    except Exception as e:
        await update.message.reply_text(f"Error processing withdrawal: {str(e)}")
    finally:
        if conn:
            conn.close()
    
    return ConversationHandler.END


def send_btc_to_seller(buyer_wallet_id, seller_id, amount, fee_amount, fee_wallet_address):
    """
    Send BTC from buyer's wallet to seller's wallet and pay the fee.
    
    Args:
        buyer_wallet_id (str): Wallet ID of the buyer
        seller_id (int): User ID of the seller
        amount (float): Total amount to send (includes fee)
        fee_amount (float): Fee amount to deduct
        fee_wallet_address (str): Bitcoin address to send the fee to
    
    Returns:
        dict: {success: bool, error: str, seller_address: str, seller_amount: float, txid: str}
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key, wallet_type, address_type FROM wallets WHERE wallet_id = ?', (buyer_wallet_id,))
        buyer_wallet = cursor.fetchone()
        
        if not buyer_wallet:
            return {'success': False, 'error': 'Buyer wallet not found'}
        
        buyer_address, buyer_private_key, wallet_type, address_type = buyer_wallet
        
        cursor.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, 'BTC'))
        seller_wallet = cursor.fetchone()
        
        if not seller_wallet:
            return {'success': False, 'error': 'Seller wallet not found'}
        
        seller_address = seller_wallet[0]
        
        try:
            result = btcwalletclient_wif.send_batch_95_5_split(
                wif_private_key=buyer_private_key,
                seller_address=seller_address
            )
            
            if result['success']:
                return {
                    'success': True,
                    'seller_address': seller_address,
                    'seller_amount': result['seller_amount'],
                    'fee_amount': result['fee_wallet_amount'],
                    'transaction_fee': result['transaction_fee'],
                    'txid': result['txid']
                }
            else:
                return {'success': False, 'error': result.get('error', 'Transaction failed')}
                
        except Exception as tx_error:
            logger.error(f"Error creating BTC transaction: {tx_error}")
            return {'success': False, 'error': f'Transaction creation failed: {str(tx_error)}'}
            
    except sqlite3.Error as db_error:
        logger.error(f"Database error in send_btc_to_seller: {db_error}")
        return {'success': False, 'error': f'Database error: {str(db_error)}'}
    finally:
        if conn:
            conn.close()


def refund_btc_to_buyer(escrow_wallet_id, seller_id):
    """
    Send 50% of BTC to seller and 50% to fee wallet for disputed transactions.
    
    Args:
        escrow_wallet_id (str): Wallet ID of the escrow wallet
        seller_id (int): User ID of the seller
    
    Returns:
        dict: {success: bool, error: str, seller_address: str, seller_amount: float, txid: str}
    """
    conn = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()
        
        cursor.execute('SELECT address, private_key, wallet_type, address_type FROM wallets WHERE wallet_id = ?', (escrow_wallet_id,))
        escrow_wallet = cursor.fetchone()
        
        if not escrow_wallet:
            return {'success': False, 'error': 'Escrow wallet not found'}
        
        escrow_address, escrow_private_key, wallet_type, address_type = escrow_wallet
        
        cursor.execute('SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?', (seller_id, 'BTC'))
        seller_wallet = cursor.fetchone()
        
        if not seller_wallet:
            return {'success': False, 'error': 'Seller wallet not found'}
        
        seller_address = seller_wallet[0]
        
        try:
            result = btcwalletclient_wif.send_dispute_refund_50_50(
                wif_private_key=escrow_private_key,
                seller_address=seller_address
            )
            
            if result['success']:
                return {
                    'success': True,
                    'seller_address': seller_address,
                    'seller_amount': result['seller_amount'],
                    'fee_amount': result['fee_wallet_amount'],
                    'transaction_fee': result['transaction_fee'],
                    'txid': result['txid']
                }
            else:
                return {'success': False, 'error': result.get('error', 'Transaction failed')}
                
        except Exception as tx_error:
            logger.error(f"Error creating BTC refund transaction: {tx_error}")
            return {'success': False, 'error': f'Transaction creation failed: {str(tx_error)}'}
            
    except sqlite3.Error as db_error:
        logger.error(f"Database error in refund_btc_to_buyer: {db_error}")
        return {'success': False, 'error': f'Database error: {str(db_error)}'}
    finally:
        if conn:
            conn.close()


@with_auto_balance_refresh
async def release_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    check_and_update_expired_transactions()
    
    user = update.effective_user

    # Get transaction_id from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        # Find the most recent pending transaction where the user is the buyer
        cursor.execute(
            '''SELECT transaction_id
               FROM transactions
               WHERE buyer_id = ? AND status = 'PENDING'
               ORDER BY creation_date DESC LIMIT 1''',
            (user.id,)
        )
        result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in release_command: {e}")
    finally:
        if conn:
            conn.close()

    if not result:
        await update.message.reply_text(
            "No pending transaction found to release. Please check your transactions with /transactions command."
        )
        return

    transaction_id = result[0]

    await update.message.reply_text(
        f"Processing release for transaction ID: {transaction_id}"
    )
    transaction = get_transaction(transaction_id)

    if not transaction:
        await update.message.reply_text(f"Transaction {transaction_id} not found.")
        return

    seller_id = transaction[1]
    buyer_id = transaction[2]
    status = transaction[6]

    if buyer_id != user.id:
        await update.message.reply_text("You can only release funds for transactions where you are the buyer.")
        return

    if status == 'EXPIRED':
        await update.message.reply_text(
            "Cannot release funds. This transaction has expired (pending for more than 24 hours)."
        )
        return

    if status != 'PENDING':
        await update.message.reply_text(f"Cannot release funds. Transaction status is {status}.")
        return
    
    if is_transaction_expired(transaction):
        update_transaction_status(transaction_id, 'EXPIRED')
        await update.message.reply_text(
            "Cannot release funds. This transaction has expired (pending for more than 24 hours)."
        )
        return

    keyboard = [
        [
            InlineKeyboardButton("Yes, release funds", callback_data=f'release_{transaction_id}'),
            InlineKeyboardButton("No, cancel", callback_data='cancel_release')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"Are you sure you want to release funds? "
        f"This action cannot be undone.",
        reply_markup=reply_markup
    )


async def release_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith('release_'):
        transaction_id = data.split('_')[1]

        # Get transaction details
        transaction = get_transaction(transaction_id)
        if not transaction:
            await query.edit_message_text(f"Error: Transaction {transaction_id} not found.")
            return

        seller_id = transaction[1]
        buyer_id = transaction[2]
        crypto_type = transaction[3]
        amount = transaction[4]
        fee_amount = transaction[5]
        status = transaction[6]
        wallet_id = transaction[10]

        user_id = query.from_user.id
        
        if buyer_id != user_id:
            await query.edit_message_text("Only the buyer can release funds for this transaction.")
            return
        
        if status == 'EXPIRED':
            await query.edit_message_text(
                "Cannot release funds. This transaction has expired (pending for more than 24 hours)."
            )
            return
        
        if status != 'PENDING':
            await query.edit_message_text(f"Cannot release funds. Transaction status is {status}.")
            return
        
        if is_transaction_expired(transaction):
            update_transaction_status(transaction_id, 'EXPIRED')
            await query.edit_message_text(
                "Cannot release funds. This transaction has expired (pending for more than 24 hours)."
            )
            return

        conn = None
        try:
            conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT address FROM wallets WHERE user_id = ? AND crypto_type = ?',
                (seller_id, crypto_type)
            )
            seller_wallet = cursor.fetchone()
            
            if not seller_wallet:
                await query.edit_message_text(
                    f"Cannot release funds: Recipient does not have a {crypto_type} wallet address in the system. "
                    f"The recipient must create a wallet before you can release funds."
                )
                return
        except sqlite3.Error as e:
            logger.error(f"Database error checking seller wallet: {e}")
            await query.edit_message_text("An error occurred while checking recipient wallet status.")
            return
        finally:
            if conn:
                conn.close()

        if crypto_type == 'BTC':
            try:
                fee_wallet_address = "bc1q8mcfyyt0hdhsqvv4ly6czz52gyak5zaayw8qa5"
                
                result = send_btc_to_seller(wallet_id, seller_id, amount, fee_amount, fee_wallet_address)
                
                if result['success']:
                    update_transaction_status(transaction_id, 'COMPLETED')
                    txid = result.get('txid', 'pending')
                    seller_amount = result['seller_amount']
                    fee_wallet_amount = result['fee_amount']
                    transaction_fee = result.get('transaction_fee', 0)
                    
                    await query.edit_message_text(
                        f"✅ Funds released for transaction {transaction_id}\n"
                        f"Transaction ID: {txid}\n\n"
                        f"Seller receives (95%): {seller_amount:.8f} BTC\n"
                        f"Platform fee (5%): {fee_wallet_amount:.8f} BTC\n"
                        f"Network fee: {transaction_fee:.8f} BTC (250 satoshis)"
                    )
                else:
                    error_msg = result.get('error', 'Unknown error')
                    await query.edit_message_text(
                        f"⚠️ Error processing transaction {transaction_id}\n"
                        f"Error: {error_msg}"
                    )
            except Exception as e:
                logger.error(f"Error processing BTC transaction: {e}")
                await query.edit_message_text(
                    f"⚠️ Error processing transaction {transaction_id} "
                    f"Please contact support for assistance."
                )
        else:
            update_transaction_status(transaction_id, 'COMPLETED')
            await query.edit_message_text(
                f"✅ Funds released for transaction {transaction_id} "
                f"The seller has been notified and will receive the funds shortly."
            )
    elif data == 'cancel_release':
        await query.edit_message_text("Release cancelled.")


@with_auto_balance_refresh
async def dispute_command(update: Update, context: CallbackContext) -> int:
    await ensure_user_and_process_pending(update)
    
    check_and_update_expired_transactions()
    
    user = update.effective_user

    # Get transaction_id from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        # Find the most recent pending or disputed transaction where the user is the buyer
        cursor.execute(
            '''SELECT transaction_id
               FROM transactions
               WHERE buyer_id = ? AND status IN ('PENDING', 'DISPUTED')
               ORDER BY creation_date DESC LIMIT 1''',
            (user.id,)
        )
        result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in dispute_command: {e}")
    finally:
        if conn:
            conn.close()

    if not result:
        await update.message.reply_text(
            "No pending transaction found to dispute. Please check your transactions with /transactions command."
        )
        return ConversationHandler.END

    transaction_id = result[0]

    await update.message.reply_text(
        f"Processing dispute for transaction ID: {transaction_id}"
    )
    transaction = get_transaction(transaction_id)

    if not transaction:
        await update.message.reply_text(f"Transaction {transaction_id} not found.")
        return ConversationHandler.END

    seller_id = transaction[1]
    buyer_id = transaction[2]
    status = transaction[6]

    if buyer_id != user.id:
        await update.message.reply_text("Only buyers can dispute transactions.")
        return ConversationHandler.END

    if status == 'EXPIRED':
        await update.message.reply_text(
            "Cannot dispute transaction. This transaction has expired (pending for more than 24 hours)."
        )
        return ConversationHandler.END

    if status == 'DISPUTED':
        await update.message.reply_text(
            "This transaction has been disputed. Please allow our team 1-2 business day(s) to make a determination regarding this dispute."
        )
        return ConversationHandler.END

    if status != 'PENDING':
        await update.message.reply_text(f"Cannot dispute transaction. Status is {status}.")
        return ConversationHandler.END
    
    if is_transaction_expired(transaction):
        update_transaction_status(transaction_id, 'EXPIRED')
        await update.message.reply_text(
            "Cannot dispute transaction. This transaction has expired (pending for more than 24 hours)."
        )
        return ConversationHandler.END

    context.user_data['dispute_transaction_id'] = transaction_id

    await update.message.reply_text(
        f"You are opening a dispute for transaction {transaction_id}. "
        f"Please explain the reason for the dispute:"
    )

    return DISPUTE_REASON


async def dispute_reason(update: Update, context: CallbackContext) -> int:
    reason = update.message.text.strip()
    context.user_data['dispute_reason'] = reason

    await update.message.reply_text(
        "Please provide evidence to support your dispute claim. "
        "This could be screenshots, transaction hashes, or any other relevant information:"
    )

    return DISPUTE_EVIDENCE


async def dispute_evidence(update: Update, context: CallbackContext) -> int:
    evidence = update.message.text.strip()

    user = update.effective_user
    transaction_id = context.user_data['dispute_transaction_id']
    reason = context.user_data['dispute_reason']

    # Create dispute in database
    dispute_id = create_dispute(transaction_id, user.id, reason, evidence)

    # Escape the dispute ID for Markdown
    escaped_dispute_id = escape_markdown(dispute_id)
    await safe_send_text(
        update.message.reply_text,
        f"✅ Dispute opened successfully!\n\n"
        f"Dispute ID: `{escaped_dispute_id}`\n\n"
        f"Our team will review your case and contact you soon. "
        f"The transaction has been put on hold until the dispute is resolved.",
        parse_mode=ParseMode.MARKDOWN
    )

    return ConversationHandler.END


@with_auto_balance_refresh
async def language_command(update: Update, context: CallbackContext) -> None:
    await ensure_user_and_process_pending(update)
    
    keyboard = [
        [
            InlineKeyboardButton("English 🇬🇧", callback_data='lang_en'),
            InlineKeyboardButton("Español 🇪🇸", callback_data='lang_es')
        ],
        [
            InlineKeyboardButton("Русский 🇷🇺", callback_data='lang_ru'),
            InlineKeyboardButton("中文 🇨🇳", callback_data='lang_zh')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select your preferred language:",
        reply_markup=reply_markup
    )


async def language_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user
    data = query.data

    if data.startswith('lang_'):
        language_code = data.split('_')[1]

        # Update user's language preference in database
        conn = None
        try:
            conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
            cursor = conn.cursor()

            cursor.execute(
                'UPDATE users SET language_code = ? WHERE user_id = ?',
                (language_code, user.id)
            )

            conn.commit()
        except sqlite3.Error as e:
            print(f"Database error in language_callback: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

        language_names = {
            'en': 'English',
            'es': 'Spanish',
            'ru': 'Russian',
            'zh': 'Chinese'
        }

        await query.edit_message_text(
            f"Your language has been set to {language_names.get(language_code, language_code)}."
        )


async def create_escrow_group_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    group_data = context.user_data.get('create_group_data')
    if not group_data:
        await query.edit_message_text("Error: No transaction data found. Please initiate a transaction first.")
        return
    
    recipient = group_data['recipient']
    transaction_id = group_data['transaction_id']
    sender_name = group_data['sender_name']
    sender_username = group_data.get('sender_username')
    sender_id = group_data.get('sender_id')
    crypto_type = group_data['crypto_type']
    amount = group_data['amount']
    usd_amount = group_data['usd_amount']
    fee = group_data['fee']
    usd_fee = group_data['usd_fee']
    total = group_data['total']
    usd_total = group_data['usd_total']
    description = group_data['description']
    escaped_transaction_id = group_data['escaped_transaction_id']
    
    random_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    group_name = f"Escrow #{random_code}"
    
    bot_username = os.getenv('BOT_USERNAME', 'IncognitoEscrowBot')
    usernames_to_add = [recipient, f"@{bot_username}"]
    
    if sender_username:
        usernames_to_add.append(f"@{sender_username}" if not sender_username.startswith('@') else sender_username)
    elif sender_id:
        usernames_to_add.append(sender_id)
    
    await query.edit_message_text(
        f"Creating escrow group '{group_name}'...\nPlease wait..."
    )
    
    try:
        result = await create_supergroup_with_users(group_name, usernames_to_add, bot_username)
        
        if result['success']:
            telethon_group_id = result.get('telethon_group_id')
            
            if telethon_group_id and telethon_client:
                group_message = (
                    f"💰 **Escrow Transaction Created**\n\n"
                    f"**Sender:** {sender_name}\n"
                    f"**Recipient:** {recipient}\n\n"
                    f"**Transaction Details:**\n"
                    f"**Cryptocurrency:** {crypto_type}\n"
                    f"**Amount:** {amount:.8f} {crypto_type}\n"
                    f"**USD Value:** ${usd_amount:.2f} USD\n"
                    f"**Escrow fee (5%):** ${usd_fee:.2f} USD\n"
                    f"**Total:** ${usd_total:.2f} USD\n"
                    f"**Transaction ID:** `{escaped_transaction_id}`\n\n"
                    f"**Description:** {description}\n\n"
                    f"⚠️ **Action Required:**\n"
                    f"@{recipient.lstrip('@')}, you need to run /start with the bot and create a {crypto_type} wallet so that the funds can be released to it."
                )
                
                await telethon_client.send_message(
                    telethon_group_id,
                    group_message
                )
            
            message = f"✅ Escrow group '{group_name}' created successfully!"
            if result.get('group_link'):
                keyboard = [[InlineKeyboardButton("Open Escrow Group", url=result['group_link'])]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await query.edit_message_text(message)
        else:
            await query.edit_message_text(f"❌ Failed to create escrow group: {result['message']}")
    except Exception as e:
        error_msg = f"An error occurred while creating the group: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await query.edit_message_text(f"❌ Error: {error_msg}")


async def enter_m(update: Update, context: CallbackContext) -> int:
    try:
        m = int(update.message.text.strip())
        if m < 1 or m > 15:
            await update.message.reply_text("Please enter a number between 1 and 15:")
            return ENTERING_M

        context.user_data['m'] = m

        await update.message.reply_text(
            f"How many total keys should be in this wallet? (n in m-of-n)\n\n"
            f"Enter a number between {m} and 15:"
        )

        return ENTERING_N
    except ValueError:
        await update.message.reply_text("Please enter a valid number:")
        return ENTERING_M


async def enter_n(update: Update, context: CallbackContext) -> int:
    try:
        n = int(update.message.text.strip())
        m = context.user_data['m']

        if n < m or n > 15:
            await update.message.reply_text(f"Please enter a number between {m} and 15:")
            return ENTERING_N

        context.user_data['n'] = n

        # Ask if user wants to enter public keys or generate new ones
        keyboard = [
            [
                InlineKeyboardButton("Generate new keys", callback_data='generate_keys'),
                InlineKeyboardButton("Enter public keys", callback_data='enter_keys')
            ]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"You're creating a {m}-of-{n} multisig wallet.\n\n"
            f"Do you want to generate new keys or enter existing public keys?",
            reply_markup=reply_markup
        )

        return ENTERING_PUBLIC_KEYS
    except ValueError:
        await update.message.reply_text("Please enter a valid number:")
        return ENTERING_N


async def public_keys_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == 'generate_keys':
        # Generate new keys
        crypto_type = context.user_data['crypto_type']
        address_type = context.user_data['address_type']
        m = context.user_data['m']
        n = context.user_data['n']

        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(query.from_user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                query.edit_message_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationHandler.END
        else:
            # Create multisig wallet
            wallet_id, address = create_wallet(
                query.from_user.id,
                crypto_type,
                wallet_type='multisig',
                address_type=address_type,
                m=m,
                n=n
            )

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            await safe_send_text(
                query.edit_message_text,
                f"✅ Your new {crypto_type} multisig wallet has been created!\n\n"
                f"Address: `{escaped_address}`\n\n"
                f"This is a {m}-of-{n} multisig wallet with {address_type} address format.\n"
                f"The private keys are securely stored in the database.",
                parse_mode=ParseMode.MARKDOWN
            )

        return ConversationHandler.END
    elif data == 'enter_keys':
        await query.edit_message_text(
            f"Please enter {context.user_data['n']} public keys, one per line:"
        )

        return CONFIRMING_WALLET

    return ConversationHandler.END


async def confirm_wallet(update: Update, context: CallbackContext) -> int:
    public_keys_text = update.message.text.strip()
    public_keys = [key.strip() for key in public_keys_text.split('\n')]

    crypto_type = context.user_data['crypto_type']
    address_type = context.user_data['address_type']
    m = context.user_data['m']
    n = context.user_data['n']

    if len(public_keys) != n:
        await update.message.reply_text(
            f"You entered {len(public_keys)} keys, but {n} are required. Please try again:"
        )
        return CONFIRMING_WALLET

    try:
        # Check if user already has a wallet for this cryptocurrency
        existing_wallets = get_user_wallets(update.effective_user.id)
        has_wallet = any(wallet[1] == crypto_type for wallet in existing_wallets)

        if has_wallet:
            # User already has a wallet for this cryptocurrency
            await safe_send_text(
                update.message.reply_text,
                f"⚠️ You already have a {crypto_type} wallet. Only one wallet per cryptocurrency is allowed.\n\n"
                f"Please use your existing wallet or choose a different cryptocurrency.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            # Create multisig wallet with provided public keys
            wallet_id, address = create_wallet(
                update.effective_user.id,
                crypto_type,
                wallet_type='multisig',
                address_type=address_type,
                m=m,
                n=n,
                public_keys=public_keys
            )

            # Escape the address for Markdown
            escaped_address = escape_markdown(address)
            await safe_send_text(
                update.message.reply_text,
                f"✅ Your new {crypto_type} multisig wallet has been created!\n\n"
                f"Address: `{escaped_address}`\n\n"
                f"This is a {m}-of-{n} multisig wallet with {address_type} address format.",
                parse_mode=ParseMode.MARKDOWN
            )

        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(
            f"Error creating wallet: {str(e)}\n\n"
            f"Please check your public keys and try again:"
        )
        return CONFIRMING_WALLET


async def cancel(update: Update, context: CallbackContext) -> int:
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors in the telegram bot."""
    print(f"An error occurred: {context.error}")

    # Handle entity parsing errors
    if isinstance(context.error, BadRequest) and "entity" in str(context.error).lower():
        print(f"Entity parsing error: {context.error}")
        if update and update.effective_message:
            try:
                # Try to send a message without formatting

                await update.effective_message.reply_text(
                    "Sorry, there was an error processing your message. "
                    "The message has been sent without formatting."
                )
            except Exception as e:
                print(f"Error sending error message: {e}")
    elif update:
        # For other errors, notify the user that something went wrong
        try:
            await update.effective_message.reply_text(
                "Sorry, an error occurred while processing your request."
            )
        except Exception as e:
            print(f"Error sending error message: {e}")
    # You can add more error handling logic here
    return


@with_auto_balance_refresh
async def sign_transaction_command(update: Update, context: CallbackContext) -> int:
    """Command to sign a multisig transaction"""
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    # Get wallet_id and txid from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        # Look for a wallet with txid
        cursor.execute(
            '''SELECT wallet_id, txid
               FROM wallets
               WHERE user_id = ? AND txid IS NOT NULL
               ORDER BY wallet_id DESC LIMIT 1''',
            (user.id,)
        )
        result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in sign_transaction_command (1): {e}")
    finally:
        if conn:
            conn.close()

    if not result:
        await update.message.reply_text(
            "No pending transaction found to sign. Please create a transaction first."
        )
        return ConversationHandler.END

    wallet_id = result[0]
    txid = result[1]

    await update.message.reply_text(
        f"Signing transaction with wallet ID: {wallet_id} and transaction ID: {txid}"
    )

    # Get wallet from database
    conn = None
    wallet = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        cursor.execute(
            '''SELECT wallet_id, crypto_type, address, wallet_type, private_key, required_sigs, total_keys
               FROM wallets WHERE wallet_id = ? AND user_id = ?''',
            (wallet_id, user.id)
        )
        wallet = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in sign_transaction_command (2): {e}")
    finally:
        if conn:
            conn.close()

    if not wallet:
        await update.message.reply_text(f"Wallet {wallet_id} not found or you don't have access to it.")
        return ConversationHandler.END

    wallet_type = wallet[3]

    if wallet_type != 'multisig':
        await update.message.reply_text("This command is only for multisig wallets.")
        return ConversationHandler.END

    try:
        # Get private keys from wallet
        private_key = wallet[4]
        private_keys = json.loads(private_key)

        # Sign transaction
        crypto_type = wallet[1]
        if crypto_type.upper() == 'BTC':
            # Extract wallet name from database
            wallet_name = f"user_{user.id}_{crypto_type.lower()}_{wallet_id}"

            # Sign transaction using TransactionManager
            signed_tx = TransactionManager.sign_transaction(wallet_name, txid, private_keys)

            if signed_tx:
                # Store the signed transaction hex in the database
                conn = None
                try:
                    conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
                    cursor = conn.cursor()

                    # Update the wallet with the signed transaction hex
                    cursor.execute(
                        'UPDATE wallets SET tx_hex = ? WHERE wallet_id = ?',
                        (signed_tx, wallet_id)
                    )

                    # Check if there's a transaction associated with this user
                    cursor.execute(
                        'SELECT transaction_id FROM transactions WHERE seller_id = ? OR buyer_id = ? ORDER BY creation_date DESC LIMIT 1',
                        (user.id, user.id)
                    )
                    transaction = cursor.fetchone()

                    if transaction:
                        # Update the transaction with the signed transaction hex
                        cursor.execute(
                            'UPDATE transactions SET tx_hex = ? WHERE transaction_id = ?',
                            (signed_tx, transaction[0])
                        )

                    conn.commit()
                except sqlite3.Error as e:
                    print(f"Database error in sign_transaction_command (3): {e}")
                    if conn:
                        conn.rollback()
                finally:
                    if conn:
                        conn.close()

                # Escape the transaction hex for Markdown
                escaped_tx = escape_markdown(signed_tx[:64])
                await safe_send_text(
                    update.message.reply_text,
                    f"Transaction signed successfully!\n\n"
                    f"Signed transaction: `{escaped_tx}...`\n\n"
                    f"You can broadcast this transaction using /broadcast <tx_hex>",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("Failed to sign transaction. Please check the transaction ID.")
        else:
            await update.message.reply_text(f"Signing {crypto_type} multisig transactions is not supported yet.")
    except Exception as e:
        await update.message.reply_text(f"Error signing transaction: {str(e)}")

    return ConversationHandler.END


@with_auto_balance_refresh
async def broadcast_transaction_command(update: Update, context: CallbackContext) -> None:
    """Command to broadcast a signed transaction"""
    await ensure_user_and_process_pending(update)
    
    user = update.effective_user

    # Get tx_hex from the database
    conn = None
    result = None
    try:
        conn = sqlite3.connect('escrow_bot.db', timeout=20.0)
        cursor = conn.cursor()

        # First, try to find a transaction associated with the user that has a tx_hex
        cursor.execute(
            '''SELECT tx_hex
               FROM transactions
               WHERE (buyer_id = ? OR seller_id = ?) AND tx_hex IS NOT NULL
               ORDER BY creation_date DESC LIMIT 1''',
            (user.id, user.id)
        )
        result = cursor.fetchone()

        if not result:
            # If no transaction found, try to find a wallet with tx_hex
            cursor.execute(
                '''SELECT tx_hex
                   FROM wallets
                   WHERE user_id = ? AND tx_hex IS NOT NULL
                   ORDER BY wallet_id DESC LIMIT 1''',
                (user.id,)
            )
            result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Database error in broadcast_transaction_command (1): {e}")
    finally:
        if conn:
            conn.close()

    if not result or not result[0]:
        await update.message.reply_text(
            "No signed transaction found to broadcast. Please sign a transaction first with /sign command."
        )
        return

    tx_hex = result[0]

    await update.message.reply_text(
        f"Broadcasting transaction with hex: {tx_hex[:32]}..."
    )

    try:
        # Broadcast transaction using TransactionManager
        txid = TransactionManager.broadcast_transaction(tx_hex)

        if txid:
            # Store tx_hex and txid in the database
            # First, check if this is for a wallet or a transaction
            conn = sqlite3.connect('escrow_bot.db')
            cursor = conn.cursor()

            # Try to find a matching wallet
            cursor.execute('SELECT wallet_id FROM wallets WHERE user_id = ?', (update.effective_user.id,))
            wallets = cursor.fetchall()

            if wallets:
                # Update the first wallet found (in a real implementation, you would specify which wallet)
                cursor.execute(
                    'UPDATE wallets SET tx_hex = ?, txid = ? WHERE wallet_id = ?',
                    (tx_hex, txid, wallets[0][0])
                )

            # Try to find a matching transaction
            cursor.execute('SELECT transaction_id FROM transactions WHERE buyer_id = ? OR seller_id = ?',
                           (update.effective_user.id, update.effective_user.id))
            transactions = cursor.fetchall()

            if transactions:
                # Update the first transaction found (in a real implementation, you would specify which transaction)
                cursor.execute(
                    'UPDATE transactions SET tx_hex = ?, txid = ? WHERE transaction_id = ?',
                    (tx_hex, txid, transactions[0][0])
                )

            conn.commit()
            conn.close()

            # Escape the transaction ID for Markdown
            escaped_txid = escape_markdown(txid)
            await safe_send_text(
                update.message.reply_text,
                f"Transaction broadcast successfully!\n\n"
                f"Transaction ID: `{escaped_txid}`",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text("Failed to broadcast transaction. Please check the transaction hex.")
    except Exception as e:
        await update.message.reply_text(f"Error broadcasting transaction: {str(e)}")


async def handle_keyboard_buttons(update: Update, context: CallbackContext) -> None:
    """Handle keyboard button presses from the main menu."""
    await ensure_user_and_process_pending(update)
    
    global app
    text = update.message.text

    if text == "My Account":
        # Handle My Account button - show nested menu
        account_keyboard = [
            [KeyboardButton("Escrow Wallet"), KeyboardButton("Start Trade")],
            [KeyboardButton("Release Funds"), KeyboardButton("File Dispute")],
            [KeyboardButton("Back to Main Menu 🔙")]
        ]
        reply_markup = ReplyKeyboardMarkup(account_keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "My Account Options:",
            reply_markup=reply_markup
        )
    elif text == "Transaction History":
        # Handle Transaction History button - redirect to transactions command
        await transactions_command(update, context)
    elif text == "Language":
        # Handle Language button - redirect to language command
        await language_command(update, context)
    elif text == "Help":
        # Handle Help button - redirect to help command
        await help_command(update, context)
    elif text == "Withdraw Funds":
        # Handle Withdraw Funds button - create a command update to trigger the conversation handler
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/withdraw",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 9}]
                    }
                },
                context.bot
            )
        )
    elif text == "Escrow Wallet":
        # Handle Create Wallet button - redirect to wallet command
        await wallet_command(update, context)
    elif text == "Start Trade":
        # Handle Deposit Funds button - create a command update to trigger the conversation handler
        # This ensures the conversation state is properly set up
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/deposit",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 8}]
                    }
                },
                context.bot
            )
        )
    elif text == "Release Funds":
        # Handle Release Funds button - redirect to release command
        await release_command(update, context)
    elif text == "File Dispute":
        # Handle File Dispute button - create a command update to trigger the conversation handler
        # This ensures the conversation state is properly set up
        await app.process_update(
            Update.de_json(
                {
                    "update_id": update.update_id,
                    "message": {
                        "message_id": update.message.message_id,
                        "from": update.message.from_user.to_dict(),
                        "chat": update.message.chat.to_dict(),
                        "date": update.message.date.timestamp(),
                        "text": "/dispute",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 8}]
                    }
                },
                context.bot
            )
        )
    elif text == "Back to Main Menu 🔙":
        # Return to main menu
        keyboard = [
            [KeyboardButton("My Account"), KeyboardButton("Transaction History")],
            [KeyboardButton("Language"), KeyboardButton("Help")],
            [KeyboardButton("Withdraw Funds")]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "Main Menu:",
            reply_markup=reply_markup
        )


async def initialize_telethon_client():
    """
    Initialize and authenticate the Telethon user client.
    Handles first-time login flow and session management.
    """
    global telethon_client
    
    try:
        api_id = int(os.getenv('API_ID', '0'))
        api_hash = os.getenv('API_HASH', '')
        
        if not api_id or not api_hash or api_id == 0:
            logger.error("API_ID or API_HASH not properly configured in .env")
            return False
        
        telethon_client = TelegramClient('user_session', api_id, api_hash)
        await telethon_client.start()
        
        me = await telethon_client.get_me()
        logger.info(f"Telethon client initialized. Logged in as: {me.first_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing Telethon client: {e}")
        return False


async def create_supergroup_with_users(group_name, usernames_to_add, bot_username):
    """
    Create a supergroup and add specified users to it.
    
    Args:
        group_name (str): Name of the supergroup to create
        usernames_to_add (list): List of Telegram usernames to add
        bot_username (str): The bot's username to add to the group
        
    Returns:
        dict: Contains 'success', 'group_id', 'group_link', and 'message'
    """
    
    if not telethon_client:
        return {
            'success': False,
            'message': 'User client not initialized. Please restart the bot.'
        }
    
    try:
        result = await telethon_client(CreateChannelRequest(
            title=group_name,
            about='Group created by bot',
            megagroup=True
        ))
        
        telethon_group_id = result.chats[0].id
        bot_api_group_id = -1000000000000 - telethon_group_id
        logger.info(f"Supergroup created: {group_name} (Telethon ID: {telethon_group_id}, Bot API ID: {bot_api_group_id})")
        
        users_added = []
        users_failed = []
        
        all_usernames = list(set(usernames_to_add + [bot_username]))
        
        for username in all_usernames:
            try:
                if isinstance(username, int):
                    input_entity = await telethon_client.get_input_entity(username)
                    user_identifier = str(username)
                else:
                    clean_username = username.lstrip('@')
                    input_entity = await telethon_client.get_input_entity(clean_username)
                    user_identifier = clean_username
                
                await telethon_client(InviteToChannelRequest(
                    channel=telethon_group_id,
                    users=[input_entity]
                ))
                
                users_added.append(user_identifier)
                logger.info(f"Added user {user_identifier} to group {telethon_group_id}")
                
            except (UsernameNotOccupiedError, UsernameInvalidError):
                users_failed.append((username, 'Username not found'))
                logger.warning(f"Username {username} not found")
            except FloodError as e:
                users_failed.append((username, f'Rate limited: {e}'))
                logger.warning(f"Rate limited while adding {username}: {e}")
            except Exception as e:
                users_failed.append((username, str(e)))
                logger.warning(f"Error adding {username}: {e}")
        
        try:
            invite = await telethon_client(ExportChatInviteRequest(telethon_group_id))
            group_link = invite.link
        except Exception as e:
            logger.warning(f"Error exporting invite link: {e}")
            group_link = f"https://t.me/c/{telethon_group_id}"
        
        message = f"Supergroup '{group_name}' created successfully!\n"
        message += f"Group ID: {bot_api_group_id}\n"
        message += f"Group Link: {group_link}\n"
        message += f"Users added: {len(users_added)}/{len(all_usernames)}\n"
        
        if users_failed:
            message += f"\nFailed to add: {len(users_failed)} user(s)\n"
            for username, reason in users_failed:
                message += f"  - {username}: {reason}\n"
        
        return {
            'success': True,
            'group_id': bot_api_group_id,
            'telethon_group_id': telethon_group_id,
            'group_link': group_link,
            'users_added': users_added,
            'users_failed': users_failed,
            'message': message
        }
        
    except Exception as e:
        error_message = f"Error creating supergroup: {str(e)}"
        logger.error(error_message)
        return {
            'success': False,
            'message': error_message
        }


@with_auto_balance_refresh
async def create_group_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handle the /creategroup command.
    Format: /creategroup <group_name> <username1> <username2> ...
    """
    await ensure_user_and_process_pending(update)
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "Usage: /creategroup <group_name> <username1> <username2> ...\n"
            "Example: /creategroup MyGroup @user1 @user2"
        )
        return
    
    try:
        group_name = context.args[0]
        usernames = context.args[1:] if len(context.args) > 1 else []
        
        if not group_name or group_name.strip() == '':
            await update.message.reply_text("Group name cannot be empty.")
            return
        
        await update.message.reply_text(
            f"Creating supergroup '{group_name}'...\nAdding {len(usernames)} user(s)...\nPlease wait..."
        )
        
        bot_username = os.getenv('BOT_USERNAME', '')
        result = await create_supergroup_with_users(group_name, usernames, bot_username)
        
        if result['success']:
            await update.message.reply_text(result['message'])
        else:
            await update.message.reply_text(f"Error: {result['message']}")
            
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await update.message.reply_text(f"Error: {error_msg}")


def main() -> None:
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Set up database
    setup_database()
    migrate_wallets_table()
    migrate_transactions_table()
    init_crypto_prices_table()
    
    # Initialize Telethon client
    bot_token = os.getenv('BOT_TOKEN', '8193003920:AAGPHNfVauCYHWFEIrh9reTlwpJ6jUtwLUY')
    try:
        loop.run_until_complete(initialize_telethon_client())
    except Exception as e:
        logger.warning(f"Could not initialize Telethon client: {e}. /creategroup command will not work.")

    # Create the Application and pass it your bot's token
    application = Application.builder().token(bot_token).build()

    # Store application in a global variable for access in handlers
    global app
    app = application

    # Add conversation handler for deposit command
    deposit_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('deposit', deposit_command)],
        states={
            SELECTING_CRYPTO: [CallbackQueryHandler(select_crypto, pattern='^deposit_')],
            ENTERING_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_amount)],
            ENTERING_RECIPIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_recipient)],
            CONFIRMING_TRANSACTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_transaction)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Add conversation handler for dispute command
    dispute_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('dispute', dispute_command)],
        states={
            DISPUTE_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, dispute_reason)],
            DISPUTE_EVIDENCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, dispute_evidence)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Add conversation handler for multisig wallet creation
    multisig_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(wallet_callback, pattern='^create_multisig_'),
            CallbackQueryHandler(wallet_callback, pattern='^address_type_')
        ],
        states={
            SELECTING_ADDRESS_TYPE: [CallbackQueryHandler(wallet_callback, pattern='^address_type_')],
            ENTERING_M: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_m)],
            ENTERING_N: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_n)],
            ENTERING_PUBLIC_KEYS: [CallbackQueryHandler(public_keys_callback, pattern='^(generate_keys|enter_keys)')],
            CONFIRMING_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_wallet)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Add conversation handler for withdrawal
    withdraw_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('withdraw', withdraw_command)],
        states={
            SELECTING_WITHDRAW_WALLET: [CallbackQueryHandler(select_withdraw_wallet, pattern='^withdraw_')],
            ENTERING_WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_withdraw_amount)],
            ENTERING_WALLET_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_wallet_address)]
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    # Register command handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('wallet', wallet_command))
    application.add_handler(CommandHandler('transactions', transactions_command))
    application.add_handler(CommandHandler('release', release_command))
    application.add_handler(CommandHandler('language', language_command))
    application.add_handler(CommandHandler('sign', sign_transaction_command))
    application.add_handler(CommandHandler('broadcast', broadcast_transaction_command))
    application.add_handler(CommandHandler('creategroup', create_group_command))

    # Register message handler for keyboard buttons
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND &
        filters.Regex('^(My Account|Transaction History|Language|Help|Withdraw Funds|Escrow Wallet|Start Trade|Release Funds|File Dispute|Back to Main Menu 🔙)$'),
        handle_keyboard_buttons
    ), group=1)

    # Register conversation handlers
    application.add_handler(deposit_conv_handler)
    application.add_handler(dispute_conv_handler)
    application.add_handler(multisig_conv_handler)
    application.add_handler(withdraw_conv_handler)

    # Register callback query handlers
    application.add_handler(
        CallbackQueryHandler(wallet_callback, pattern='^(create_wallet_|create_new_wallet|refresh_balances|confirm_wallet_BTC_segwit)'))
    application.add_handler(
        CallbackQueryHandler(transaction_callback, pattern='^(confirm_transaction|cancel_transaction)'))
    application.add_handler(CallbackQueryHandler(release_callback, pattern='^(release_|cancel_release)'))
    application.add_handler(CallbackQueryHandler(language_callback, pattern='^lang_'))
    application.add_handler(CallbackQueryHandler(create_escrow_group_callback, pattern='^create_escrow_group$'))

    # Add error handler
    application.add_error_handler(error_handler)

    # Start the Bot
    application.run_polling()


if __name__ == '__main__':
    main()
