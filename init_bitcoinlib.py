"""
Initialization script for bitcoinlib database.
This script should be called before importing bitcoinlib to ensure the database is properly set up.
"""

import os
import sqlite3
import logging
import sys

logger = logging.getLogger(__name__)

def get_bitcoinlib_db_path():
    """Get the path to the bitcoinlib database."""
    try:
        import bitcoinlib.db
        db_uri = bitcoinlib.db.DEFAULT_DATABASE
        
        if db_uri.startswith('sqlite:///'):
            return db_uri.replace('sqlite:///', '')
        return db_uri
    except Exception as e:
        logger.error(f"Could not determine bitcoinlib database path: {e}")
        return None

def fix_bitcoinlib_database():
    """
    Fix the bitcoinlib database version mismatch and duplicate column issue.
    This function updates the database version to match the library version without
    attempting to add duplicate columns.
    """
    try:
        db_path = get_bitcoinlib_db_path()
        
        if not db_path:
            logger.warning("Could not determine database path")
            return False
        
        if not os.path.exists(db_path):
            logger.info(f"Database does not exist yet at {db_path} - will be created on first use")
            return True
        
        logger.info(f"Checking bitcoinlib database at: {db_path}")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='db_variables'")
        if cursor.fetchone():
            cursor.execute("SELECT value FROM db_variables WHERE variable='version'")
            result = cursor.fetchone()
            
            if result:
                current_version = result[0]
                logger.info(f"Current database version: {current_version}")
                
                try:
                    import bitcoinlib
                    lib_version = '0.7.6'
                    
                    if current_version != lib_version:
                        logger.info(f"Updating database version from {current_version} to {lib_version}")
                        cursor.execute("UPDATE db_variables SET value=? WHERE variable='version'", (lib_version,))
                        conn.commit()
                        logger.info("Database version updated successfully")
                except Exception as e:
                    logger.warning(f"Could not get bitcoinlib version: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error fixing bitcoinlib database: {e}")
        return False

def delete_bitcoinlib_database():
    """
    Delete the bitcoinlib database to force a fresh start.
    WARNING: This will remove all stored wallet information!
    """
    try:
        db_path = get_bitcoinlib_db_path()
        
        if not db_path or not os.path.exists(db_path):
            logger.info("No database to delete")
            return True
        
        os.remove(db_path)
        logger.info(f"Deleted bitcoinlib database at {db_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error deleting bitcoinlib database: {e}")
        return False

def suppress_bitcoinlib_warnings():
    """Suppress bitcoinlib database version warnings."""
    logging.getLogger('bitcoinlib.db').setLevel(logging.ERROR)
    logging.getLogger('bitcoinlib').setLevel(logging.ERROR)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'fix':
            fix_bitcoinlib_database()
        elif sys.argv[1] == 'delete':
            response = input("This will delete all wallet data. Are you sure? (yes/no): ")
            if response.lower() == 'yes':
                delete_bitcoinlib_database()
        elif sys.argv[1] == 'path':
            print(get_bitcoinlib_db_path())
    else:
        print("Usage:")
        print("  python init_bitcoinlib.py fix    - Fix database version mismatch")
        print("  python init_bitcoinlib.py delete - Delete database (WARNING: removes all wallet data)")
        print("  python init_bitcoinlib.py path   - Show database path")
