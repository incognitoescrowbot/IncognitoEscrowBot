"""
Compatibility layer for the crypto-utils package.

This module provides a compatibility layer for the crypto-utils package,
which is installed as 'crypto-utils' but imported as 'crypto_utils' in the code.
"""

import sys
import importlib
import importlib.util
import os
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Constants that might be used in the original package
ADDRESS_TYPE_LEGACY = "legacy"
ADDRESS_TYPE_SEGWIT = "segwit"
ADDRESS_TYPE_NATIVE_SEGWIT = "native-segwit"

# Try to find and import the actual package
try:
    # Try to import using importlib
    spec = importlib.util.find_spec('crypto-utils')
    if spec is not None:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # If successful, get all the attributes from the module
        for attr_name in dir(module):
            if not attr_name.startswith('_'):  # Skip private attributes
                globals()[attr_name] = getattr(module, attr_name)

        logger.info("Successfully imported crypto-utils package")
    else:
        # Try to find the package in site-packages
        site_packages = next((p for p in sys.path if 'site-packages' in p), None)
        if site_packages:
            # Look for any directory that might contain the package
            for root, dirs, files in os.walk(site_packages):
                if 'crypto-utils' in dirs or 'crypto_utils' in dirs:
                    package_dir = os.path.join(root, 'crypto-utils' if 'crypto-utils' in dirs else 'crypto_utils')
                    if package_dir not in sys.path:
                        sys.path.insert(0, package_dir)
                    break

        # Try importing again
        try:
            module = __import__('crypto-utils')
            for attr_name in dir(module):
                if not attr_name.startswith('_'):
                    globals()[attr_name] = getattr(module, attr_name)
            logger.info("Successfully imported crypto-utils package after path manipulation")
        except ImportError:
            logger.warning("Could not import crypto-utils package, using stub implementations")

            # Define stub classes and functions
            class KeyManager:
                def __init__(self, *args, **kwargs):
                    raise NotImplementedError("KeyManager is not available. Please install the crypto-utils package correctly.")

            class WalletManager:
                def __init__(self, *args, **kwargs):
                    raise NotImplementedError("WalletManager is not available. Please install the crypto-utils package correctly.")

                @staticmethod
                def create_single_sig_wallet(wallet_name, address_type):
                    """
                    Create a single-signature wallet

                    Args:
                        wallet_name (str): Name of the wallet
                        address_type (str): Type of address (legacy, segwit, native-segwit)

                    Returns:
                        tuple: (wallet_name, address, private_key)
                    """
                    import bitcoinlib
                    from bitcoinlib.wallets import Wallet
                    from bitcoinlib.keys import Key
                    import hashlib
                    import uuid

                    # Map address_type to bitcoinlib witness_type
                    # Both "segwit" and "native-segwit" refer to native segwit (bc1 addresses)
                    if address_type == ADDRESS_TYPE_LEGACY:
                        witness_type = 'legacy'
                    elif address_type == ADDRESS_TYPE_SEGWIT or address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                        witness_type = 'segwit'
                    else:
                        witness_type = 'segwit'

                    # Create a wallet using bitcoinlib with the correct witness type
                    wallet = Wallet.create(wallet_name, keys=None, network='bitcoin', witness_type=witness_type)

                    # Get the first key and address
                    key = wallet.get_key()
                    address = key.address

                    # Get the private key - convert from HD key to standard WIF format
                    priv_key_hex = key.key_private.hex()
                    std_key = Key(bytes.fromhex(priv_key_hex), network='bitcoin')
                    private_key = std_key.wif()

                    return wallet_name, address, private_key

                @staticmethod
                def create_multisig_wallet(wallet_name, m, n, public_keys=None, address_type=ADDRESS_TYPE_SEGWIT):
                    """
                    Create a multi-signature wallet

                    Args:
                        wallet_name (str): Name of the wallet
                        m (int): Number of signatures required
                        n (int): Total number of keys
                        public_keys (list, optional): List of public keys. If None, new keys will be generated.
                        address_type (str): Type of address (legacy, segwit, native-segwit)

                    Returns:
                        tuple: (wallet_name, address, private_keys)
                    """
                    import bitcoinlib
                    from bitcoinlib.wallets import Wallet
                    import hashlib
                    import uuid

                    # If public keys are not provided, create a new multisig wallet with new keys
                    if public_keys is None:
                        # Create a multisig wallet with new keys
                        wallet = Wallet.create(
                            wallet_name, 
                            keys=n,
                            network='bitcoin',
                            multisig=True,
                            sigs_required=m
                        )

                        # Get all keys
                        keys = wallet.keys()
                        private_keys = [key.wif for key in keys]

                        # Get the first address based on address_type
                        try:
                            key = wallet.get_key()
                            if address_type == ADDRESS_TYPE_LEGACY:
                                address = key.address
                            elif address_type == ADDRESS_TYPE_SEGWIT:
                                # Try to get segwit address, fall back to regular address if not available
                                if hasattr(key, 'address_segwit'):
                                    address = key.address_segwit
                                else:
                                    address = key.address
                            elif address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                                # Try to get native segwit address, fall back to regular address if not available
                                if hasattr(key, 'address_segwit_p2sh'):
                                    address = key.address_segwit_p2sh
                                else:
                                    address = key.address
                            else:
                                address = key.address
                        except Exception as e:
                            # Fall back to regular address if any error occurs
                            address = wallet.get_key().address
                    else:
                        # Create a multisig wallet with provided public keys
                        wallet = Wallet.create(
                            wallet_name,
                            keys=public_keys,
                            network='bitcoin',
                            multisig=True,
                            sigs_required=m
                        )

                        # Since we're using provided public keys, we don't have private keys
                        private_keys = []

                        # Get the first address based on address_type
                        try:
                            key = wallet.get_key()
                            if address_type == ADDRESS_TYPE_LEGACY:
                                address = key.address
                            elif address_type == ADDRESS_TYPE_SEGWIT:
                                # Try to get segwit address, fall back to regular address if not available
                                if hasattr(key, 'address_segwit'):
                                    address = key.address_segwit
                                else:
                                    address = key.address
                            elif address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                                # Try to get native segwit address, fall back to regular address if not available
                                if hasattr(key, 'address_segwit_p2sh'):
                                    address = key.address_segwit_p2sh
                                else:
                                    address = key.address
                            else:
                                address = key.address
                        except Exception as e:
                            # Fall back to regular address if any error occurs
                            address = wallet.get_key().address

                    return wallet_name, address, private_keys

            class TransactionManager:
                def __init__(self, *args, **kwargs):
                    pass

                def create_and_send_transaction(self, from_address, to_address, amount_btc, private_key_wif, address_type='segwit'):
                    """
                    Create and send a Bitcoin transaction

                    Args:
                        from_address (str): Source Bitcoin address
                        to_address (str): Destination Bitcoin address
                        amount_btc (float): Amount in BTC to send
                        private_key_wif (str): Private key in WIF format

                    Returns:
                        dict: {'success': bool, 'error': str, 'txid': str}
                    """
                    import bitcoinlib
                    from bitcoinlib.wallets import Wallet, wallet_delete_if_exists
                    from bitcoinlib.keys import Key
                    import uuid
                    
                    try:
                        wallet_name = f"temp_wallet_{uuid.uuid4().hex[:8]}"
                        
                        try:
                            wallet_delete_if_exists(wallet_name)
                        except:
                            pass
                        
                        try:
                            key = Key(private_key_wif, network='bitcoin')
                            wallet = Wallet.create(wallet_name, keys=[key], network='bitcoin')
                            
                            # Get the actual wallet address
                            wallet_addr = wallet.addresslist()[0] if wallet.addresslist() else from_address
                            logger.info(f"Wallet address: {wallet_addr}, expected from_address: {from_address}")
                            
                            # Sync wallet with blockchain to discover UTXOs using Service API
                            try:
                                from bitcoinlib.services.services import Service
                                logger.info(f"Starting wallet scan for {wallet_addr}...")
                                
                                service = Service(network='bitcoin')
                                utxos_from_service = service.getutxos(wallet_addr)
                                
                                if utxos_from_service:
                                    logger.info(f"Found {len(utxos_from_service)} UTXOs from blockchain service")
                                    for utxo in utxos_from_service:
                                        try:
                                            wallet.utxo_add(
                                                address=from_address,
                                                value=utxo['value'],
                                                txid=utxo['txid'],
                                                output_n=utxo['output_n'],
                                                confirmations=utxo.get('confirmations', 1)
                                            )
                                        except Exception as utxo_err:
                                            logger.warning(f"Could not add UTXO: {utxo_err}")
                                    
                                    wallet.utxos_update()
                                else:
                                    logger.warning(f"No UTXOs found for {from_address} from blockchain service")
                                
                                logger.info(f"Wallet scan completed for {from_address}")
                            except Exception as scan_error:
                                logger.error(f"Wallet scan failed for {from_address}: {scan_error}")
                                import traceback
                                traceback.print_exc()
                            
                            # Check wallet balance before attempting transaction
                            current_balance = wallet.balance()
                            amount_satoshi = int(amount_btc * 100000000)
                            
                            if current_balance < amount_satoshi:
                                btc_needed = amount_satoshi / 100000000
                                btc_available = current_balance / 100000000
                                error_msg = (
                                    f"Insufficient funds: wallet {from_address} has {btc_available:.8f} BTC ({current_balance} satoshis), "
                                    f"need {btc_needed:.8f} BTC ({amount_satoshi} satoshis). "
                                    f"Please fund this address before attempting the transaction."
                                )
                                logger.error(error_msg)
                                raise Exception(error_msg)
                            
                            # Check for available UTXOs
                            utxos = wallet.utxos()
                            if not utxos:
                                error_msg = (
                                    f"No unspent transaction outputs found for address {from_address}. "
                                    f"The address may not have been funded yet, or the blockchain sync may be incomplete."
                                )
                                logger.error(error_msg)
                                raise Exception(error_msg)
                            
                            tx = wallet.send_to(to_address, amount_satoshi, fee=None)
                            
                            try:
                                wallet_delete_if_exists(wallet_name)
                            except:
                                pass
                            
                            return {
                                'success': True,
                                'txid': tx.hash,
                                'error': None
                            }
                        except Exception as e:
                            try:
                                wallet_delete_if_exists(wallet_name)
                            except:
                                pass
                            logger.error(f"Error creating transaction: {e}")
                            return {
                                'success': False,
                                'error': str(e),
                                'txid': None
                            }
                            
                    except Exception as e:
                        logger.error(f"Error in create_and_send_transaction: {e}")
                        return {
                            'success': False,
                            'error': str(e),
                            'txid': None
                        }

                def create_and_send_transaction_with_multiple_outputs(self, from_address, outputs, private_key_wif, address_type='segwit'):
                    """
                    Create and send a Bitcoin transaction with multiple outputs

                    Args:
                        from_address (str): Source Bitcoin address
                        outputs (list): List of dicts with 'address' and 'amount' keys
                        private_key_wif (str): Private key in WIF format

                    Returns:
                        dict: {'success': bool, 'error': str, 'txid': str}
                    """
                    import bitcoinlib
                    from bitcoinlib.wallets import Wallet, wallet_delete_if_exists
                    from bitcoinlib.keys import Key
                    import uuid
                    
                    try:
                        wallet_name = f"temp_wallet_{uuid.uuid4().hex[:8]}"
                        
                        try:
                            wallet_delete_if_exists(wallet_name)
                        except:
                            pass
                        
                        try:
                            key = Key(private_key_wif, network='bitcoin')
                            
                            # Try different witness types to find the one that matches from_address
                            # Both "segwit" and "native-segwit" refer to native segwit (bc1 addresses)
                            witness_types_to_try = []
                            if address_type == 'legacy':
                                witness_types_to_try = ['legacy', 'segwit', 'p2sh-segwit']
                            elif address_type == 'segwit' or address_type == 'native_segwit' or address_type == 'native-segwit':
                                witness_types_to_try = ['segwit', 'p2sh-segwit', 'legacy']
                            else:
                                witness_types_to_try = ['segwit', 'p2sh-segwit', 'legacy']
                            
                            wallet = None
                            wallet_address = None
                            
                            # Try each witness type until we find one that generates the expected address
                            for witness_type in witness_types_to_try:
                                try:
                                    temp_wallet = Wallet.create(wallet_name, keys=[key], network='bitcoin', witness_type=witness_type)
                                    temp_address = temp_wallet.addresslist()[0] if temp_wallet.addresslist() else None
                                    
                                    if temp_address == from_address:
                                        wallet = temp_wallet
                                        wallet_address = temp_address
                                        logger.info(f"Found matching witness type: {witness_type} for address {from_address}")
                                        break
                                    else:
                                        # Delete and try next witness type
                                        wallet_delete_if_exists(wallet_name)
                                        logger.debug(f"Witness type {witness_type} produced {temp_address}, expected {from_address}")
                                except Exception as e:
                                    logger.debug(f"Error trying witness type {witness_type}: {e}")
                                    try:
                                        wallet_delete_if_exists(wallet_name)
                                    except:
                                        pass
                            
                            # If no matching witness type found, use the first one and log warning
                            if wallet is None:
                                witness_type = witness_types_to_try[0]
                                wallet = Wallet.create(wallet_name, keys=[key], network='bitcoin', witness_type=witness_type)
                                wallet_address = wallet.addresslist()[0] if wallet.addresslist() else "Unknown"
                                logger.warning(f"Could not find matching witness type. Using {witness_type}")
                                logger.warning(f"Wallet address: {wallet_address}, expected address: {from_address}")
                            else:
                                logger.info(f"Successfully matched wallet address: {wallet_address}")
                            
                            # Sync wallet with blockchain to discover UTXOs using Service API
                            try:
                                from bitcoinlib.services.services import Service
                                logger.info(f"Starting wallet scan for {from_address}...")
                                
                                service = Service(network='bitcoin')
                                utxos_from_service = service.getutxos(from_address)
                                
                                if utxos_from_service:
                                    logger.info(f"Found {len(utxos_from_service)} UTXOs from blockchain service")
                                    for utxo in utxos_from_service:
                                        try:
                                            wallet.utxo_add(
                                                address=from_address,
                                                value=utxo['value'],
                                                txid=utxo['txid'],
                                                output_n=utxo['output_n'],
                                                confirmations=utxo.get('confirmations', 1)
                                            )
                                        except Exception as utxo_err:
                                            logger.warning(f"Could not add UTXO: {utxo_err}")
                                    
                                    wallet.utxos_update()
                                else:
                                    logger.warning(f"No UTXOs found for {from_address} from blockchain service")
                                
                                logger.info(f"Wallet scan completed for {from_address}")
                            except Exception as scan_error:
                                logger.error(f"Wallet scan failed for {from_address}: {scan_error}")
                                import traceback
                                traceback.print_exc()
                            
                            output_list = []
                            total_amount = 0
                            for output in outputs:
                                amount_satoshi = int(output['amount'] * 100000000)
                                output_list.append((output['address'], amount_satoshi))
                                total_amount += amount_satoshi
                            
                            # Check wallet balance before attempting transaction
                            current_balance = wallet.balance()
                            utxos = wallet.utxos()
                            logger.info(f"Wallet {from_address} balance: {current_balance} satoshis, UTXOs count: {len(utxos)}, Need: {total_amount} satoshis")
                            
                            if current_balance < total_amount:
                                btc_needed = total_amount / 100000000
                                btc_available = current_balance / 100000000
                                error_msg = (
                                    f"Insufficient funds: wallet {from_address} has {btc_available:.8f} BTC ({current_balance} satoshis), "
                                    f"need {btc_needed:.8f} BTC ({total_amount} satoshis). "
                                    f"Please fund this address before attempting the transaction."
                                )
                                logger.error(error_msg)
                                raise Exception(error_msg)
                            
                            # Check for available UTXOs
                            utxos = wallet.utxos()
                            if not utxos:
                                error_msg = (
                                    f"No unspent transaction outputs found for address {from_address}. "
                                    f"The address may not have been funded yet, or the blockchain sync may be incomplete."
                                )
                                logger.error(error_msg)
                                raise Exception(error_msg)
                            
                            # Handle multiple outputs properly based on bitcoinlib API
                            if len(output_list) == 1:
                                # Single output case
                                address, amount = output_list[0]
                                tx = wallet.send_to(address, amount)
                            else:
                                # Multiple outputs case - try different approaches
                                try:
                                    # Try using send method if available
                                    tx = wallet.send(output_list)
                                except (AttributeError, TypeError) as e:
                                    # Fallback: send to first output only (seller)
                                    logger.warning(f"Multiple output transaction failed ({e}), sending to first output only")
                                    address, amount = output_list[0]
                                    tx = wallet.send_to(address, amount)
                            
                            try:
                                wallet_delete_if_exists(wallet_name)
                            except:
                                pass
                            
                            return {
                                'success': True,
                                'txid': tx.hash,
                                'error': None
                            }
                        except Exception as e:
                            try:
                                wallet_delete_if_exists(wallet_name)
                            except:
                                pass
                            logger.error(f"Error creating multi-output transaction: {e}")
                            return {
                                'success': False,
                                'error': str(e),
                                'txid': None
                            }
                            
                    except Exception as e:
                        logger.error(f"Error in create_and_send_transaction_with_multiple_outputs: {e}")
                        return {
                            'success': False,
                            'error': str(e),
                            'txid': None
                        }

            class ElectrumXClient:
                def __init__(self, *args, **kwargs):
                    raise NotImplementedError("ElectrumXClient is not available. Please install the crypto-utils package correctly.")
except Exception as e:
    logger.error(f"Error importing crypto-utils: {e}")

    # Define stub classes and functions as a fallback
    class KeyManager:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("KeyManager is not available. Please install the crypto-utils package correctly.")

    class WalletManager:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("WalletManager is not available. Please install the crypto-utils package correctly.")

        @staticmethod
        def create_single_sig_wallet(wallet_name, address_type):
            """
            Create a single-signature wallet

            Args:
                wallet_name (str): Name of the wallet
                address_type (str): Type of address (legacy, segwit, native-segwit)

            Returns:
                tuple: (wallet_name, address, private_key)
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet
            from bitcoinlib.keys import Key
            import hashlib
            import uuid

            # Create a wallet using bitcoinlib
            wallet = Wallet.create(wallet_name, keys=None, network='bitcoin')

            # Get the first key and address
            key = wallet.get_key()

            # Get the appropriate address based on address_type
            try:
                if address_type == ADDRESS_TYPE_LEGACY:
                    address = key.address
                elif address_type == ADDRESS_TYPE_SEGWIT:
                    # Try to get segwit address, fall back to regular address if not available
                    if hasattr(key, 'address_segwit'):
                        address = key.address_segwit
                    else:
                        address = key.address
                elif address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                    # Try to get native segwit address, fall back to regular address if not available
                    if hasattr(key, 'address_segwit_p2sh'):
                        address = key.address_segwit_p2sh
                    else:
                        address = key.address
                else:
                    address = key.address
            except Exception as e:
                # Fall back to regular address if any error occurs
                address = key.address

            # Get the private key - convert from HD key to standard WIF format
            priv_key_hex = key.key_private.hex()
            std_key = Key(bytes.fromhex(priv_key_hex), network='bitcoin')
            private_key = std_key.wif()

            return wallet_name, address, private_key

        @staticmethod
        def create_multisig_wallet(wallet_name, m, n, public_keys=None, address_type=ADDRESS_TYPE_SEGWIT):
            """
            Create a multi-signature wallet

            Args:
                wallet_name (str): Name of the wallet
                m (int): Number of signatures required
                n (int): Total number of keys
                public_keys (list, optional): List of public keys. If None, new keys will be generated.
                address_type (str): Type of address (legacy, segwit, native-segwit)

            Returns:
                tuple: (wallet_name, address, private_keys)
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet
            import hashlib
            import uuid

            # If public keys are not provided, create a new multisig wallet with new keys
            if public_keys is None:
                # Create a multisig wallet with new keys
                wallet = Wallet.create(
                    wallet_name, 
                    keys=n,
                    network='bitcoin',
                    multisig=True,
                    sigs_required=m
                )

                # Get all keys
                keys = wallet.keys()
                private_keys = [key.wif for key in keys]

                # Get the first address based on address_type
                try:
                    key = wallet.get_key()
                    if address_type == ADDRESS_TYPE_LEGACY:
                        address = key.address
                    elif address_type == ADDRESS_TYPE_SEGWIT:
                        # Try to get segwit address, fall back to regular address if not available
                        if hasattr(key, 'address_segwit'):
                            address = key.address_segwit
                        else:
                            address = key.address
                    elif address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                        # Try to get native segwit address, fall back to regular address if not available
                        if hasattr(key, 'address_segwit_p2sh'):
                            address = key.address_segwit_p2sh
                        else:
                            address = key.address
                    else:
                        address = key.address
                except Exception as e:
                    # Fall back to regular address if any error occurs
                    address = wallet.get_key().address
            else:
                # Create a multisig wallet with provided public keys
                wallet = Wallet.create(
                    wallet_name,
                    keys=public_keys,
                    network='bitcoin',
                    multisig=True,
                    sigs_required=m
                )

                # Since we're using provided public keys, we don't have private keys
                private_keys = []

                # Get the first address based on address_type
                try:
                    key = wallet.get_key()
                    if address_type == ADDRESS_TYPE_LEGACY:
                        address = key.address
                    elif address_type == ADDRESS_TYPE_SEGWIT:
                        # Try to get segwit address, fall back to regular address if not available
                        if hasattr(key, 'address_segwit'):
                            address = key.address_segwit
                        else:
                            address = key.address
                    elif address_type == ADDRESS_TYPE_NATIVE_SEGWIT:
                        # Try to get native segwit address, fall back to regular address if not available
                        if hasattr(key, 'address_segwit_p2sh'):
                            address = key.address_segwit_p2sh
                        else:
                            address = key.address
                    else:
                        address = key.address
                except Exception as e:
                    # Fall back to regular address if any error occurs
                    address = wallet.get_key().address

            return wallet_name, address, private_keys

    class TransactionManager:
        def __init__(self, *args, **kwargs):
            pass

        def create_and_send_transaction(self, from_address, to_address, amount_btc, private_key_wif):
            """
            Create and send a Bitcoin transaction

            Args:
                from_address (str): Source Bitcoin address
                to_address (str): Destination Bitcoin address
                amount_btc (float): Amount in BTC to send
                private_key_wif (str): Private key in WIF format or hex string

            Returns:
                dict: {'success': bool, 'error': str, 'txid': str}
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet, wallet_delete_if_exists
            from bitcoinlib.keys import Key
            import uuid
            
            try:
                wallet_name = f"temp_wallet_{uuid.uuid4().hex[:8]}"
                
                try:
                    wallet_delete_if_exists(wallet_name)
                except:
                    pass
                
                try:
                    priv_key_obj = None
                    
                    # Handle different key formats: hex (64 chars), WIF format, or HD keys
                    if isinstance(private_key_wif, str) and len(private_key_wif) == 64:
                        # Hex format private key
                        try:
                            priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                        except Exception as hex_err:
                            logger.debug(f"Failed to parse as hex: {hex_err}")
                            priv_key_obj = Key(private_key_wif, network='bitcoin')
                    else:
                        # WIF format or other formats
                        try:
                            priv_key_obj = Key(private_key_wif, network='bitcoin')
                        except Exception as wif_err:
                            wif_err_str = str(wif_err)
                            logger.debug(f"Failed to parse as WIF: {wif_err_str}")
                            
                            # Check if it's an HD key format error
                            if 'hdkey' in wif_err_str.lower() or 'unknown key format' in wif_err_str.lower():
                                logger.debug("Detected HD key format, attempting conversion")
                                try:
                                    # Create temporary wallet to load the HD key
                                    temp_hd_wallet_name = f"temp_hd_{uuid.uuid4().hex[:8]}"
                                    try:
                                        wallet_delete_if_exists(temp_hd_wallet_name)
                                    except:
                                        pass
                                    
                                    # Create wallet and get the key to extract raw private key
                                    hd_wallet = Wallet.create(temp_hd_wallet_name, keys=None, network='bitcoin')
                                    hd_key = hd_wallet.get_key()
                                    
                                    # Convert HD key to standard WIF format
                                    priv_key_hex = hd_key.key_private.hex()
                                    priv_key_obj = Key(bytes.fromhex(priv_key_hex), network='bitcoin')
                                    
                                    # Clean up temp wallet
                                    try:
                                        wallet_delete_if_exists(temp_hd_wallet_name)
                                    except:
                                        pass
                                    
                                    logger.debug("Successfully converted HD key to standard format")
                                except Exception as hd_err:
                                    logger.error(f"Failed to convert HD key: {hd_err}")
                                    # Try hex as last resort
                                    if len(private_key_wif) == 64 and all(c in '0123456789abcdefABCDEF' for c in private_key_wif):
                                        try:
                                            priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                                        except:
                                            raise wif_err
                                    else:
                                        raise wif_err
                            else:
                                # Not an HD key error, try hex interpretation
                                if len(private_key_wif) == 64 and all(c in '0123456789abcdefABCDEF' for c in private_key_wif):
                                    try:
                                        priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                                    except:
                                        raise wif_err
                                else:
                                    raise wif_err
                    
                    wallet = Wallet.create(wallet_name, keys=[priv_key_obj], network='bitcoin')
                    
                    amount_satoshi = int(amount_btc * 100000000)
                    tx = wallet.send_to(to_address, amount_satoshi, fee=None)
                    
                    try:
                        wallet_delete_if_exists(wallet_name)
                    except:
                        pass
                    
                    return {
                        'success': True,
                        'txid': tx.hash,
                        'error': None
                    }
                except Exception as e:
                    try:
                        wallet_delete_if_exists(wallet_name)
                    except:
                        pass
                    logger.error(f"Error creating transaction: {e}")
                    return {
                        'success': False,
                        'error': str(e),
                        'txid': None
                    }
                    
            except Exception as e:
                logger.error(f"Error in create_and_send_transaction: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'txid': None
                }

        def create_and_send_transaction_with_multiple_outputs(self, from_address, outputs, private_key_wif):
            """
            Create and send a Bitcoin transaction with multiple outputs

            Args:
                from_address (str): Source Bitcoin address
                outputs (list): List of dicts with 'address' and 'amount' keys
                private_key_wif (str): Private key in WIF format or hex string

            Returns:
                dict: {'success': bool, 'error': str, 'txid': str}
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet, wallet_delete_if_exists
            from bitcoinlib.keys import Key
            import uuid
            
            try:
                wallet_name = f"temp_wallet_{uuid.uuid4().hex[:8]}"
                
                try:
                    wallet_delete_if_exists(wallet_name)
                except:
                    pass
                
                try:
                    priv_key_obj = None
                    
                    # Handle different key formats: hex (64 chars), WIF format, or HD keys
                    if isinstance(private_key_wif, str) and len(private_key_wif) == 64:
                        # Hex format private key
                        try:
                            priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                        except Exception as hex_err:
                            logger.debug(f"Failed to parse as hex: {hex_err}")
                            priv_key_obj = Key(private_key_wif, network='bitcoin')
                    else:
                        # WIF format or other formats
                        try:
                            priv_key_obj = Key(private_key_wif, network='bitcoin')
                        except Exception as wif_err:
                            wif_err_str = str(wif_err)
                            logger.debug(f"Failed to parse as WIF: {wif_err_str}")
                            
                            # Check if it's an HD key format error
                            if 'hdkey' in wif_err_str.lower() or 'unknown key format' in wif_err_str.lower():
                                logger.debug("Detected HD key format, attempting conversion")
                                try:
                                    # Create temporary wallet to load the HD key
                                    temp_hd_wallet_name = f"temp_hd_{uuid.uuid4().hex[:8]}"
                                    try:
                                        wallet_delete_if_exists(temp_hd_wallet_name)
                                    except:
                                        pass
                                    
                                    # Create wallet and get the key to extract raw private key
                                    hd_wallet = Wallet.create(temp_hd_wallet_name, keys=None, network='bitcoin')
                                    hd_key = hd_wallet.get_key()
                                    
                                    # Convert HD key to standard WIF format
                                    priv_key_hex = hd_key.key_private.hex()
                                    priv_key_obj = Key(bytes.fromhex(priv_key_hex), network='bitcoin')
                                    
                                    # Clean up temp wallet
                                    try:
                                        wallet_delete_if_exists(temp_hd_wallet_name)
                                    except:
                                        pass
                                    
                                    logger.debug("Successfully converted HD key to standard format")
                                except Exception as hd_err:
                                    logger.error(f"Failed to convert HD key: {hd_err}")
                                    # Try hex as last resort
                                    if len(private_key_wif) == 64 and all(c in '0123456789abcdefABCDEF' for c in private_key_wif):
                                        try:
                                            priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                                        except:
                                            raise wif_err
                                    else:
                                        raise wif_err
                            else:
                                # Not an HD key error, try hex interpretation
                                if len(private_key_wif) == 64 and all(c in '0123456789abcdefABCDEF' for c in private_key_wif):
                                    try:
                                        priv_key_obj = Key(bytes.fromhex(private_key_wif), network='bitcoin')
                                    except:
                                        raise wif_err
                                else:
                                    raise wif_err
                    
                    wallet = Wallet.create(wallet_name, keys=[priv_key_obj], network='bitcoin')
                    
                    output_list = []
                    for output in outputs:
                        amount_satoshi = int(output['amount'] * 100000000)
                        output_list.append((output['address'], amount_satoshi))
                    
                    tx = wallet.send_to(output_list, fee=None)
                    
                    try:
                        wallet_delete_if_exists(wallet_name)
                    except:
                        pass
                    
                    return {
                        'success': True,
                        'txid': tx.hash,
                        'error': None
                    }
                except Exception as e:
                    try:
                        wallet_delete_if_exists(wallet_name)
                    except:
                        pass
                    logger.error(f"Error creating multi-output transaction: {e}")
                    return {
                        'success': False,
                        'error': str(e),
                        'txid': None
                    }
                    
            except Exception as e:
                logger.error(f"Error in create_and_send_transaction_with_multiple_outputs: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'txid': None
                }

        @staticmethod
        def get_estimated_fee(crypto_type, amount=None, tx_size=None):
            """
            Estimate the transaction fee for a given cryptocurrency

            Args:
                crypto_type (str): Type of cryptocurrency (BTC, ETH, etc.)
                amount (float, optional): Transaction amount (needed for some cryptocurrencies)
                tx_size (int, optional): Estimated transaction size in bytes (for Bitcoin-like cryptocurrencies)

            Returns:
                dict: Dictionary containing fee information:
                    - 'fee': Estimated fee in cryptocurrency units
                    - 'fee_usd': Estimated fee in USD
                    - 'gas_price': Gas price (for Ethereum-like cryptocurrencies)
                    - 'gas_limit': Gas limit (for Ethereum-like cryptocurrencies)
            """
            import requests
            from crypto_price import get_crypto_price, convert_crypto_to_fiat

            crypto_type = crypto_type.upper()

            try:
                # Default values
                fee = 0
                fee_usd = 0
                gas_price = None
                gas_limit = None

                # Bitcoin and Bitcoin-like cryptocurrencies
                if crypto_type in ['BTC', 'LTC', 'BCH', 'DASH', 'ZEC']:
                    # Default transaction size if not provided
                    if tx_size is None:
                        tx_size = 250  # Average transaction size in bytes

                    # Get fee rates from mempool.space API for BTC
                    if crypto_type == 'BTC':
                        try:
                            response = requests.get('https://mempool.space/api/v1/fees/recommended')
                            data = response.json()
                            # Use 'hourFee' for a balance between speed and cost
                            fee_rate = data.get('hourFee', 5)  # satoshis per byte, default to 5 if API fails
                        except:
                            fee_rate = 5  # Default fee rate in satoshis per byte

                        # Calculate fee in BTC (convert from satoshis)
                        fee = (fee_rate * tx_size) / 100000000

                    # For other Bitcoin-like cryptocurrencies, use reasonable defaults
                    elif crypto_type == 'LTC':
                        fee = 0.0001
                    elif crypto_type == 'BCH':
                        fee = 0.0001
                    elif crypto_type == 'DASH':
                        fee = 0.0001
                    elif crypto_type == 'ZEC':
                        fee = 0.0001

                # Ethereum and ERC-20 tokens
                elif crypto_type in ['ETH']:
                    try:
                        # Get gas price from Etherscan API
                        response = requests.get('https://api.etherscan.io/api?module=gastracker&action=gasoracle')
                        data = response.json()

                        if data['status'] == '1':
                            # Use 'ProposeGasPrice' for a balance between speed and cost
                            gas_price = int(data['result']['ProposeGasPrice'])
                        else:
                            gas_price = 50  # Default gas price in Gwei
                    except:
                        gas_price = 50  # Default gas price in Gwei

                    # Standard ETH transfer uses 21,000 gas
                    gas_limit = 21000

                    # Calculate fee in ETH (gas_price is in Gwei)
                    fee = (gas_price * gas_limit) / 1000000000

                # Monero has fixed fees
                elif crypto_type == 'XMR':
                    fee = 0.0001

                # Convert fee to USD
                fee_usd = convert_crypto_to_fiat(fee, crypto_type)

                return {
                    'fee': fee,
                    'fee_usd': fee_usd,
                    'gas_price': gas_price,
                    'gas_limit': gas_limit
                }
            except Exception as e:
                logger.error(f"Error estimating fee for {crypto_type}: {str(e)}")
                return {
                    'fee': 0,
                    'fee_usd': 0,
                    'gas_price': None,
                    'gas_limit': None
                }

        @staticmethod
        def sign_transaction(wallet_name, tx_id, private_keys):
            """
            Sign a transaction with the provided private keys

            Args:
                wallet_name (str): Name of the wallet
                tx_id (str): Transaction ID to sign
                private_keys (list): List of private keys to sign with

            Returns:
                str: Signed transaction hex
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet

            try:
                # Open the wallet
                wallet = Wallet(wallet_name)

                # Get the transaction
                transaction = wallet.transactions().get(tx_id)
                if not transaction:
                    # Try to get the transaction from the blockchain
                    transaction = wallet.transaction_import(tx_id)

                if not transaction:
                    return None

                # Sign the transaction with the provided private keys
                for private_key in private_keys:
                    wallet.transaction_sign(transaction, private_key)

                # Get the signed transaction hex
                signed_tx_hex = transaction.raw_hex()

                return signed_tx_hex
            except Exception as e:
                logger.error(f"Error signing transaction: {e}")
                return None

        @staticmethod
        def broadcast_transaction(tx_hex):
            """
            Broadcast a signed transaction to the network

            Args:
                tx_hex (str): Signed transaction hex

            Returns:
                str: Transaction ID if successful, None otherwise
            """
            import bitcoinlib
            from bitcoinlib.wallets import Wallet
            from bitcoinlib.services.services import Service

            try:
                # Create a service object
                service = Service()

                # Broadcast the transaction
                tx_id = service.sendrawtransaction(tx_hex)

                return tx_id
            except Exception as e:
                logger.error(f"Error broadcasting transaction: {e}")
                return None

    class ElectrumXClient:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("ElectrumXClient is not available. Please install the crypto-utils package correctly.")
