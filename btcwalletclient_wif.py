import requests
import hashlib
import json
import struct
import binascii
import base58

wif_private_key = "L4GtTKLq5hm2vL8SZQXwBMKsxEJ9AP8e44dHHFZi8EKMSwApSB6H"
destination_address = "bc1q6x8yau59enx4ze6s4am5gn7h2z5zr7ytjumhwy"
FEE_WALLET_ADDRESS = "bc1q8mcfyyt0hdhsqvv4ly6czz52gyak5zaayw8qa5"

def bech32_decode(bech):
    charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    if bech.lower() != bech and bech.upper() != bech:
        return None, None
    bech = bech.lower()
    pos = bech.rfind('1')
    if pos < 1 or pos + 7 > len(bech) or len(bech) > 90:
        return None, None
    if not all(x in charset for x in bech[pos+1:]):
        return None, None
    hrp = bech[:pos]
    data = [charset.find(x) for x in bech[pos+1:]]
    if not bech32_verify_checksum(hrp, data):
        return None, None
    return hrp, data[:-6]

def bech32_verify_checksum(hrp, data):
    return bech32_polymod(bech32_hrp_expand(hrp) + data) == 1

def bech32_hrp_expand(hrp):
    return [ord(x) >> 5 for x in hrp] + [0] + [ord(x) & 31 for x in hrp]

def bech32_polymod(values):
    GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    chk = 1
    for value in values:
        b = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ value
        for i in range(5):
            chk ^= GEN[i] if ((b >> i) & 1) else 0
    return chk

def convertbits(data, frombits, tobits, pad=True):
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    max_acc = (1 << (frombits + tobits - 1)) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None
        acc = ((acc << frombits) | value) & max_acc
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret

def decode_bech32_address(address):
    hrp, data = bech32_decode(address)
    if hrp is None:
        return None
    decoded = convertbits(data[1:], 5, 8, False)
    if decoded is None or len(decoded) < 2 or len(decoded) > 40:
        return None
    if data[0] > 16:
        return None
    return bytes(decoded)

def decode_wif(wif):
    decoded = base58.b58decode(wif)
    private_key = decoded[1:33]
    compressed = len(decoded) == 38
    return private_key, compressed

def private_key_to_public_key(private_key_bytes, compressed=True):
    import ecdsa
    sk = ecdsa.SigningKey.from_string(private_key_bytes, curve=ecdsa.SECP256k1)
    vk = sk.get_verifying_key()
    if compressed:
        x = vk.pubkey.point.x()
        y = vk.pubkey.point.y()
        prefix = b'\x02' if y % 2 == 0 else b'\x03'
        return prefix + x.to_bytes(32, 'big')
    else:
        return b'\x04' + vk.to_string()

def public_key_to_bech32_address(public_key_bytes):
    sha256_hash = hashlib.sha256(public_key_bytes).digest()
    ripemd160_hash = hashlib.new('ripemd160', sha256_hash).digest()
    
    witness_version = 0
    witness_program = ripemd160_hash
    
    five_bit_data = convertbits(witness_program, 8, 5)
    if five_bit_data is None:
        return None
    
    charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    hrp = "bc"
    data = [witness_version] + five_bit_data
    
    checksum = bech32_create_checksum(hrp, data)
    combined = data + checksum
    
    return hrp + '1' + ''.join([charset[d] for d in combined])

def bech32_create_checksum(hrp, data):
    values = bech32_hrp_expand(hrp) + data
    polymod = bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]

def get_utxos(address):
    try:
        url = f"https://blockstream.info/api/address/{address}/utxo"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch UTXOs: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching UTXOs: {e}")
        return []

def get_balance(address):
    utxos = get_utxos(address)
    return sum(utxo['value'] for utxo in utxos)

def broadcast_transaction(raw_tx_hex):
    try:
        url = "https://blockstream.info/api/tx"
        response = requests.post(url, data=raw_tx_hex)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Broadcast failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error broadcasting transaction: {e}")
        return None

def var_int(i):
    if i < 0xfd:
        return struct.pack('<B', i)
    elif i <= 0xffff:
        return b'\xfd' + struct.pack('<H', i)
    elif i <= 0xffffffff:
        return b'\xfe' + struct.pack('<I', i)
    else:
        return b'\xff' + struct.pack('<Q', i)

def make_canonical_signature(signature_der):
    import ecdsa
    r, s = ecdsa.util.sigdecode_der(signature_der, ecdsa.SECP256k1.order)
    half_order = ecdsa.SECP256k1.order // 2
    if s > half_order:
        s = ecdsa.SECP256k1.order - s
    return ecdsa.util.sigencode_der(r, s, ecdsa.SECP256k1.order)

def build_segwit_transaction(utxos, outputs, private_key_hex, public_key_hex):
    version = struct.pack('<I', 2)
    marker = b'\x00'
    flag = b'\x01'
    
    inputs_data = b''
    witness_data = b''
    
    for utxo in utxos:
        txid = bytes.fromhex(utxo['txid'])[::-1]
        vout = struct.pack('<I', utxo['vout'])
        script_sig = b''
        sequence = struct.pack('<I', 0xfffffffd)
        inputs_data += txid + vout + var_int(len(script_sig)) + script_sig + sequence
    
    outputs_data = b''
    for output in outputs:
        value = struct.pack('<Q', output['value'])
        witness_program = decode_bech32_address(output['address'])
        script_pubkey = b'\x00\x14' + witness_program
        outputs_data += value + var_int(len(script_pubkey)) + script_pubkey
    
    private_key_bytes = bytes.fromhex(private_key_hex)
    public_key_bytes = bytes.fromhex(public_key_hex)
    
    for i, utxo in enumerate(utxos):
        hash_prevouts = hashlib.sha256(hashlib.sha256(
            b''.join([bytes.fromhex(u['txid'])[::-1] + struct.pack('<I', u['vout']) for u in utxos])
        ).digest()).digest()
        
        hash_sequence = hashlib.sha256(hashlib.sha256(
            b''.join([struct.pack('<I', 0xfffffffd) for _ in utxos])
        ).digest()).digest()
        
        outpoint = bytes.fromhex(utxo['txid'])[::-1] + struct.pack('<I', utxo['vout'])
        script_code = b'\x19\x76\xa9\x14' + hashlib.new('ripemd160', hashlib.sha256(public_key_bytes).digest()).digest() + b'\x88\xac'
        amount = struct.pack('<Q', utxo['value'])
        sequence = struct.pack('<I', 0xfffffffd)
        
        hash_outputs = hashlib.sha256(hashlib.sha256(outputs_data).digest()).digest()
        
        sighash_preimage = (
            struct.pack('<I', 2) +
            hash_prevouts +
            hash_sequence +
            outpoint +
            script_code +
            amount +
            sequence +
            hash_outputs +
            struct.pack('<I', 0) +
            struct.pack('<I', 1)
        )
        
        sighash = hashlib.sha256(hashlib.sha256(sighash_preimage).digest()).digest()
        
        import ecdsa
        sk = ecdsa.SigningKey.from_string(private_key_bytes, curve=ecdsa.SECP256k1)
        signature_der = sk.sign_digest(sighash, sigencode=ecdsa.util.sigencode_der)
        signature_canonical = make_canonical_signature(signature_der)
        signature = signature_canonical + b'\x01'
        
        witness = var_int(2) + var_int(len(signature)) + signature + var_int(len(public_key_bytes)) + public_key_bytes
        witness_data += witness
    
    locktime = struct.pack('<I', 0)
    
    raw_tx = (
        version +
        marker +
        flag +
        var_int(len(utxos)) +
        inputs_data +
        var_int(len(outputs)) +
        outputs_data +
        witness_data +
        locktime
    )
    
    return raw_tx.hex()

def send_max_btc_auto(wif_private_key, destination_address):
    try:
        private_key_bytes, compressed = decode_wif(wif_private_key)
        public_key_bytes = private_key_to_public_key(private_key_bytes, compressed)
        source_address = public_key_to_bech32_address(public_key_bytes)
        
        private_key_hex = private_key_bytes.hex()
        public_key_hex = public_key_bytes.hex()
        
        utxos = get_utxos(source_address)
        
        if not utxos:
            return {'success': False, 'error': 'No UTXOs found'}
        
        balance = sum(utxo['value'] for utxo in utxos)
        
        MAX_FEE = 250
        estimated_size = len(utxos) * 148 + 34 + 10
        
        transaction_fee = min(MAX_FEE, balance - 546)
        
        if transaction_fee < 1:
            return {'success': False, 'error': 'Balance too low to cover transaction fees'}
        
        max_sendable = balance - transaction_fee
        
        if max_sendable < 546:
            return {'success': False, 'error': 'Balance too low to send minimum amount (546 satoshis)'}
        
        amount_to_send = max_sendable
        
        outputs = [{
            'address': destination_address,
            'value': amount_to_send
        }]
        
        tx = build_segwit_transaction(utxos, outputs, private_key_hex, public_key_hex)
        
        tx_id = broadcast_transaction(tx)
        
        if tx_id:
            return {
                'success': True,
                'txid': tx_id,
                'amount_sent': amount_to_send / 1e8,
                'fee': transaction_fee / 1e8,
                'balance': balance / 1e8
            }
        else:
            return {'success': False, 'error': 'Failed to broadcast transaction'}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': f'Error processing transaction: {str(e)}'}

def send_batch_95_5_split(wif_private_key, seller_address):
    try:
        private_key_bytes, compressed = decode_wif(wif_private_key)
        public_key_bytes = private_key_to_public_key(private_key_bytes, compressed)
        source_address = public_key_to_bech32_address(public_key_bytes)
        
        private_key_hex = private_key_bytes.hex()
        public_key_hex = public_key_bytes.hex()
        
        utxos = get_utxos(source_address)
        
        if not utxos:
            return {'success': False, 'error': 'No UTXOs found'}
        
        balance = sum(utxo['value'] for utxo in utxos)
        
        MAX_FEE = 250
        estimated_size = len(utxos) * 148 + 2 * 34 + 10
        
        transaction_fee = min(MAX_FEE, balance - 1092)
        
        if transaction_fee < 1:
            return {'success': False, 'error': 'Balance too low to cover transaction fees'}
        
        max_sendable = balance - transaction_fee
        
        if max_sendable < 1092:
            return {'success': False, 'error': 'Balance too low to send minimum amount (1092 satoshis for 2 outputs)'}
        
        seller_amount = int(max_sendable * 0.95)
        fee_amount = max_sendable - seller_amount
        
        if seller_amount < 546:
            return {'success': False, 'error': 'Seller amount too low (minimum 546 satoshis)'}
        
        if fee_amount < 546:
            return {'success': False, 'error': 'Fee amount too low (minimum 546 satoshis)'}
        
        address = FEE_WALLET_ADDRESS
        
        outputs = [
            {
                'address': seller_address,
                'value': seller_amount
            },
            {
                'address': address,
                'value': fee_amount
            }
        ]
        
        tx = build_segwit_transaction(utxos, outputs, private_key_hex, public_key_hex)
        
        tx_id = broadcast_transaction(tx)
        
        if tx_id:
            return {
                'success': True,
                'txid': tx_id,
                'seller_amount': seller_amount / 1e8,
                'fee_wallet_amount': fee_amount / 1e8,
                'transaction_fee': transaction_fee / 1e8,
                'balance': balance / 1e8
            }
        else:
            return {'success': False, 'error': 'Failed to broadcast transaction'}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': f'Error processing transaction: {str(e)}'}

def send_dispute_refund_50_50(wif_private_key, seller_address):
    try:
        private_key_bytes, compressed = decode_wif(wif_private_key)
        public_key_bytes = private_key_to_public_key(private_key_bytes, compressed)
        source_address = public_key_to_bech32_address(public_key_bytes)
        
        private_key_hex = private_key_bytes.hex()
        public_key_hex = public_key_bytes.hex()
        
        utxos = get_utxos(source_address)
        
        if not utxos:
            return {'success': False, 'error': 'No UTXOs found'}
        
        balance = sum(utxo['value'] for utxo in utxos)
        
        MAX_FEE = 250
        estimated_size = len(utxos) * 148 + 2 * 34 + 10
        
        transaction_fee = min(MAX_FEE, balance - 1092)
        
        if transaction_fee < 1:
            return {'success': False, 'error': 'Balance too low to cover transaction fees'}
        
        max_sendable = balance - transaction_fee
        
        if max_sendable < 1092:
            return {'success': False, 'error': 'Balance too low to send minimum amount (1092 satoshis for 2 outputs)'}
        
        seller_amount = int(max_sendable * 0.50)
        fee_amount = max_sendable - seller_amount
        
        if seller_amount < 546:
            return {'success': False, 'error': 'Seller amount too low (minimum 546 satoshis)'}
        
        if fee_amount < 546:
            return {'success': False, 'error': 'Fee amount too low (minimum 546 satoshis)'}
        
        address = FEE_WALLET_ADDRESS
        
        outputs = [
            {
                'address': seller_address,
                'value': seller_amount
            },
            {
                'address': address,
                'value': fee_amount
            }
        ]
        
        tx = build_segwit_transaction(utxos, outputs, private_key_hex, public_key_hex)
        
        tx_id = broadcast_transaction(tx)
        
        if tx_id:
            return {
                'success': True,
                'txid': tx_id,
                'seller_amount': seller_amount / 1e8,
                'fee_wallet_amount': fee_amount / 1e8,
                'transaction_fee': transaction_fee / 1e8,
                'balance': balance / 1e8
            }
        else:
            return {'success': False, 'error': 'Failed to broadcast transaction'}
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': f'Error processing transaction: {str(e)}'}

def send_max_btc(private_key_hex, public_key_hex, source_address, destination):
    try:
        utxos = get_utxos(source_address)
        
        if not utxos:
            print("No UTXOs found.")
            return False
        
        balance = sum(utxo['value'] for utxo in utxos)
        print(f"Balance: {balance} satoshis ({balance / 1e8} BTC)")
        
        MAX_FEE = 250
        estimated_size = len(utxos) * 148 + 34 + 10
        
        transaction_fee = min(MAX_FEE, balance - 546)
        
        if transaction_fee < 1:
            print("Balance too low to cover transaction fees.")
            return False
        
        actual_fee_rate = transaction_fee / estimated_size
        max_sendable = balance - transaction_fee
        
        print(f"Transaction fee: {transaction_fee} satoshis ({transaction_fee / 1e8} BTC)")
        print(f"Fee rate: {actual_fee_rate:.2f} sat/vByte (low priority)")
        print(f"Maximum sendable: {max_sendable} satoshis ({max_sendable / 1e8} BTC)")
        
        if max_sendable < 546:
            print("Balance too low to send minimum amount (546 satoshis).")
            return False
        
        while True:
            try:
                btc_input = input("\nHow much BTC do you want to send? (or 'max' for maximum): ").strip().lower()
                
                if btc_input == 'max':
                    amount_to_send = max_sendable
                    break
                else:
                    btc_amount = float(btc_input)
                    amount_to_send = int(btc_amount * 1e8)
                    
                    if amount_to_send < 546:
                        print("Amount too low. Minimum is 546 satoshis (0.00000546 BTC).")
                        continue
                    
                    if amount_to_send > max_sendable:
                        print(f"Amount exceeds maximum sendable ({max_sendable / 1e8} BTC including fees).")
                        continue
                    
                    break
            except ValueError:
                print("Invalid input. Please enter a number or 'max'.")
                continue
        
        print(f"Sending {amount_to_send} satoshis ({amount_to_send / 1e8} BTC)")
        print(f"Fee: {transaction_fee} satoshis ({transaction_fee / 1e8} BTC)")
        
        outputs = [{
            'address': destination,
            'value': amount_to_send
        }]
        
        tx = build_segwit_transaction(utxos, outputs, private_key_hex, public_key_hex)
        
        print(f"Transaction created: {tx}")
        
        tx_id = broadcast_transaction(tx)
        
        if tx_id:
            print(f"Transaction sent! TX Hash: {tx_id}")
            return True
        else:
            print("Failed to broadcast transaction.")
            return False
            
    except Exception as e:
        print(f"Error processing transaction: {e}")
        import traceback
        traceback.print_exc()
        return False