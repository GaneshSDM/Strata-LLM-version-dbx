import os
import json
from cryptography.fernet import Fernet

FERNET_KEY_FILE = os.path.join(os.path.dirname(__file__), ".fernet_key")

def get_fernet_key():
    key = os.getenv("FERNET_KEY")
    if key:
        return key.encode()
    
    if os.path.exists(FERNET_KEY_FILE):
        with open(FERNET_KEY_FILE, "rb") as f:
            return f.read()
    
    key = Fernet.generate_key()
    try:
        with open(FERNET_KEY_FILE, "wb") as f:
            f.write(key)
    except (IOError, OSError):
        pass
    return key

fernet = Fernet(get_fernet_key())

def encrypt_credentials(credentials: dict) -> bytes:
    json_str = json.dumps(credentials)
    return fernet.encrypt(json_str.encode())

def decrypt_credentials(enc_credentials: bytes) -> dict:
    decrypted = fernet.decrypt(enc_credentials)
    return json.loads(decrypted.decode())