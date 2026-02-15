import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from encryption import encrypt_credentials, decrypt_credentials
import json

# Test data
test_credentials = {
    "host": "localhost",
    "port": "5432",
    "database": "testdb",
    "username": "testuser",
    "password": "testpass"
}

print("Testing encrypt_credentials function...")
try:
    # Test encryption
    encrypted = encrypt_credentials(test_credentials)
    print(f"Encryption successful. Encrypted data type: {type(encrypted)}")
    print(f"Encrypted data length: {len(encrypted)}")
    
    # Test decryption
    decrypted = decrypt_credentials(encrypted)
    print(f"Decryption successful. Decrypted data: {decrypted}")
    
    # Verify data integrity
    if decrypted == test_credentials:
        print("✅ Encryption/Decryption test PASSED")
    else:
        print("❌ Encryption/Decryption test FAILED - data mismatch")
        
except Exception as e:
    print(f"❌ Encryption/Decryption test FAILED with error: {e}")
    import traceback
    traceback.print_exc()