import sys
import os
import traceback

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 50)
print("COMPREHENSIVE TEST OF ENCRYPTION FUNCTIONALITY")
print("=" * 50)

# Test 1: Import encryption module
print("\n1. Testing encryption module import...")
try:
    from encryption import encrypt_credentials, decrypt_credentials, fernet
    print("✅ encryption module imported successfully")
    print(f"   Fernet object: {fernet}")
except Exception as e:
    print(f"❌ Failed to import encryption module: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 2: Test encryption function directly
print("\n2. Testing encrypt_credentials function directly...")
try:
    test_data = {"host": "localhost", "port": "5432", "database": "testdb"}
    encrypted = encrypt_credentials(test_data)
    print("✅ encrypt_credentials function works correctly")
    print(f"   Encrypted data type: {type(encrypted)}")
    print(f"   Encrypted data length: {len(encrypted)}")
except Exception as e:
    print(f"❌ encrypt_credentials function failed: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 3: Test decryption function directly
print("\n3. Testing decrypt_credentials function directly...")
try:
    decrypted = decrypt_credentials(encrypted)
    if decrypted == test_data:
        print("✅ decrypt_credentials function works correctly")
        print(f"   Decrypted data matches original: {decrypted}")
    else:
        print("❌ decrypt_credentials function failed - data mismatch")
        print(f"   Original: {test_data}")
        print(f"   Decrypted: {decrypted}")
        sys.exit(1)
except Exception as e:
    print(f"❌ decrypt_credentials function failed: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 4: Import main module
print("\n4. Testing main module import...")
try:
    import main
    print("✅ main module imported successfully")
    
    # Check if encrypt_credentials is available in main module
    if hasattr(main, 'encrypt_credentials'):
        print("✅ encrypt_credentials function is available in main module")
    else:
        print("⚠️  encrypt_credentials function is NOT directly available in main module")
        print("   This might be expected if it's not exported at module level")
        
    # Try to access it through the module
    try:
        from main import encrypt_credentials as main_encrypt
        print("✅ encrypt_credentials can be imported from main module")
    except Exception as e:
        print(f"⚠️  Cannot import encrypt_credentials directly from main: {e}")
        
except Exception as e:
    print(f"❌ Failed to import main module: {e}")
    traceback.print_exc()
    sys.exit(1)

# Test 5: Check the actual save_connection function
print("\n5. Checking save_connection function...")
try:
    # Read the main.py file to verify the code
    with open('main.py', 'r') as f:
        content = f.read()
        
    if 'encrypt_credentials(req.credentials)' in content:
        print("✅ encrypt_credentials call found in save_connection function")
    else:
        print("❌ encrypt_credentials call NOT found in save_connection function")
        
    if 'from encryption import decrypt_credentials, encrypt_credentials' in content:
        print("✅ Correct import statement found in main.py")
    else:
        print("❌ Correct import statement NOT found in main.py")
        
except Exception as e:
    print(f"❌ Failed to read main.py: {e}")

print("\n" + "=" * 50)
print("TEST COMPLETED")
print("=" * 50)