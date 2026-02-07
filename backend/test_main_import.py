import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

print("Testing import of main.py...")
try:
    import main
    print("✅ main.py imported successfully")
    
    # Check if encrypt_credentials is available in the main module
    if hasattr(main, 'encrypt_credentials'):
        print("✅ encrypt_credentials function is available in main module")
    else:
        print("❌ encrypt_credentials function is NOT available in main module")
        print("Available attributes in main module:", dir(main))
        
    # Try to import encrypt_credentials directly
    try:
        from main import encrypt_credentials
        print("✅ encrypt_credentials imported directly from main module")
    except Exception as e:
        print(f"❌ Failed to import encrypt_credentials directly: {e}")
        
except Exception as e:
    print(f"❌ Failed to import main.py: {e}")
    import traceback
    traceback.print_exc()