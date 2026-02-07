#!/usr/bin/env python3
"""
Script to verify that the encrypt_credentials import fix is in place
"""

import ast
import sys

def check_import_statement():
    """Check if the import statement includes encrypt_credentials"""
    try:
        with open('main.py', 'r') as f:
            content = f.read()
            
        # Parse the AST to find import statements
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module == 'encryption':
                    # Check if encrypt_credentials is in the imported names
                    for alias in node.names:
                        if alias.name == 'encrypt_credentials':
                            return True
                            
        return False
    except Exception as e:
        print(f"Error reading/parsing main.py: {e}")
        return False

def check_function_calls():
    """Check if encrypt_credentials is called in the save functions"""
    try:
        with open('main.py', 'r') as f:
            content = f.read()
            
        # Check for encrypt_credentials calls
        save_connection_call = 'enc_creds = encrypt_credentials(req.credentials)' in content
        update_connection_call = 'enc_creds = encrypt_credentials(req.credentials)' in content
        
        return save_connection_call and update_connection_call
    except Exception as e:
        print(f"Error checking function calls: {e}")
        return False

def main():
    print("Verifying encrypt_credentials fix...")
    print("=" * 40)
    
    # Check import statement
    import_correct = check_import_statement()
    print(f"Import statement correct: {'✅ YES' if import_correct else '❌ NO'}")
    
    # Check function calls
    calls_correct = check_function_calls()
    print(f"Function calls present: {'✅ YES' if calls_correct else '❌ NO'}")
    
    if import_correct and calls_correct:
        print("\n🎉 All checks passed! The fix should be working.")
        return True
    else:
        print("\n❌ Some checks failed. The fix may not be complete.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)