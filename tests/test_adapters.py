import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))
from adapters import ADAPTERS
print('SUCCESS: Adapters loaded:', list(ADAPTERS.keys()))