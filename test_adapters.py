import sys
sys.path.append('backend')
from adapters import ADAPTERS
print('SUCCESS: Adapters loaded:', list(ADAPTERS.keys()))