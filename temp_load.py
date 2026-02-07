import os, importlib.util
from dotenv import load_dotenv
ROOT = r"C:/Users/Localuser/Videos/Strata-LLM-version-dbx"
load_dotenv(os.path.join(ROOT, ".env"))
load_dotenv(os.path.join(ROOT, "backend", ".env"))
spec = importlib.util.spec_from_file_location('backend_ai', os.path.join(ROOT, "backend", "ai.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print('Loaded module. URL:', os.getenv("DATABRICKS_LLM_INVOCATIONS_URL"))
print('Token present?', bool(os.getenv("DATABRICKS_LLM_TOKEN")))
