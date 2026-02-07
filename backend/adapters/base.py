from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable

class DatabaseAdapter(ABC):
    def __init__(self, credentials: dict):
        self.credentials = credentials
        self.driver_available = True

    def get_connection(self):
        """
        Optional helper for synchronous validation paths that expect a DB-API connection.
        Concrete adapters should override when supported; default raises.
        """
        raise NotImplementedError("get_connection is not implemented for this adapter")
    
    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def introspect_analysis(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def extract_objects(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def create_objects(self, translated_ddl: List[Dict]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def copy_table_data(
        self,
        table_name: str,
        source_adapter: 'DatabaseAdapter',
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def run_validation_checks(self, source_adapter: 'DatabaseAdapter') -> Dict[str, Any]:
        pass

    @abstractmethod
    async def drop_tables(self, table_names: List[str]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def rename_column(self, table_name: str, old_column_name: str, new_column_name: str) -> Dict[str, Any]:
        pass

    async def drop_column(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Drop a column from a table. Optional; override in adapters that support it."""
        raise NotImplementedError("drop_column not implemented for this adapter")

    async def list_columns(self, table_name: str) -> List[str]:
        """Return column names for the given table. Optional; override when supported."""
        raise NotImplementedError("list_columns not implemented for this adapter")
