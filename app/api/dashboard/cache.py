import time
from functools import wraps
from typing import Callable, Any, Dict, Tuple

# Global dictionary keeping cache data
# Schema: {(func_name, schema): (expiration_timestamp_float, cached_data)}
_IN_MEMORY_CACHE: Dict[Tuple[str, str], Tuple[float, Any]] = {}

def ttl_cache(ttl_seconds: int = 60):
    """
    Dependency-free, simple in-memory TTL caching decorator.
    Only intended for functions that take (db: Session, schema: str).
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(db, schema: str, *args, **kwargs):
            key = (func.__name__, schema)
            now = time.time()
            
            # Check if valid cache strictly exists
            if key in _IN_MEMORY_CACHE:
                expiry, data = _IN_MEMORY_CACHE[key]
                if now < expiry:
                    return data
            
            # Execute actual function (fetching fresh data)
            result = func(db, schema, *args, **kwargs)
            
            # Save into cache
            _IN_MEMORY_CACHE[key] = (now + ttl_seconds, result)
            
            return result
        return wrapper
    return decorator
