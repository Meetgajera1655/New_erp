from typing import List, Dict, Any, Tuple
import math

def paginate_list(data: List[Dict[str, Any]], page: int = 1, limit: int = 10) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Paginate a list of items completely in-memory.
    Returns: (paginated_slice, total_items, total_pages)
    """
    if not data:
        return [], 0, 1

    total = len(data)
    
    # Enforce constraints
    page = max(1, page)
    limit = max(1, limit)
    limit = min(100, limit)  # hard-cap at 100 items for safety

    total_pages = math.ceil(total / limit)
    
    # Calculate slice indices
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit

    sliced_data = data[start_idx:end_idx]

    return sliced_data, total, total_pages

