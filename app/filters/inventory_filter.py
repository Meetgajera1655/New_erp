from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

INVENTORY_FILTER_CONFIG = {
    "ip": {
        "table": "inventory_products",
        "date": "created_at"
    },
    "sl": {
        "table": "stock_ledger",
        "branch": "branch_id",
        "date": "created_at"
    },
    "sml": {
        "table": "stock_movement_logs",
        "branch": "branch_id",
        "date": "created_at"
    },
    "cse": {
        "table": "central_stock_entries",
        "branch": "branch_id",
        "date": "created_at"
    }
}

def apply_dashboard_filters(
    branch: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    aliases: List[str] = None,
    **kwargs
) -> Tuple[str, Dict[str, Any]]:
    conditions = []
    params = {}

    if not aliases:
        return "1=1", {}

    if branch:
        schema = kwargs.get("schema", "public")
        for alias in aliases:
            if alias in INVENTORY_FILTER_CONFIG and "branch" in INVENTORY_FILTER_CONFIG[alias]:
                col = INVENTORY_FILTER_CONFIG[alias]["branch"]
                conditions.append(f"{alias}.{col} = ANY(:branches)")
        if conditions:
            params["branches"] = branch

    if period:
        # Construct period logic mapping (assuming period format matches PostgreSQL interval)
        if period.endswith('D'):
            interval = f"{period[:-1]} days"
        elif period.endswith('M'):
            interval = f"{period[:-1]} months"
        elif period.endswith('Y'):
            interval = f"{period[:-1]} years"
        else:
            interval = period
            
        for alias in aliases:
            if alias in INVENTORY_FILTER_CONFIG and "date" in INVENTORY_FILTER_CONFIG[alias]:
                col = INVENTORY_FILTER_CONFIG[alias]["date"]
                conditions.append(f"{alias}.{col} >= CURRENT_DATE - INTERVAL '{interval}'")
                
    elif from_date and to_date:
        try:
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            f_to_date = to_date_obj.strftime("%Y-%m-%d")
            
            for alias in aliases:
                if alias in INVENTORY_FILTER_CONFIG and "date" in INVENTORY_FILTER_CONFIG[alias]:
                    col = INVENTORY_FILTER_CONFIG[alias]["date"]
                    conditions.append(f"{alias}.{col} >= :from_date AND {alias}.{col} < :to_date")
            
            if conditions:
                params["from_date"] = from_date
                params["to_date"] = f_to_date
        except (ValueError, TypeError):
            # Fallback for malformed dates
            pass

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params
