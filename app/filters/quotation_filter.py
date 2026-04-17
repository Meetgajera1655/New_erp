from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_quotation_filters(
    alias: str = "q",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    date_column: str = "created_at",
    **kwargs
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates dynamic SQL WHERE clause and parameters for the Quotation dashboard.
    Uses created_at (datetime) with +1 day fix.
    Branch filtering has been removed as per requirement.
    """
    conditions = []
    params = {}

    # 1. Date/Period Filter
    if period:
        # Standard 30 day period as per requirement
        conditions.append(f"{alias}.{date_column} >= CURRENT_DATE - INTERVAL '30 days'")
        
    elif from_date and to_date:
        try:
            # handle +1 day in Python for inclusive end date
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)

            conditions.append(f"{alias}.{date_column} >= :from_date AND {alias}.{date_column} < :to_date")
            params["from_date"] = from_date_obj
            params["to_date"] = to_date_obj
        except (ValueError, TypeError):
            pass

    # Combine conditions
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params
