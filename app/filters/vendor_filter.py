from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_vendor_filters(
    alias: str = "v",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    **kwargs
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates dynamic SQL WHERE clause and parameters for the Vendor dashboard.
    Includes the +1 day logic for the to_date parameter.
    """
    conditions = []
    params = {}

    # 1. Date/Period Filter
    if period:
        # Period mapping to PostgreSQL intervals
        period_map = {
            "7d": "7 days",
            "30d": "30 days",
            "1m": "1 month",
            "3m": "3 months",
            "6m": "6 months",
            "1y": "1 year"
        }
        interval = period_map.get(period.lower(), "30 days")
        conditions.append(f"{alias}.created_at >= CURRENT_DATE - INTERVAL '{interval}'")
        
    elif from_date and to_date:
        try:
            # handle +1 day in Python for inclusive end date
            target_to_date = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            f_to_date = target_to_date.strftime("%Y-%m-%d")

            conditions.append(f"{alias}.created_at >= :from_date AND {alias}.created_at < :to_date")
            params["from_date"] = from_date
            params["to_date"] = f_to_date
        except (ValueError, TypeError):
            # Fallback if invalid date
            pass

    # Combine conditions
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params
