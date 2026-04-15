from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_gma_filters(
    alias: str = "g",
    branch: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    date_column: str = "created_at",
    branch_alias: Optional[str] = None,
    **kwargs
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates dynamic SQL WHERE clause and parameters for the GMA dashboard.
    Uses created_at (datetime) with +1 day fix and NO branch normalization.
    """
    conditions = []
    params = {}

    # 1. Branch Filter (No Normalization)
    if branch:
        b_alias = branch_alias if branch_alias else alias
        conditions.append(f"{b_alias}.branch_id = ANY(:branches)")
        params["branches"] = branch

    # 2. Date/Period Filter
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
        conditions.append(f"{alias}.{date_column} >= CURRENT_DATE - INTERVAL '{interval}'")
        
    elif from_date and to_date:
        try:
            # handle +1 day in Python for inclusive end date
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)

            conditions.append(f"{alias}.{date_column} >= :from_date AND {alias}.{date_column} < :to_date")
            params["from_date"] = from_date_obj
            params["to_date"] = to_date_obj
        except (ValueError, TypeError):
            # Fallback if invalid date
            pass

    # Combine conditions
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params
