from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_hrm_filters(
    alias: str,
    date_column: str,
    branch: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    branch_alias: Optional[str] = None
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates dynamic SQL WHERE clause and parameters for the HRM dashboard.
    Supports branch filtering and custom business date columns.
    Uses +1 day date boundaries.
    """
    conditions = []
    params = {}
    
    # Use branch_alias if provided, otherwise fallback to alias
    b_alias = branch_alias if branch_alias else alias

    # 1. Branch filter
    if branch:
        conditions.append(f"{b_alias}.branch_id = ANY(:branches)")
        params["branches"] = branch

    # 2. Period filter
    if period:
        conditions.append(
            f"{alias}.{date_column} >= CURRENT_DATE - INTERVAL '30 days'"
        )

    # 3. Date filter (+1 FIX)
    elif from_date and to_date:
        try:
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)

            conditions.append(
                f"{alias}.{date_column} >= :from_date AND {alias}.{date_column} < :to_date"
            )

            params["from_date"] = from_date_obj
            params["to_date"] = to_date_obj
        except (ValueError, TypeError):
            pass

    return " AND ".join(conditions) if conditions else "1=1", params
