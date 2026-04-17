from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_petty_cash_filters(
    alias: str,
    date_column: str,
    branch: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None
) -> Tuple[str, Dict[str, Any]]:
    """
    Generates dynamic SQL WHERE clause and parameters for the Petty Cash dashboard.
    Supports branch filtering and custom business date columns (e.g., submitted_at).
    Uses +1 day date boundaries.
    """
    conditions = []
    params = {}

    # 1. Branch filter
    if branch:
        # For petty_cash_requests, the branch ID is typically requester_branch_id
        conditions.append(f"{alias}.requester_branch_id = ANY(:branches)")
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
