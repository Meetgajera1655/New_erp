from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

def apply_branch_management_filters(
    schema: str = "public",
    alias: str = "b",
    branch: Optional[List[str]] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    **kwargs
) -> Tuple[str, Dict[str, Any]]:
    conditions = []
    params = {}

    if branch:
        conditions.append(f"{alias}.id = ANY(:branches)")
        params["branches"] = branch

    if period:
        # Construct period logic mapping (assuming period format matches PostgreSQL interval)
        if period.endswith('d') or period.endswith('D'):
            interval = f"{period[:-1]} days"
        elif period.endswith('m') or period.endswith('M'):
            interval = f"{period[:-1]} months"
        elif period.endswith('y') or period.endswith('Y'):
            interval = f"{period[:-1]} years"
        else:
            interval = period
            
        conditions.append(f"{alias}.created_at >= CURRENT_DATE - INTERVAL '{interval}'")
        
    elif from_date and to_date:
        try:
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
            f_to_date = to_date_obj.strftime("%Y-%m-%d")

            conditions.append(f"{alias}.created_at >= :from_date AND {alias}.created_at < :to_date")
            params["from_date"] = from_date
            params["to_date"] = f_to_date
        except (ValueError, TypeError):
            pass

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    return where_clause, params
