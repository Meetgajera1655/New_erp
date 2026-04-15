import asyncio
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.utils.auth import get_current_tenant, verify_dashboard_access

from app.api.dashboard.registry import get_adapter
from app.api.dashboard.models import (
    DashboardResponseVariant,
    DashboardStandardResponse,
    DashboardTableResponseSingle,
    DashboardTableResponseMultiple,
    TableDataPayload,
    PaginationMeta,
    TablePaginationParams
)
from app.api.dashboard.pagination import paginate_list

router = APIRouter()

def _extract_pagination_for_table(raw_params: dict, table_id: str) -> TablePaginationParams:
    """
    Checks for table-specific pagination first (e.g. `recent_orders_page`).
    Falls back to global `page` and `size` parameters.
    """
    page_key = f"{table_id}_page"
    size_key = f"{table_id}_size"
    
    if page_key in raw_params or size_key in raw_params:
        page = int(raw_params.get(page_key, 1))
        size = int(raw_params.get(size_key, 10))
    else:
        page = int(raw_params.get("page", 1))
        size = int(raw_params.get("size", 10))
    
    return TablePaginationParams(page=page, size=size)


@router.get("/{dashboard_name}", response_model=DashboardResponseVariant)
async def get_unified_dashboard(
    dashboard_name: str,
    request: Request,
    table: Optional[List[str]] = Query(
        default=None,
        description="List of table IDs. Supports multiple ?table= params or comma-separated strings."
    ),
    branch: List[str] = Query(default=[]),
    from_date: Optional[str] = Query(default=None),
    to_date: Optional[str] = Query(default=None),
    period: Optional[str] = Query(default=None),
    auth_ctx: dict = Depends(verify_dashboard_access),
    db: Session = Depends(get_db),
):
    """
    Unified Orchestrating Endpoint for dashboard loading.
    Strict Mode: Multi-role DB check against dashboard_name via verify_dashboard_access.
    """
    # 1. Validation Logic
    print("from_date:", from_date)
    print("to_date:", to_date)

    if from_date and not to_date:
        raise HTTPException(status_code=400, detail="to_date is required when from_date is provided")
    
    if to_date and not from_date:
        raise HTTPException(status_code=400, detail="from_date is required when to_date is provided")

    try:
        if from_date:
            datetime.strptime(from_date, "%Y-%m-%d")
        if to_date:
            datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    tenant = auth_ctx["tenant"]
    role = auth_ctx["role"]
    allowed_modules = auth_ctx.get("allowed_modules", [])
    
    adapter = get_adapter(dashboard_name)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"Dashboard '{dashboard_name}' is not registered.")

    # ------------------------------------------------------------------ #
    # BRANCH A: Exclusive Table Request
    # ------------------------------------------------------------------ #
    if table:
        # Normalize table request: handles both ?table=a&table=b AND ?table=a,b
        requested_ids = []
        for item in table:
            requested_ids.extend([t.strip() for t in item.split(",") if t.strip()])

        if not requested_ids:
             # Fallback to standard flow if empty list resulted from splitting
             pass 
        else:
            # 1. Fetch raw table sets from the existing service layer
            # (This bypasses KPIs & Charts execution entirely)
            all_tables_dict = adapter.get_tables(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)
            
            raw_params = dict(request.query_params)
            payloads = []
            
            # 2. Iterate, slice, and form standard payload
            for tid in requested_ids:
                if tid not in all_tables_dict:
                    continue # Safely ignore requested tables that don't exist
                
                raw_data_list = all_tables_dict[tid]
                pagination_params = _extract_pagination_for_table(raw_params, tid)
                
                sliced_data, total_items, total_pages = paginate_list(
                    data=raw_data_list, 
                    page=pagination_params.page, 
                    limit=pagination_params.size
                )
                
                payloads.append(
                    TableDataPayload(
                        id=tid,
                        data=sliced_data,
                        pagination=PaginationMeta(
                            page=pagination_params.page,
                            size=pagination_params.size,
                            total=total_items,
                            total_pages=total_pages
                        )
                    )
                )

            # Response routing based on payload count
            if not payloads:
                raise HTTPException(status_code=404, detail="None of the requested tables were found.")
                
            if len(payloads) == 1:
                return DashboardTableResponseSingle(role=role, access="granted", modules=allowed_modules, table=payloads[0])
            else:
                return DashboardTableResponseMultiple(role=role, access="granted", modules=allowed_modules, tables=payloads)

    # ------------------------------------------------------------------ #
    # BRANCH B: Default Dashboard Load (KPIs + Charts Only)
    # ------------------------------------------------------------------ #
    else:
        # Note: We run these sequentially because SQLAlchemy Session is NOT thread-safe.
        # Passing `db` to different threads via `asyncio.to_thread` causes IllegalStateChangeError.
        # The TTL caching we added drops response times to <2s on subsequent loads anyway.
        kpis = adapter.get_kpis(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)
        charts = adapter.get_charts(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)
        
        return DashboardStandardResponse(
            dashboard=dashboard_name,
            role=role,
            access="granted",
            modules=allowed_modules,
            kpis=kpis,
            charts=charts
        )
