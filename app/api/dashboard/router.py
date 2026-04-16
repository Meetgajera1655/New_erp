import asyncio
from typing import Optional, List
from datetime import datetime

import os
import tempfile
from fastapi import APIRouter, Depends, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import FileResponse
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


@router.get("/financial/export/excel")
async def export_financial_excel(
    request: Request,
    background_tasks: BackgroundTasks,
    branch: List[str] = Query(default=[]),
    from_date: Optional[str] = Query(default=None),
    to_date: Optional[str] = Query(default=None),
    period: Optional[str] = Query(default=None),
    auth_ctx: dict = Depends(verify_dashboard_access),
    db: Session = Depends(get_db),
):
    """
    Export financial tables to a multi-sheet Excel file.
    """
    import pandas as pd

    if from_date and not to_date:
        raise HTTPException(status_code=400, detail="to_date is required when from_date is provided")
    if to_date and not from_date:
        raise HTTPException(status_code=400, detail="from_date is required when to_date is provided")
    try:
        if from_date: datetime.strptime(from_date, "%Y-%m-%d")
        if to_date: datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    tenant = auth_ctx["tenant"]
    allowed_modules = auth_ctx.get("allowed_modules", [])

    adapter = get_adapter("financial")
    if not adapter:
        raise HTTPException(status_code=404, detail="Financial dashboard adapter not found.")

    all_tables_dict = adapter.get_tables(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)

    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for table_id, data in all_tables_dict.items():
            sheet_name = table_id.replace("_", " ").title()[:31]
            if data:
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                pd.DataFrame({"No Data": []}).to_excel(writer, sheet_name=sheet_name, index=False)

    background_tasks.add_task(os.remove, path)
    return FileResponse(path, filename="financial_export.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.get("/financial/export/pdf")
async def export_financial_pdf(
    request: Request,
    background_tasks: BackgroundTasks,
    branch: List[str] = Query(default=[]),
    from_date: Optional[str] = Query(default=None),
    to_date: Optional[str] = Query(default=None),
    period: Optional[str] = Query(default=None),
    auth_ctx: dict = Depends(verify_dashboard_access),
    db: Session = Depends(get_db),
):
    """
    Export financial charts to a PDF document.
    """
    import plotly.express as px
    import plotly.graph_objects as go
    from reportlab.platypus import SimpleDocTemplate, Image as RLImage, Spacer, Paragraph
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet

    if from_date and not to_date:
        raise HTTPException(status_code=400, detail="to_date is required when from_date is provided")
    if to_date and not from_date:
        raise HTTPException(status_code=400, detail="from_date is required when to_date is provided")
    try:
        if from_date: datetime.strptime(from_date, "%Y-%m-%d")
        if to_date: datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    tenant = auth_ctx["tenant"]
    allowed_modules = auth_ctx.get("allowed_modules", [])

    adapter = get_adapter("financial")
    if not adapter:
        raise HTTPException(status_code=404, detail="Financial dashboard adapter not found.")

    charts_data = adapter.get_charts(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)

    temp_dir = tempfile.mkdtemp()

    def cleanup():
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    background_tasks.add_task(cleanup)

    def is_chart_empty(chart_name, chart_data):
        if isinstance(chart_data, list):
            return len(chart_data) == 0
        if isinstance(chart_data, dict):
            return all(v == 0 or v is None for v in chart_data.values())
        return True

    pdf_path = os.path.join(temp_dir, "financial_charts.pdf")
    doc = SimpleDocTemplate(pdf_path, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()

    def add_fig_to_pdf(fig, title):
        img_path = os.path.join(temp_dir, f"{title}.png")
        fig.write_image(img_path, engine="kaleido", width=800, height=400)
        elements.append(Paragraph(title.replace('_', ' ').title(), styles['Heading2']))
        elements.append(Spacer(1, 10))
        elements.append(RLImage(img_path, width=500, height=250))
        elements.append(Spacer(1, 20))

    for chart_name, data_val in charts_data.items():
        fig = None
        
        # Always create figure - let Plotly handle empty datasets naturally
        
        # Specific configurations for known charts
        if chart_name == "country_revenue":
            if data_val:
                fig = px.line(data_val, x="date", y="revenue", title="Revenue Over Time", markers=True)
            else:
                fig = go.Figure()
                fig.update_layout(title="Revenue Over Time", xaxis_title="date", yaxis_title="revenue")
        elif chart_name == "branch_revenue":
            if data_val:
                fig = px.bar(data_val, x="branch_id", y="revenue", color="branch_id", title="Branch Revenue")
            else:
                fig = go.Figure()
                fig.update_layout(title="Branch Revenue", xaxis_title="branch_id", yaxis_title="revenue")
        elif chart_name == "revenue_breakup":
            if data_val:
                fig = px.pie(data_val, values="value", names="type", title="Revenue Breakup")
            else:
                fig = go.Figure()
                fig.update_layout(title="Revenue Breakup")
        elif chart_name == "collection_vs_outstanding":
            if isinstance(data_val, dict):
                collected = data_val.get("collected", 0)
                outstanding = data_val.get("outstanding", 0)
                data = [
                    {"status": "Collected", "amount": collected},
                    {"status": "Outstanding", "amount": outstanding}
                ]
                fig = px.bar(data, x="status", y="amount", color="status", title="Collection vs Outstanding")
        elif chart_name == "invoice_status":
            if data_val:
                fig = px.pie(data_val, values="count", names="status", title="Invoice Status")
            else:
                fig = go.Figure()
                fig.update_layout(title="Invoice Status")
        elif chart_name == "employee_growth":
            if data_val:
                fig = px.line(data_val, x="date", y="count", title="Employee Growth Over Time", markers=True)
            else:
                fig = go.Figure()
                fig.update_layout(title="Employee Growth Over Time", xaxis_title="date", yaxis_title="count")
        elif chart_name == "technician_productivity":
            if data_val:
                fig = px.bar(data_val, x="technician_name", y="productivity", color="technician_name", title="Technician Productivity")
            else:
                fig = go.Figure()
                fig.update_layout(title="Technician Productivity", xaxis_title="technician_name", yaxis_title="productivity")
        else:
            # Generic fallback for any other new charts added to the service
            if isinstance(data_val, list):
                if len(data_val) > 0:
                    keys = list(data_val[0].keys())
                    x_val = keys[0]
                    y_val = keys[1] if len(keys) > 1 else keys[0]
                    # Default to bar graph
                    fig = px.bar(data_val, x=x_val, y=y_val, title=chart_name.replace("_", " ").title())
                else:
                    # Empty list - create empty bar chart
                    fig = go.Figure()
                    fig.update_layout(title=chart_name.replace("_", " ").title())
            elif isinstance(data_val, dict):
                # For dict-based charts, create a simple bar chart with the values
                names = list(data_val.keys())
                values = list(data_val.values())
                fig = px.bar(x=names, y=values, title=chart_name.replace("_", " ").title())
        
        if fig:
            add_fig_to_pdf(fig, chart_name)

    if elements:
        doc.build(elements)
    else:
        # Avoid empty document error
        elements.append(Paragraph("No chart data available for the given filters.", styles['Normal']))
        doc.build(elements)

    return FileResponse(pdf_path, filename="financial_charts.pdf", media_type="application/pdf")


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
