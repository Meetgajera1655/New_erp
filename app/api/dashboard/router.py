import asyncio
from typing import Optional, List
from datetime import datetime

import os
import tempfile
from fastapi import APIRouter, Depends, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import FileResponse
from app.utils.excel_format import format_excel
from sqlalchemy.orm import Session

from app.database import get_db
from app.utils.auth import get_current_tenant, verify_dashboard_access

from app.services.branch_management_service import BranchManagementService

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
from app.api.dashboard.table_schemas import TABLE_SCHEMAS

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


TABLE_COLUMN_MAPPING = {
    "stock_movements": {
        "reference_type": "Reference Type",
        "reference_id": "Reference ID",
        "product_id": "Product ID",
        "product_name": "Product Name",
        "branch_name": "Branch Name",
        "stock_type": "Stock Type",
        "quantity_delta": "Quantity",
        "action": "Action",
        "created_by": "Created By",
        "created_at": "Created At"
    },
    "low_stock_table": {
        "product_name": "Product Name",
        "product_code": "Product Code",
        "branch_name": "Branch Name",
        "category": "Category",
        "assets_qty": "Assets Qty",
        "consumable_qty": "Consumable Qty",
        "resell_qty": "Resell Qty",
        "status": "Status"
    }
}

COLUMN_ORDER = {
    "stock_movements": [
        "Reference Type", "Reference ID", "Product ID", "Product Name",
        "Branch Name", "Stock Type", "Quantity", "Action",
        "Created By", "Created At"
    ],
    "low_stock_table": [
        "Product Name", "Product Code", "Branch Name", "Category", 
        "Assets Qty", "Consumable Qty", "Resell Qty", "Status"
    ]
}

@router.get("/{dashboard_name}/export/excel")
async def export_dashboard_excel(
    dashboard_name: str,
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
    Generic Multi-Sheet Excel Export
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

    adapter = get_adapter(dashboard_name)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"Dashboard adapter '{dashboard_name}' not found.")

    all_tables_dict = adapter.get_tables(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)

    if not all_tables_dict:
        all_tables_dict = {"No Data Available": []}

    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)


    sheet_titles = {}
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        
        # 1. Main Summary Sheet
        summary_data = []
        for table_id, data in all_tables_dict.items():
            summary_data.append({
                "Table Name": table_id.replace("_", " ").title(),
                "Total Records": len(data) if isinstance(data, list) else 0
            })
        
        if summary_data:
            df_summary = pd.DataFrame(summary_data)
        else:
            df_summary = pd.DataFrame(columns=["Table Name", "Total Records"])
            
        sheet_titles["Summary"] = "Main Summary"
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

        # 2. Iterate each table
        for table_id, data in all_tables_dict.items():
            sheet_name = table_id.replace("_", " ").title()[:31]
            sheet_titles[sheet_name] = table_id.replace("_", " ").title()
            
            if data and len(data) > 0:
                df = pd.DataFrame(data)
            else:
                # 3. Ensure headers always exist
                if table_id in COLUMN_ORDER:
                    columns = COLUMN_ORDER[table_id]
                elif table_id in TABLE_COLUMN_MAPPING:
                    columns = list(TABLE_COLUMN_MAPPING[table_id].values())
                elif table_id in TABLE_SCHEMAS:
                    columns = [str(c).replace("_", " ").title() for c in TABLE_SCHEMAS[table_id]]
                else:
                    columns = ["No Data Available"]
                df = pd.DataFrame(columns=columns)
            
            if not df.empty or len(df.columns) > 0:
                # 4. Remove internal fields globally
                for internal_col in ["branch_id", "id", "_id"]:
                    if internal_col in df.columns:
                        df = df.drop(columns=[internal_col])
                
                # 5. Apply central column mapping
                if table_id in TABLE_COLUMN_MAPPING:
                    df = df.rename(columns=TABLE_COLUMN_MAPPING[table_id])
                else:
                    # Fallback dynamic title case
                    df.columns = [str(c).replace("_", " ").title() for c in df.columns]

                # 6. Apply column ordering
                if table_id in COLUMN_ORDER:
                    cols = [c for c in COLUMN_ORDER[table_id] if c in df.columns]
                    df = df[cols]
                
                # 7. Null handling
                df = df.fillna("-")
                
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Apply formatting
    format_excel(path, sheet_titles)

    filename = f"{dashboard_name}_export.xlsx"
    background_tasks.add_task(os.remove, path)
    return FileResponse(path, filename=filename, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@router.get("/{dashboard_name}/export/pdf")
async def export_dashboard_pdf(
    dashboard_name: str,
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
    Generic PDF Chart Export
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

    adapter = get_adapter(dashboard_name)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"Dashboard adapter '{dashboard_name}' not found.")

    charts_data = adapter.get_charts(db, tenant, allowed_modules, branch=branch, from_date=from_date, to_date=to_date, period=period)

    temp_dir = tempfile.mkdtemp()
    def cleanup():
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    background_tasks.add_task(cleanup)

    pdf_path = os.path.join(temp_dir, f"{dashboard_name}_charts.pdf")
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
        
        # Specific configurations for known charts
        if dashboard_name == "financial":
            if chart_name == "country_revenue":
                if data_val:
                    fig = px.line(data_val, x="date", y="revenue", title="Revenue Over Time", markers=True)
                else:
                    fig = go.Figure()
                    fig.update_layout(title="Revenue Over Time", xaxis_title="date", yaxis_title="revenue")
            elif chart_name == "branch_revenue":
                if data_val:
                    fig = px.bar(data_val, x="branch_name", y="revenue", color="branch_name", title="Branch Revenue")
                else:
                    fig = go.Figure()
                    fig.update_layout(title="Branch Revenue", xaxis_title="branch_name", yaxis_title="revenue")
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
            elif chart_name == "chemical_consumption":
                if data_val:
                    fig = px.bar(data_val, x="product_name", y="total_consumption", title="Chemical Consumption Trend")
                else:
                    fig = go.Figure()
                    fig.update_layout(title="Chemical Consumption Trend", xaxis_title="product_name", yaxis_title="total_consumption")
                    
        elif dashboard_name == "inventory":
            if chart_name == "stock_by_category":
                if data_val:
                    fig = px.bar(data_val, x="category", y="total_stock", title="Stock by Category")
                else:
                    fig = go.Figure()
                    fig.update_layout(title="Stock by Category")
            elif chart_name == "stock_by_type":
                if isinstance(data_val, dict):
                    pie_data = [{"type": k, "count": v} for k, v in data_val.items()]
                    fig = px.pie(pie_data, values="count", names="type", title="Stock by Type")
            elif chart_name == "branch_stock":
                if data_val:
                    fig = px.bar(data_val, x="branch_id", y="total_stock", title="Stock by Branch")
            elif chart_name == "stock_movement_trend":
                if data_val:
                    fig = px.line(data_val, x="date", y="movement", title="Stock Movement Trend", markers=True)
            elif chart_name == "inventory_value_by_category":
                if data_val:
                    fig = px.bar(data_val, x="category", y="value", title="Inventory Value by Category")
            elif chart_name == "monthly_stock_comparison":
                if data_val:
                    fig = px.bar(data_val, x="month", y=["assets", "consumables", "resell"], barmode="group", title="Monthly Stock Comparison")

        elif dashboard_name == "vendor":
            if chart_name == "vendors_by_category":
                if data_val:
                    fig = px.bar(data_val, x="vendor_category", y="count", title="Vendors by Category")
            elif chart_name == "contract_status_split":
                if data_val:
                    fig = px.pie(data_val, values="count", names="has_contract", title="Contract Status Split")
            elif chart_name == "rating_distribution":
                if data_val:
                    fig = px.bar(data_val, x="vendor_rating", y="count", title="Rating Distribution")

        elif dashboard_name == "purchase":
            if chart_name == "po_status":
                if data_val:
                    fig = px.pie(data_val, values="count", names="status", title="Purchase Order Status")
            elif chart_name == "vendor_spending":
                if data_val:
                    fig = px.bar(data_val, x="vendor", y="amount", color="vendor", title="Vendor Spending")
            elif chart_name == "daily_po":
                if data_val:
                    fig = px.line(data_val, x="date", y="count", title="Daily Purchase Orders", markers=True)
            elif chart_name == "monthly_purchase_value":
                if data_val:
                    fig = px.bar(data_val, x="month", y="total", title="Monthly Purchase Order Value")

        elif dashboard_name == "gma":
            if chart_name == "status_distribution":
                if data_val:
                    fig = px.pie(data_val, values="count", names="status", title="GMA Status Distribution")
            elif chart_name == "branch_gma":
                if data_val:
                    fig = px.bar(data_val, x="branch", y="count", title="GMA by Branch")
            elif chart_name == "monthly_gma":
                if data_val:
                    fig = px.line(data_val, x="month", y="count", title="Monthly GMA Creation", markers=True)
            elif chart_name == "monthly_gma_value":
                if data_val:
                    fig = px.bar(data_val, x="month", y=["total_cost", "total_price", "avg_margin"], barmode="group", title="Monthly GMA Value (Cost vs Price vs Margin)")

        elif dashboard_name == "task":
            if chart_name == "status_chart":
                if data_val:
                    fig = px.pie(data_val, values="count", names="status", title="Task Status Distribution")
            elif chart_name == "monthly_trend":
                if data_val:
                    fig = px.line(data_val, x="month", y="count", title="Monthly Task Volume", markers=True)
            elif chart_name == "technician_workload":
                if data_val:
                    fig = px.bar(data_val, x="technician", y="count", title="Technician Workload")

        elif dashboard_name == "contract":
            if chart_name == "status_distribution":
                if data_val:
                    fig = px.pie(data_val, values="count", names="status", title="Contract Status Distribution")
            elif chart_name == "branch_contracts":
                if data_val:
                    fig = px.bar(data_val, x="branch", y="count", title="Contracts by Branch")
            elif chart_name == "monthly_value":
                if data_val:
                    fig = px.line(data_val, x="month", y="value", title="Monthly Contract Value", markers=True)
            elif chart_name == "monthly_contract_revenue":
                if data_val:
                    fig = px.bar(data_val, x="month", y="total_revenue", title="Monthly Contract Revenue")

        # Fallback for any other new charts added to any service
        if not fig:
            if isinstance(data_val, list) and len(data_val) > 0:
                keys = list(data_val[0].keys())
                x_val = keys[0]
                y_val = keys[1] if len(keys) > 1 else keys[0]
                fig = px.bar(data_val, x=x_val, y=y_val, title=chart_name.replace("_", " ").title())
            elif isinstance(data_val, dict):
                names = list(data_val.keys())
                values = list(data_val.values())
                fig = px.bar(x=names, y=values, title=chart_name.replace("_", " ").title())

        if fig:
            add_fig_to_pdf(fig, chart_name)

    if not elements:
        elements.append(Paragraph("No chart data available for the given filters.", styles['Normal']))
    
    doc.build(elements)
    filename = f"{dashboard_name}_charts.pdf"
    return FileResponse(pdf_path, filename=filename, media_type="application/pdf")




@router.get("/branches")
async def get_branches(
    tenant: str = Depends(get_current_tenant),  #  tenant from token
    db: Session = Depends(get_db),
):
    """
    Auth required (token), but NO module/permission restriction.
    """
    branches = BranchManagementService.get_all_branches(db, tenant)
    return {"branches": branches}


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
                raw_data_list = []
                
                if tid in all_tables_dict:
                    raw_data_list = all_tables_dict[tid]
                
                # If table is empty OR missing from service response, inject null row from schema
                if not raw_data_list and tid in TABLE_SCHEMAS:
                    raw_data_list = [{col: None for col in TABLE_SCHEMAS[tid]}]
                
                # If still empty and not in schema, we skip it
                if not raw_data_list and tid not in all_tables_dict:
                    continue
                
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