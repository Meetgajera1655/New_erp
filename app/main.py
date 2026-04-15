from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
import traceback
import time

# =========================
# Unified Dashboard Router
# =========================
from app.api.dashboard.router import router as dashboard_router

# =========================
# Branch Management
# =========================
from app.api.branch_management import (
    kpi as bm_kpi,
    charts as bm_charts,
    tables as bm_tables,
    alerts as bm_alerts
)

# =========================
# Inventory
# =========================
from app.api.inventory import (
    kpi as inventory_kpi,
    charts as inventory_charts,
    tables as inventory_tables,
    alerts as inventory_alerts
)

# =========================
# Vendor Management
# =========================
from app.api.vendor_management import (
    kpi as vendor_kpi,
    charts as vendor_charts,
    tables as vendor_tables,
    alerts as vendor_alerts
)

# =========================
# Purchase
# =========================
from app.api.purchase import (
    kpi as purchase_kpi,
    charts as purchase_charts,
    tables as purchase_tables,
    alerts as purchase_alerts
)

# =========================
# Lead Follow-up
# =========================
from app.api.lead_followup import (
    kpi as lead_kpi,
    charts as lead_charts,
    tables as lead_tables,
    alerts as lead_alerts
)

# =========================
# Quotation
# =========================
from app.api.quotation import (
    kpi as quotation_kpi,
    charts as quotation_charts,
    tables as quotation_tables,
    alerts as quotation_alerts
)

# =========================
# GMA
# =========================
from app.api.gma import (
    kpi as gma_kpi,
    charts as gma_charts,
    tables as gma_tables,
    alerts as gma_alerts
)

# =========================
# Employee Management
# =========================
from app.api.employee_management import (
    kpi as emp_kpi,
    charts as emp_charts,
    tables as emp_tables,
    alerts as emp_alerts
)

# =========================
# Customer management
# =========================
from app.api.customer_management import kpi as cust_kpi
from app.api.customer_management import charts as cust_charts
from app.api.customer_management import tables as cust_tables
from app.api.customer_management import alerts as cust_alerts

# =========================
# Contract management
# =========================

from app.api.contract_management import kpi as contract_kpi
from app.api.contract_management import charts as contract_charts
from app.api.contract_management import tables as contract_tables
from app.api.contract_management import alerts as contract_alerts

# =========================
# Sales-order
# =========================

from app.api.sales_order import kpi as so_kpi
from app.api.sales_order import charts as so_charts
from app.api.sales_order import tables as so_tables
from app.api.sales_order import alerts as so_alerts

# =========================
# Task-Managemet
# =========================

from app.api.task_management import kpi as task_kpi
from app.api.task_management import charts as task_charts
from app.api.task_management import tables as task_tables
from app.api.task_management import alerts as task_alerts

# =========================
# Customer Support
# =========================

from app.api.customer_support import kpi as support_kpi
from app.api.customer_support import charts as support_charts
from app.api.customer_support import tables as support_tables
from app.api.customer_support import alerts as support_alerts

# =========================
# Petty Cash
# =========================

from app.api.petty_cash import kpi as petty_kpi
from app.api.petty_cash import charts as petty_charts
from app.api.petty_cash import tables as petty_tables
from app.api.petty_cash import alerts as petty_alerts

# =========================
# HRM
# =========================

from app.api.hrm import kpi as hrm_kpi
from app.api.hrm import charts as hrm_charts
from app.api.hrm import tables as hrm_tables
from app.api.hrm import alerts as hrm_alerts

# =========================
# Financial 
# =========================
from app.api.financial import (
    charts as financial_charts,
    tables as financial_tables,
    download as financial_download
)


app = FastAPI()


# =========================
# Global Error Handlers
# =========================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print("❌ ERROR:", str(exc))
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Something went wrong",
            "detail": str(exc)
        }
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "fail",
            "message": exc.detail
        }
    )

@app.exception_handler(SQLAlchemyError)
async def db_exception_handler(request: Request, exc: SQLAlchemyError):
    print("❌ DB ERROR:", str(exc))
    return JSONResponse(
        status_code=500,
        content={
            "status": "db_error",
            "message": "Database error occurred",
            "detail": str(exc)
        }
    )


# =========================
# Branch Management Routes
# =========================
app.include_router(bm_kpi.router, prefix="/api/branch-management/kpi", tags=["Branch KPI"])
app.include_router(bm_charts.router, prefix="/api/branch-management/charts", tags=["Branch Charts"])
app.include_router(bm_tables.router, prefix="/api/branch-management/tables", tags=["Branch Tables"])
app.include_router(bm_alerts.router, prefix="/api/branch-management/alerts", tags=["Branch Alerts"])


# =========================
# Inventory Routes
# =========================
app.include_router(inventory_kpi.router, prefix="/api/inventory/kpi", tags=["Inventory KPI"])
app.include_router(inventory_charts.router, prefix="/api/inventory/charts", tags=["Inventory Charts"])
app.include_router(inventory_tables.router, prefix="/api/inventory/tables", tags=["Inventory Tables"])
app.include_router(inventory_alerts.router, prefix="/api/inventory/alerts", tags=["Inventory Alerts"])


# =========================
# Vendor Management Routes
# =========================
app.include_router(vendor_kpi.router, prefix="/api/vendor-management/kpi", tags=["Vendor KPI"])
app.include_router(vendor_charts.router, prefix="/api/vendor-management/charts", tags=["Vendor Charts"])
app.include_router(vendor_tables.router, prefix="/api/vendor-management/tables", tags=["Vendor Tables"])
app.include_router(vendor_alerts.router, prefix="/api/vendor-management/alerts", tags=["Vendor Alerts"])


# =========================
# Purchase Routes
# =========================
app.include_router(purchase_kpi.router, prefix="/api/purchase/kpi", tags=["Purchase KPI"])
app.include_router(purchase_charts.router, prefix="/api/purchase/charts", tags=["Purchase Charts"])
app.include_router(purchase_tables.router, prefix="/api/purchase/tables", tags=["Purchase Tables"])
app.include_router(purchase_alerts.router, prefix="/api/purchase/alerts", tags=["Purchase Alerts"])


# =========================
# Lead Follow-up Routes
# =========================
app.include_router(lead_kpi.router, prefix="/api/lead-followup/kpi", tags=["Lead KPI"])
app.include_router(lead_charts.router, prefix="/api/lead-followup/charts", tags=["Lead Charts"])
app.include_router(lead_tables.router, prefix="/api/lead-followup/tables", tags=["Lead Tables"])
app.include_router(lead_alerts.router, prefix="/api/lead-followup/alerts", tags=["Lead Alerts"])


# =========================
# Quotation Routes
# =========================
app.include_router(quotation_kpi.router, prefix="/api/quotation/kpi", tags=["Quotation KPI"])
app.include_router(quotation_charts.router, prefix="/api/quotation/charts", tags=["Quotation Charts"])
app.include_router(quotation_tables.router, prefix="/api/quotation/tables", tags=["Quotation Tables"])
app.include_router(quotation_alerts.router, prefix="/api/quotation/alerts", tags=["Quotation Alerts"])


# =========================
# GMA Routes
# =========================
app.include_router(gma_kpi.router, prefix="/api/gma/kpi", tags=["GMA KPI"])
app.include_router(gma_charts.router, prefix="/api/gma/charts", tags=["GMA Charts"])
app.include_router(gma_tables.router, prefix="/api/gma/tables", tags=["GMA Tables"])
app.include_router(gma_alerts.router, prefix="/api/gma/alerts", tags=["GMA Alerts"])


# =========================
# Employee Management Routes
# =========================
app.include_router(emp_kpi.router, prefix="/api/employee-management/kpi", tags=["Employee KPI"])
app.include_router(emp_charts.router, prefix="/api/employee-management/charts", tags=["Employee Charts"])
app.include_router(emp_tables.router, prefix="/api/employee-management/tables", tags=["Employee Tables"])
app.include_router(emp_alerts.router, prefix="/api/employee-management/alerts", tags=["Employee Alerts"])

# =========================
# Customer Management Routes
# =========================

app.include_router(cust_kpi.router, prefix="/api/customer-management/kpi")
app.include_router(cust_charts.router, prefix="/api/customer-management/charts")
app.include_router(cust_tables.router, prefix="/api/customer-management/tables")
app.include_router(cust_alerts.router, prefix="/api/customer-management/alerts")

# =========================
# Contract Management Routes
# =========================

app.include_router(contract_kpi.router, prefix="/api/contract-management/kpi")
app.include_router(contract_charts.router, prefix="/api/contract-management/charts")
app.include_router(contract_tables.router, prefix="/api/contract-management/tables")
app.include_router(contract_alerts.router, prefix="/api/contract-management/alerts")

# =========================
# Sales-order Routes
# =========================

app.include_router(so_kpi.router, prefix="/api/sales-order/kpi", tags=["Sales Order KPI"])
app.include_router(so_charts.router, prefix="/api/sales-order/charts", tags=["Sales Order Charts"])
app.include_router(so_tables.router, prefix="/api/sales-order/tables", tags=["Sales Order Tables"])
app.include_router(so_alerts.router, prefix="/api/sales-order/alerts", tags=["Sales Order Alerts"])

# =========================
# Task-Managemet Routes
# =========================

app.include_router(task_kpi.router, prefix="/api/task-management/kpi")
app.include_router(task_charts.router, prefix="/api/task-management/charts")
app.include_router(task_tables.router, prefix="/api/task-management/tables")
app.include_router(task_alerts.router, prefix="/api/task-management/alerts")

# =========================
# Customer Support Routes
# =========================

app.include_router(support_kpi.router, prefix="/api/customer-support/kpi")
app.include_router(support_charts.router, prefix="/api/customer-support/charts")
app.include_router(support_tables.router, prefix="/api/customer-support/tables")
app.include_router(support_alerts.router, prefix="/api/customer-support/alerts")

# =========================
# Petty Cash Routes
# =========================

app.include_router(petty_kpi.router, prefix="/api/petty-cash/kpi")
app.include_router(petty_charts.router, prefix="/api/petty-cash/charts")
app.include_router(petty_tables.router, prefix="/api/petty-cash/tables")
app.include_router(petty_alerts.router, prefix="/api/petty-cash/alerts")

# =========================
# HRM Routes
# =========================

app.include_router(hrm_kpi.router, prefix="/api/hrm/kpi")
app.include_router(hrm_charts.router, prefix="/api/hrm/charts")
app.include_router(hrm_tables.router, prefix="/api/hrm/tables")
app.include_router(hrm_alerts.router, prefix="/api/hrm/alerts")

# =========================
# Financial Routes 
# =========================
app.include_router(financial_charts.router, prefix="/api/financial/charts", tags=["Financial Charts"])
app.include_router(financial_tables.router, prefix="/api/financial/tables", tags=["Financial Tables"])
app.include_router(financial_download.router, prefix="/api/financial/download", tags=["Financial Download"])


# =========================
# Unified Dashboard Route
# (replaces all per-module kpi/charts/tables/alerts routes over time)
# GET /api/dashboard/{dashboard_name}
# =========================
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["Unified Dashboard"])
