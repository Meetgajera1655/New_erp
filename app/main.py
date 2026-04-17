from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
import traceback
import time

# =========================
# Unified Dashboard Router
# =========================
from app.api.dashboard.router import router as dashboard_router

# =========================
# NOTE: Legacy per-module API folders (branch_management, inventory, etc.)
# do not exist under app/api/. All dashboard functionality is served through
# the Unified Dashboard Router below. These imports are commented out to
# prevent startup failures.
# =========================


app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
# Legacy per-module routes are commented out (modules not present in app/api/).
# All dashboard endpoints are served through the Unified Dashboard Router below.
# =========================


# =========================
# Unified Dashboard Route
# (replaces all per-module kpi/charts/tables/alerts routes over time)
# GET /api/dashboard/{dashboard_name}
# =========================
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["Unified Dashboard"])
