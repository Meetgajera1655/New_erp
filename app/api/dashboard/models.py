from typing import Any, List, Optional, Dict, Union
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Request parameters modeling 
# ---------------------------------------------------------------------------
class TablePaginationParams(BaseModel):
    page: int = 1
    size: int = 10

# ---------------------------------------------------------------------------
# Base Response Models
# ---------------------------------------------------------------------------
class PaginationMeta(BaseModel):
    page: int
    size: int
    total: int
    total_pages: int

class TableDataPayload(BaseModel):
    id: str
    data: List[Any]
    pagination: PaginationMeta

# ---------------------------------------------------------------------------
# Exclusive Response Cases
# ---------------------------------------------------------------------------

class DashboardStandardResponse(BaseModel):
    """Returned when NO ?table query parameter is passed (Speed focused)."""
    dashboard: str
    role: str
    access: str
    modules: List[str] = []
    kpis: Optional[dict] = None
    charts: Optional[dict] = None

class DashboardTableResponseSingle(BaseModel):
    """Returned when ONE ?table query parameter is passed."""
    role: str = "CEO"
    access: str = "granted"
    modules: List[str] = []
    table: TableDataPayload

class DashboardTableResponseMultiple(BaseModel):
    """Returned when MULTIPLE ?table query parameters are passed comma-separated."""
    role: str = "CEO"
    access: str = "granted"
    modules: List[str] = []
    tables: List[TableDataPayload]

# The main endpoint router response can be a Union
DashboardResponseVariant = Union[DashboardStandardResponse, DashboardTableResponseSingle, DashboardTableResponseMultiple]
