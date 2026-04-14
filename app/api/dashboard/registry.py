import abc
from typing import Dict, Any

from app.services.branch_management_service import BranchManagementService
from app.services.financial_service import FinancialService
from app.api.dashboard.cache import ttl_cache

class BaseDashboardAdapter(abc.ABC):
    """
    Abstract adapter defining the expected interface for any dashboard.
    Concrete adapters should map these methods to the underlying existing Service 
    to prevent duplication of business logic or SQL queries.
    """
    
    @abc.abstractmethod
    def get_kpis(self, db, schema: str) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def get_charts(self, db, schema: str) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def get_tables(self, db, schema: str) -> Dict[str, Any]:
        pass

    def get_alerts(self, db, schema: str) -> Dict[str, Any]:
        return {}


# ---------------------------------------------------------------------------
# Example Integration: Branch Management
# Note: Handlers are wrapped with caching logic locally at the adapter 
# boundary to avoid polluting underlying pure service functions.
# ---------------------------------------------------------------------------
class BranchManagementAdapter(BaseDashboardAdapter):
    
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str) -> Dict[str, Any]:
        return BranchManagementService.get_dashboard_kpi(db, schema)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str) -> Dict[str, Any]:
        return BranchManagementService.get_branch_charts(db, schema)
        
    def get_tables(self, db, schema: str) -> Dict[str, Any]:
        return BranchManagementService.get_branch_tables(db, schema)

    def get_alerts(self, db, schema: str) -> Dict[str, Any]:
        return BranchManagementService.get_branch_alerts(db, schema)


class FinancialAdapter(BaseDashboardAdapter):
    
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str) -> Dict[str, Any]:
        return {}  # Financial service has no explicit KPIs block
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str) -> Dict[str, Any]:
        return FinancialService.get_charts(db, schema)
        
    def get_tables(self, db, schema: str) -> Dict[str, Any]:
        return FinancialService.get_tables(db, schema)


# ---------------------------------------------------------------------------
# Central Registry Map
# ---------------------------------------------------------------------------
REGISTRY: Dict[str, BaseDashboardAdapter] = {
    "branch_management": BranchManagementAdapter(),
    "financial": FinancialAdapter(),
    # To migrate another module, simply create a minimal adapter like above 
    # and link its string identifier here. No SQL modifications required!
}

def get_adapter(dashboard_name: str) -> BaseDashboardAdapter | None:
    """Safely retrieves the adapter instance mapping to isolated logic."""
    return REGISTRY.get(dashboard_name.lower())
