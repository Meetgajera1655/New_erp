import abc
from typing import Dict, Any

from app.api.dashboard.cache import ttl_cache

# --- Service Imports ---
from app.services.inventory_service import InventoryService
from app.services.branch_management_service import BranchManagementService
from app.services.sales_order_service import SalesOrderService
from app.services.vendor_service import VendorService
from app.services.purchase_service import PurchaseService
from app.services.lead_service import LeadService
from app.services.quotation_service import QuotationService
from app.services.customer_service import CustomerService
from app.services.contract_service import ContractService
from app.services.support_service import SupportService
from app.services.gma_service import GMAService
from app.services.task_service import TaskService
from app.services.employee_service import EmployeeService
from app.services.hrm_service import HRMService
from app.services.petty_cash_service import PettyCashService
from app.services.financial_service import FinancialService

class BaseDashboardAdapter(abc.ABC):
    """Abstract adapter defining the expected interface for any dashboard."""
    
    @abc.abstractmethod
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        pass

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return {}


# ---------------------------------------------------------------------------
# Dynamic Standard Adapter (Used for the 12 consistently named services)
# ---------------------------------------------------------------------------
class StandardAdapter(BaseDashboardAdapter):
    def __init__(self, service_class):
        self.service = service_class

    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return getattr(self.service, "get_kpi", lambda d, s: {})(db, schema)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return getattr(self.service, "get_charts", lambda d, s: {})(db, schema)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return getattr(self.service, "get_tables", lambda d, s: {})(db, schema)

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return getattr(self.service, "get_alerts", lambda d, s: {})(db, schema)


# ---------------------------------------------------------------------------
# Custom Adapters (For services that have non-standard method names)
# ---------------------------------------------------------------------------
class BranchManagementAdapter(BaseDashboardAdapter):
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return BranchManagementService.get_dashboard_kpi(db, schema)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return BranchManagementService.get_branch_charts(db, schema)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return BranchManagementService.get_branch_tables(db, schema)

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return BranchManagementService.get_branch_alerts(db, schema)


class FinancialAdapter(BaseDashboardAdapter):
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return getattr(FinancialService, "get_kpis", lambda d, s: {})(db, schema)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return FinancialService.get_charts(db, schema)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return FinancialService.get_tables(db, schema)


class InventoryAdapter(BaseDashboardAdapter):
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return InventoryService.get_inventory_kpi(db, schema, allowed_modules)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return InventoryService.get_inventory_charts(db, schema, allowed_modules)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return InventoryService.get_inventory_tables(db, schema, allowed_modules)

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return InventoryService.get_inventory_alerts(db, schema, allowed_modules)


class VendorAdapter(BaseDashboardAdapter):
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return VendorService.get_vendor_kpi(db, schema)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return VendorService.get_vendor_charts(db, schema)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return VendorService.get_vendor_tables(db, schema)

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return VendorService.get_vendor_alerts(db, schema)

class LeadAdapter(BaseDashboardAdapter):
    @ttl_cache(ttl_seconds=60)
    def get_kpis(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return LeadService.get_kpi(db, schema, allowed_modules)
        
    @ttl_cache(ttl_seconds=60)
    def get_charts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return LeadService.get_charts(db, schema, allowed_modules)
        
    def get_tables(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return LeadService.get_tables(db, schema, allowed_modules)

    def get_alerts(self, db, schema: str, allowed_modules: list = None) -> Dict[str, Any]:
        return LeadService.get_alerts(db, schema, allowed_modules)


# ---------------------------------------------------------------------------
# Central Registry Map
# ---------------------------------------------------------------------------
REGISTRY: Dict[str, BaseDashboardAdapter] = {
    # Custom Named Services
    "branch_management": BranchManagementAdapter(),
    "financial": FinancialAdapter(),
    "inventory": InventoryAdapter(),
    "vendor_management": VendorAdapter(),
    "lead_followup": LeadAdapter(),

    # Standardized Methods
    "sales_order": StandardAdapter(SalesOrderService),
    "purchase": StandardAdapter(PurchaseService),
    "quotation": StandardAdapter(QuotationService),
    "customer_management": StandardAdapter(CustomerService),
    "contract_management": StandardAdapter(ContractService),
    "customer_support": StandardAdapter(SupportService),
    "gma": StandardAdapter(GMAService),
    "task_management": StandardAdapter(TaskService),
    "employee_management": StandardAdapter(EmployeeService),
    "hrm": StandardAdapter(HRMService),
    "petty_cash": StandardAdapter(PettyCashService),
}

def get_adapter(dashboard_name: str) -> BaseDashboardAdapter | None:
    """Safely retrieves the adapter instance mapping to isolated logic."""
    return REGISTRY.get(dashboard_name.lower())
