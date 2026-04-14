from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.branch_management_service import BranchManagementService
from app.utils.auth import get_current_tenant

router = APIRouter()

# ✅ Single combined KPI API
@router.get("")
def module7_kpi(
    tenant: str = Depends(get_current_tenant),
    db: Session = Depends(get_db)
):
    return BranchManagementService.get_dashboard_kpi(db, tenant)