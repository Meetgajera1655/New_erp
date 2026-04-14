from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.branch_management_service import BranchManagementService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def branch_management_tables(
    tenant: str = Depends(get_current_tenant),
    db: Session = Depends(get_db)
):
    return BranchManagementService.get_branch_tables(db, tenant)