from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.lead_service import LeadService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def tables(
    tenant: str = Depends(get_current_tenant),
    db: Session = Depends(get_db)
):
    return LeadService.get_tables(db, tenant)