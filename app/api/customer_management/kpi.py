from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.customer_service import CustomerService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def kpi(tenant: str = Depends(get_current_tenant), db: Session = Depends(get_db)):
    return CustomerService.get_kpi(db, tenant)