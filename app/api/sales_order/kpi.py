from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.sales_order_service import SalesOrderService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def kpi(tenant: str = Depends(get_current_tenant), db: Session = Depends(get_db)):
    return SalesOrderService.get_kpi(db, tenant)