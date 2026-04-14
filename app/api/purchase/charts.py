from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.purchase_service import PurchaseService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def purchase_charts(tenant: str = Depends(get_current_tenant), db: Session = Depends(get_db)):
    return PurchaseService.get_charts(db, tenant)