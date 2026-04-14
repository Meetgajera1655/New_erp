from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.gma_service import GMAService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def alerts(tenant: str = Depends(get_current_tenant), db: Session = Depends(get_db)):
    return GMAService.get_alerts(db, tenant)