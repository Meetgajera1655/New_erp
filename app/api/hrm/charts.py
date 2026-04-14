from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.hrm_service import HRMService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("/")
def charts(db: Session = Depends(get_db), tenant: str = Depends(get_current_tenant)):
    return HRMService.get_charts(db, tenant)