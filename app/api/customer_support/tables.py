from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.support_service import SupportService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("/")
def tables(db: Session = Depends(get_db), tenant: str = Depends(get_current_tenant)):
    return SupportService.get_tables(db, tenant)