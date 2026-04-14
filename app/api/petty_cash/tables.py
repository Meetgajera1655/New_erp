from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.petty_cash_service import PettyCashService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("/")
def tables(db: Session = Depends(get_db), tenant: str = Depends(get_current_tenant)):
    return PettyCashService.get_tables(db, tenant)