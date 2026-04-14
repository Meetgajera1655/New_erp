from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.financial_service import FinancialService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("/")
def charts(db: Session = Depends(get_db), tenant=Depends(get_current_tenant)):
    return FinancialService.get_charts(db, tenant)