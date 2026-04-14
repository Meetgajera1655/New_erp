from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.employee_service import EmployeeService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def alerts(tenant: str = Depends(get_current_tenant), db: Session = Depends(get_db)):
    return EmployeeService.get_alerts(db, tenant)