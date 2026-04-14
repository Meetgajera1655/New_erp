from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.task_service import TaskService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("/")
def kpi(db: Session = Depends(get_db), tenant: str = Depends(get_current_tenant)):
    return TaskService.get_kpi(db, tenant)