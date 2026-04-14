from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.inventory_service import InventoryService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def inventory_tables(
    tenant: str = Depends(get_current_tenant),
    db: Session = Depends(get_db)
):
    return InventoryService.get_inventory_tables(db, tenant)