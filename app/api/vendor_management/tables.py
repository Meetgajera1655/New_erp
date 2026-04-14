from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.vendor_service import VendorService
from app.utils.auth import get_current_tenant

router = APIRouter()

@router.get("")
def vendor_tables(
    tenant: str = Depends(get_current_tenant),
    db: Session = Depends(get_db)
):
    return VendorService.get_vendor_tables(db, tenant)