from fastapi import Header, HTTPException, Depends, Request
from sqlalchemy.orm import Session
from sqlalchemy import text
from jose import jwt

from app.database import get_db

def decode_token(token: str) -> dict:
    """Reusable function to safely decode payload constraints."""
    try:
        # Decode WITHOUT secret (temporary per architecture constraints)
        return jwt.get_unverified_claims(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_current_tenant(
    authorization: str = Header(None, alias="Authorization")
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        token = authorization.split(" ")[1]
        payload = decode_token(token)
        tenant = payload.get("tenantSchema")
        
        if not tenant:
            raise HTTPException(status_code=401, detail="Tenant not found")
            
        return tenant.lower()
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


DASHBOARD_MODULE_MAP = {
    "lead_followup": ["LEADS_MANAGEMENT", "FOLLOW_UP_MANAGEMENT"],
    "vendor_management": ["VENDOR_MANAGEMENT"],
    "purchase": ["PURCHASE_ORDER_MANAGEMENT"],
    "inventory": ["PRODUCT_MANAGEMENT", "STOCK_MANAGEMENT"],
    "quotation": ["QUOTATION_MANAGEMENT"],
    "gma": ["GMA_SHEET_MANAGEMENT"],
    "customer_management": ["CUSTOMER_CONTRACT_MANAGEMENT"],
    "sales_order": ["SALES_ORDER_MANAGEMENT"],
    "task_management": ["TASK_MANAGEMENT"],
    "customer_support": ["CUSTOMER_SUPPORT_MANAGEMENT"],
    "petty_cash": ["PETTY_CASH_MANAGEMENT"],
    "branch_management": ["BRANCH_MANAGEMENT"]
}

async def get_user_allowed_modules(db: Session, schema: str, user_id: int) -> list:
    """
    Safely retrieves all explicitly allowed READ modules for a given user 
    against the user_permissions database map.
    """
    query = text(f"""
        SELECT m.name AS module_name
        FROM {schema}.user_permissions up
        JOIN public.modules m ON up.module_id = m.id
        JOIN public.actions a ON up.action_id = a.id
        WHERE up.user_id = :user_id
          AND up.allowed = true
          AND a.name = 'READ'
    """)
    
    # Ideally use run_in_threadpool or AsyncSession if doing explicit non-blocking DB calls.
    # We maintain db.execute compatibility inside an async def to satisfy asynchronous mapping parameters.
    results = db.execute(query, {"user_id": user_id}).fetchall()
    
    # DEBUG Requirements
    print("tenantSchema:", schema)
    print("DB result:", results)
    
    return [r[0] for r in results]

async def verify_dashboard_access(
    request: Request,
    authorization: str = Header(None, alias="Authorization"),
    db: Session = Depends(get_db)
) -> dict:
    """Strictly maps a user's role array onto the exact URI module being invoked."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        token = authorization.split(" ")[1]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token format")
        
    payload = decode_token(token)
    
    # DEBUG CODE: Print exactly what we decoded from the JWT payload
    print("======== JWT PAYLOAD DEBUG ========")
    print(payload)
    print("===================================")
    
    tenant = payload.get("tenantSchema")
    user_id = payload.get("user_id", payload.get("userId"))
    role_id = payload.get("role_id", payload.get("roleId", payload.get("role")))

    if not tenant:
        raise HTTPException(status_code=401, detail="Tenant schema not found")

    if not user_id:
        raise HTTPException(status_code=401, detail="User ID resolution failed in token.")
        
    schema = tenant.lower()
    
    # Step A: Parse Role name with robust fallback
    role_token_name = payload.get("role")
    role_token_id = payload.get("role_id", payload.get("roleId"))
    
    role_name = None
    if role_token_id and (str(role_token_id).isdigit() or isinstance(role_token_id, int)):
        q = text(f"SELECT name FROM public.roles WHERE id = :role_id")
        rec = db.execute(q, {"role_id": role_token_id}).fetchone()
        if rec:
            role_name = rec[0]
            
    # Fallback to the string 'role' from token if DB lookup failed or was skipped
    if not role_name:
        role_name = role_token_name

    # Enforce safe minimum existence check
    if not role_name:
        raise HTTPException(status_code=403, detail="Role resolution failed: roleId not in DB and role name missing in token.")

    # Convert request path parameter safely scaling through explicit maps 
    dashboard_name = request.path_params.get("dashboard_name", "")
    target_modules = DASHBOARD_MODULE_MAP.get(dashboard_name, [dashboard_name.upper()])

    allowed_modules = []

    # Step B: CEO bypass OR granular evaluation logic
    if role_name and str(role_name).upper() == "CEO":
        allowed_modules = target_modules
    else:
        # 1. First comprehensively pull all of the user's enabled arrays universally
        user_all_modules = await get_user_allowed_modules(db, schema, user_id)
        
        # Ensure absolutely bulletproof Type + Strip + Uppercase coercion 
        # (Resolves hidden whitespace padding if PostgreSQL uses CHAR instead of VARCHAR)
        safe_user_modules = [str(m).strip().upper() for m in user_all_modules if m]
        safe_target_modules = [str(m).strip().upper() for m in target_modules if m]
        
        # 2. Extract intersection dynamically based simply on the active dashboard constraints
        intersection = list(set(safe_user_modules) & set(safe_target_modules))
        allowed_modules = intersection

        # DEBUG Requirements
        print("tenantSchema:", schema)
        print("user_id:", user_id)
        print("DB result:", user_all_modules)
        print("intersection:", intersection)

        # DEBUG CODE specifically checking silent equality errors
        print("======== AUTH ARRAY DEBUG ========")
        print(f"type(user_all_modules): {type(user_all_modules)}")
        print(f"repr(user_all_modules): {repr(user_all_modules)}")
        print(f"repr(target_modules): {repr(target_modules)}")
        print(f"repr(allowed_modules): {repr(allowed_modules)}")
        print("==================================")
        
        # 3. Block cleanly natively immediately if the list resolves physically empty
        if not allowed_modules:
            raise HTTPException(status_code=403, detail=f"Access denied for {dashboard_name}")

    return {
        "tenant": schema,
        "user_id": user_id,
        "role": role_name,
        "allowed_modules": allowed_modules
    }