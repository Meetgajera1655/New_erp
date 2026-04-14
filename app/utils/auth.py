from fastapi import Header, HTTPException
from jose import jwt

# If you don’t have SECRET_KEY → use unverified for now
# Later replace with jwt.decode()

def get_current_tenant(
    authorization: str = Header(None, alias="Authorization")
):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        # Extract token
        token = authorization.split(" ")[1]

        # Decode WITHOUT secret (temporary)
        payload = jwt.get_unverified_claims(token)

        # 🔥 YOUR FIELD FROM TOKEN
        tenant = payload.get("tenantSchema")

        if not tenant:
            raise HTTPException(status_code=401, detail="Tenant not found")

        return tenant.lower()

    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")