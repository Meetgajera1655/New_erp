from fastapi import Header, HTTPException

def get_current_tenant(authorization: str = Header(...)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing token")

    try:
        token = authorization.split(" ")[1]
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

    return token