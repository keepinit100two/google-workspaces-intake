import os
from fastapi import Header, HTTPException


def require_ops_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    expected = os.getenv("OPS_API_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="OPS_API_KEY not configured")
    if x_api_key != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
