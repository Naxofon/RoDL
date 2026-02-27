from typing import Optional


def mask_token(token: Optional[str], *, head: int = 4, tail: int = 4) -> str:
    """Return a safe token fingerprint for logs (prefix/suffix only)."""
    if not token:
        return "<empty>"

    token = str(token).strip()
    if not token:
        return "<empty>"

    if len(token) <= head + tail:
        return f"{token[:1]}***{token[-1:]}" if len(token) > 2 else "***"

    return f"{token[:head]}...{token[-tail:]}"


def format_auth_fingerprint(login: Optional[str], token: Optional[str]) -> str:
    """Format login + masked token for safe logging."""
    login_str = str(login).strip() if login is not None else ""
    if not login_str:
        login_str = "unknown"
    return f"login={login_str}, token={mask_token(token)}"


__all__ = ["mask_token", "format_auth_fingerprint"]
