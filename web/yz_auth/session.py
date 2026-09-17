"""yz_auth 会话管理 — cookie-based token（文件持久化，重启不丢会话）"""
import json
import logging
import secrets
import time
from pathlib import Path
from typing import Optional

from fastapi import Request, Response

logger = logging.getLogger(__name__)

_sessions: dict[str, dict] = {}
_SESSION_TTL = 86400  # 24 小时
_COOKIE_NAME = "taf_sso_token"

# 会话持久化文件（temp/ 已在 .gitignore 中）
_SESSIONS_FILE = Path("temp/yz_sessions.json")


def _load_sessions() -> None:
    """启动时从文件加载会话（过滤已过期的）"""
    try:
        if not _SESSIONS_FILE.exists():
            return
        data = json.loads(_SESSIONS_FILE.read_text(encoding="utf-8"))
        now_ts = time.time()
        for token, sess in data.items():
            if now_ts - sess.get("created", 0) <= _SESSION_TTL:
                _sessions[token] = sess
        if _sessions:
            logger.info(f"从文件恢复 {len(_sessions)} 个 SSO 会话")
    except Exception as e:
        logger.warning(f"加载 SSO 会话文件失败（忽略，相当于全部重新登录）: {e}")


def _save_sessions() -> None:
    """保存会话到文件（原子替换，避免写一半损坏）"""
    try:
        _SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = _SESSIONS_FILE.with_suffix(".tmp")
        tmp_file.write_text(json.dumps(_sessions), encoding="utf-8")
        tmp_file.replace(_SESSIONS_FILE)
    except Exception as e:
        logger.warning(f"保存 SSO 会话文件失败: {e}")


def create_session(user_id: int, username: str, display_name: str, is_admin: int) -> str:
    token = secrets.token_hex(32)
    _sessions[token] = {
        "user_id": user_id,
        "username": username,
        "display_name": display_name,
        "is_admin": is_admin,
        "created": time.time(),
    }
    _save_sessions()
    return token


def get_session(request: Request) -> Optional[dict]:
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        return None
    session = _sessions.get(token)
    if not session:
        return None
    if time.time() - session["created"] > _SESSION_TTL:
        del _sessions[token]
        _save_sessions()
        return None
    return session


def is_admin(request: Request) -> bool:
    session = get_session(request)
    return session is not None and session.get("is_admin") == 1


def set_session_cookie(response: Response, token: str) -> Response:
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        max_age=_SESSION_TTL,
        httponly=True,
        samesite="lax",
    )
    return response


def clear_session_cookie(response: Response) -> Response:
    response.delete_cookie(key=_COOKIE_NAME)
    return response


# 模块加载时恢复会话
_load_sessions()
