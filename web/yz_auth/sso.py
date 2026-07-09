"""yz_auth SSO — 跳转、回调、ticket 验证"""
import logging

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse

from config.settings import get_settings
from web.yz_auth.session import (
    clear_session_cookie,
    create_session,
    get_session,
    set_session_cookie,
)

logger = logging.getLogger(__name__)

router = APIRouter()

YZ_LOGIN_URL = "http://192.168.0.19:5555"
TAF_CALLBACK_URL = "http://192.168.0.19:8081/api/yz/callback"


def _get_urls():
    """从配置获取 yz-login 地址"""
    settings = get_settings()
    yz_url = getattr(settings, "YZ_LOGIN_URL", None) or YZ_LOGIN_URL
    callback = getattr(settings, "TAF_CALLBACK_URL", None) or TAF_CALLBACK_URL
    return yz_url, callback


async def sso_middleware(request: Request, call_next):
    """SSO 认证中间件"""
    path = request.url.path

    # 不需要认证的路径
    public_paths = {
        "/", "/health", "/favicon.ico",
        "/api/yz/callback", "/api/yz/logout", "/api/yz/user",
        "/api/user/login",
    }
    if path in public_paths:
        return await call_next(request)

    # 静态资源不需要认证
    if path.startswith("/static/"):
        return await call_next(request)

    # 检查会话
    session = get_session(request)
    if session:
        # 只允许管理员访问
        if session.get("is_admin") != 1:
            return JSONResponse(status_code=403, content={"detail": "需要管理员权限"})
        request.state.user = session
        return await call_next(request)

    # API 请求返回 401
    if path.startswith("/api/"):
        return JSONResponse(status_code=401, content={"detail": "未登录"})

    # 页面请求跳转 SSO
    yz_url, callback = _get_urls()
    login_url = f"{yz_url}/login?from={callback}"
    return RedirectResponse(url=login_url, status_code=302)


@router.get("/api/yz/callback")
async def yz_callback(request: Request):
    """yz-login 回调"""
    ticket = request.query_params.get("ticket")
    if not ticket:
        return RedirectResponse(url="/", status_code=302)

    yz_url, _ = _get_urls()
    verify_url = f"{yz_url}/api/ticket/verify"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(verify_url, params={"ticket": ticket})
            if resp.status_code != 200:
                logger.warning(f"ticket 验证失败: HTTP {resp.status_code}")
                return RedirectResponse(url="/", status_code=302)

            data = resp.json()
            if not data.get("ok"):
                logger.warning(f"ticket 验证失败: {data.get('msg', 'unknown')}")
                return RedirectResponse(url="/", status_code=302)

        token = create_session(
            user_id=data["id"],
            username=data["username"],
            display_name=data["display_name"],
            is_admin=data.get("is_admin", 0),
        )
        logger.info(f"SSO 登录成功: {data['display_name']} (admin={data.get('is_admin', 0)})")

        response = RedirectResponse(url="/?sso=1", status_code=302)
        return set_session_cookie(response, token)

    except Exception as e:
        logger.error(f"SSO 回调异常: {e}")
        return RedirectResponse(url="/", status_code=302)


@router.get("/api/yz/logout")
async def yz_logout(request: Request):
    """退出登录：清 TAF Cookie，跳转 yz-login 登出清除 SSO 会话"""
    yz_url, _ = _get_urls()
    response = RedirectResponse(url=f"{yz_url}/logout", status_code=302)
    return clear_session_cookie(response)


@router.get("/api/yz/user")
async def yz_user_info(request: Request):
    session = get_session(request)
    if not session:
        return {"ok": False, "msg": "未登录"}
    return {
        "ok": True,
        "user_id": session["user_id"],
        "username": session["username"],
        "display_name": session["display_name"],
        "is_admin": session["is_admin"],
    }
