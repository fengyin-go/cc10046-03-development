"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource
from app.routers import resume
from app.routers import chains

# 续采计划含固定路径 /api/resume/...，必须排在通用资源路由 /api/{module} 之前。
ROUTERS = [resume, resource, chains]
