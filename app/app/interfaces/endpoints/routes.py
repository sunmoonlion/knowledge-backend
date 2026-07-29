from fastapi import APIRouter, Depends

from app.interfaces.endpoints.auth_routes import router as auth_router
from app.interfaces.endpoints.knowledge_routes import (
    internal_router as knowledge_internal_router,
    router as knowledge_router,
)
from app.interfaces.endpoints.tasks_routes import router as tasks_router
from app.interfaces.middleware.auth import require_knowledge_admin

router = APIRouter()

router.include_router(auth_router)
router.include_router(
    tasks_router, dependencies=[Depends(require_knowledge_admin)]
)
router.include_router(
    knowledge_router, dependencies=[Depends(require_knowledge_admin)]
)
router.include_router(knowledge_internal_router)

# 在此注册其他业务模块路由
