from fastapi import APIRouter, Depends

from app.interfaces.endpoints.knowledge_routes import (
    internal_router as knowledge_internal_router,
)
from app.interfaces.endpoints.knowledge_routes import (
    router as knowledge_admin_router,
)
from app.interfaces.http.admin.auth import router as admin_auth_router
from app.interfaces.http.admin.diagnostics import router as admin_diagnostics_router
from app.interfaces.http.middleware.auth import require_knowledge_admin
from app.interfaces.http.web.auth import router as web_auth_router
from app.interfaces.http.web.interactions import router as web_interactions_router

router = APIRouter()
router.include_router(admin_auth_router)
router.include_router(web_auth_router)
router.include_router(admin_diagnostics_router)
router.include_router(web_interactions_router)
router.include_router(
    knowledge_admin_router,
    dependencies=[Depends(require_knowledge_admin)],
)
router.include_router(knowledge_internal_router)
