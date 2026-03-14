from fastapi import FastAPI

from app.api.routes.reports import router as reports_router
from app.api.routes.metrics import router as metrics_router
from app.config import settings

app = FastAPI(title=settings.APP_NAME)

app.include_router(reports_router)
app.include_router(metrics_router)


@app.get("/health")
def health():
    return {"status": "ok", "app": settings.APP_NAME}