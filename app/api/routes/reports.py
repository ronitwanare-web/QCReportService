import io
import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse

from app.models.schemas import DownloadReportsRequest, UUIDFetchRequest
from app.services.report_service import ReportService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/reports", tags=["Reports"])
report_service = ReportService()


@router.post("/uuids")
def get_uuids(payload: UUIDFetchRequest):
    logger.info(
        "POST /reports/uuids | station_type=%s source_flag=%s start=%s end=%s size=%s",
        payload.station_type,
        payload.source_flag.value,
        payload.start_time,
        payload.end_time,
        payload.size,
    )

    rows = report_service.get_uuids(
        station_type=payload.station_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        source_flag=payload.source_flag.value,
        size=payload.size,
    )

    logger.info("UUID fetch complete | count=%s", len(rows))
    return JSONResponse(content={"count": len(rows), "data": rows})


@router.post("/download")
def download_reports(payload: DownloadReportsRequest):
    zip_bytes, _summary = report_service.build_download_zip(
        station_type=payload.station_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        source_flag=payload.source_flag.value,
        download_type=payload.download_type.value,
        image_variant=payload.image_variant.value if payload.image_variant else None,
        uuid_list=payload.uuid_list,
        single_uuid=payload.single_uuid,
    )

    filename = f"{payload.source_flag.value}_{payload.download_type.value}_reports.zip"

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )