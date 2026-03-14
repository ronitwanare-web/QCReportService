import io
from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse

from app.models.schemas import DownloadReportsRequest, UUIDFetchRequest
from app.services.report_service import ReportService

router = APIRouter(prefix="/reports", tags=["Reports"])
report_service = ReportService()


@router.post("/uuids")
def get_uuids(payload: UUIDFetchRequest):
    rows = report_service.get_uuids(
        station_type=payload.station_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        source_flag=payload.source_flag.value,
        size=payload.size,
    )
    return JSONResponse(content={"count": len(rows), "data": rows})


@router.post("/download")
def download_reports(payload: DownloadReportsRequest):
    zip_bytes, _summary = report_service.build_download_zip(
        station_type=payload.station_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        source_flag=payload.source_flag.value,
        download_type=payload.download_type.value,
        uuid_list=payload.uuid_list,
        single_uuid=payload.single_uuid,
    )

    filename = f"{payload.source_flag.value}_{payload.download_type.value}_reports.zip"

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )