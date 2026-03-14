import io
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.models.schemas import MetricsCsvRequest
from app.services.metrics_service import MetricsService

router = APIRouter(prefix="/metrics", tags=["Metrics"])
metrics_service = MetricsService()


@router.post("/csv")
def download_metrics_csv(payload: MetricsCsvRequest):
    csv_bytes = metrics_service.build_preeol_csv(
        station_type=payload.station_type,
        start_time=payload.start_time,
        end_time=payload.end_time,
        source_flag=payload.source_flag.value,
        uuid_list=payload.uuid_list,
        single_uuid=payload.single_uuid,
        size=payload.size,
    )

    filename = f"{payload.source_flag.value}_{payload.station_type}_preeol_metrics.csv"

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )