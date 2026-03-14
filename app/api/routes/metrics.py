import io
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.models.schemas import MetricsCsvRequest
from app.services.metrics_service import MetricsService

router = APIRouter(prefix="/metrics", tags=["Metrics"])
metrics_service = MetricsService()


def safe_name(value: str) -> str:
    return (
        value.replace(":", "-")
        .replace(" ", "_")
        .replace("/", "-")
    )


@router.post("/csv")
def download_metrics_csv(payload: MetricsCsvRequest):
    if payload.source_flag.value == "sks":
        if payload.station_type == "preeol":
            phase = "G0"

            csv_bytes = metrics_service.build_sks_phase_csv(
                station_type=payload.station_type,
                start_time=payload.start_time,
                end_time=payload.end_time,
                source_flag=payload.source_flag.value,
                phase=phase,
                uuid_list=payload.uuid_list,
                single_uuid=payload.single_uuid,
                size=payload.size,
            )

            filename = (
                f"{payload.source_flag.value}_"
                f"{payload.station_type}_"
                f"{phase}_"
                f"{safe_name(payload.start_time)}_to_{safe_name(payload.end_time)}.csv"
            )

        elif payload.station_type == "eol":
            if not payload.preeol_phase:
                raise HTTPException(
                    status_code=400,
                    detail="phase is required for sks eol.",
                )

            phase = payload.preeol_phase.value

            csv_bytes = metrics_service.build_sks_phase_csv(
                station_type=payload.station_type,
                start_time=payload.start_time,
                end_time=payload.end_time,
                source_flag=payload.source_flag.value,
                phase=phase,
                uuid_list=payload.uuid_list,
                single_uuid=payload.single_uuid,
                size=payload.size,
            )

            filename = (
                f"{payload.source_flag.value}_"
                f"{payload.station_type}_"
                f"{phase}_"
                f"{safe_name(payload.start_time)}_to_{safe_name(payload.end_time)}.csv"
            )

        elif payload.station_type == "camera":
            if not payload.camera_ev:
                raise HTTPException(
                    status_code=400,
                    detail="camera_ev is required for sks camera CSV",
                )

            csv_bytes = metrics_service.build_camera_csv(
                station_type=payload.station_type,
                start_time=payload.start_time,
                end_time=payload.end_time,
                source_flag=payload.source_flag.value,
                camera_ev=payload.camera_ev.value,
                uuid_list=payload.uuid_list,
                single_uuid=payload.single_uuid,
                size=payload.size,
            )

            filename = (
                f"{payload.source_flag.value}_"
                f"{payload.station_type}_"
                f"{payload.camera_ev.value}_"
                f"{safe_name(payload.start_time)}_to_{safe_name(payload.end_time)}.csv"
            )

        else:
            raise HTTPException(
                status_code=400,
                detail="For sks, CSV is currently supported only for preeol, eol, and camera.",
            )

    elif payload.source_flag.value == "centum":
        if payload.station_type == "camera":
            if not payload.camera_ev:
                raise HTTPException(
                    status_code=400,
                    detail="camera_ev is required for camera CSV",
                )

            csv_bytes = metrics_service.build_camera_csv(
                station_type=payload.station_type,
                start_time=payload.start_time,
                end_time=payload.end_time,
                source_flag=payload.source_flag.value,
                camera_ev=payload.camera_ev.value,
                uuid_list=payload.uuid_list,
                single_uuid=payload.single_uuid,
                size=payload.size,
            )

            filename = (
                f"{payload.source_flag.value}_"
                f"{payload.station_type}_"
                f"{payload.camera_ev.value}_"
                f"{safe_name(payload.start_time)}_to_{safe_name(payload.end_time)}.csv"
            )

        elif payload.station_type in ["preeol", "eol"]:
            if not payload.preeol_phase:
                raise HTTPException(
                    status_code=400,
                    detail="preeol_phase is required",
                )

            csv_bytes = metrics_service.build_phase_csv(
                station_type=payload.station_type,
                start_time=payload.start_time,
                end_time=payload.end_time,
                source_flag=payload.source_flag.value,
                phase=payload.preeol_phase.value,
                uuid_list=payload.uuid_list,
                single_uuid=payload.single_uuid,
                size=payload.size,
            )

            filename = (
                f"{payload.source_flag.value}_"
                f"{payload.station_type}_"
                f"{payload.preeol_phase.value}_"
                f"{safe_name(payload.start_time)}_to_{safe_name(payload.end_time)}.csv"
            )

        else:
            raise HTTPException(
                status_code=400,
                detail="For centum, CSV flow is currently implemented only for preeol, eol, and camera.",
            )

    else:
        raise HTTPException(
            status_code=400,
            detail="Unsupported source_flag.",
        )

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )