import io
import pandas as pd
from typing import Any, Dict, List, Optional

from app.services.report_service import ReportService
from app.services.gcs_service import GCSService


class MetricsService:
    def __init__(self) -> None:
        self.report_service = ReportService()
        self.gcs_service = GCSService()

    def safe_get(self, data: Any, path: List[Any], default=None):
        cur = data
        for key in path:
            try:
                cur = cur[key]
            except (KeyError, IndexError, TypeError):
                return default
        return cur

    def extract_metric_range_by_name(
        self,
        data: Dict,
        test_idx: int = 10,
        start_idx: int = 70,
        end_idx: int = 106,
    ) -> Dict[str, Any]:
        extracted: Dict[str, Any] = {}

        metrics = self.safe_get(
            data,
            ["report", "tests", test_idx, "metadata", "metrics"],
            default=[]
        )

        if not isinstance(metrics, list):
            return extracted

        for i in range(start_idx, end_idx + 1):
            if i >= len(metrics):
                continue

            metric = metrics[i]
            if not isinstance(metric, dict):
                continue

            name = metric.get("name")
            measurement = metric.get("measurement")

            if name is not None:
                extracted[name] = measurement

        return extracted

    def build_preeol_rows_from_gcs(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        uuids = self.report_service.resolve_uuids(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        rows: List[Dict[str, Any]] = []

        for uuid in uuids:
            data = self.gcs_service.download_report_json(uuid, source_flag)
            if not data:
                continue

            test_10 = self.safe_get(data, ["report", "tests", 10], default={})
            nodeid = test_10.get("nodeid") if isinstance(test_10, dict) else None
            outcome = test_10.get("outcome") if isinstance(test_10, dict) else None

            row = {
                "uuid": self.safe_get(data, ["uuid"]),
                "component_id": self.safe_get(data, ["component_id"]),
                "detected_strip": self.safe_get(
                    data,
                    ["report", "tests", 10, "metadata", "images", 0, "result", "verdict", "model", "detected_strip"]
                )
            }

            if nodeid:
                row[nodeid] = outcome

            metric_map = self.extract_metric_range_by_name(
                data,
                test_idx=10,
                start_idx=70,
                end_idx=106,
            )
            row.update(metric_map)

            rows.append(row)

        return rows

    def build_preeol_csv(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> bytes:
        rows = self.build_preeol_rows_from_gcs(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        if not rows:
            df = pd.DataFrame(columns=[
                "uuid",
                "component_id",
                "detected_strip",
            ])
        else:
            base_columns = [
                "uuid",
                "component_id",
                "detected_strip",
            ]

            extra_columns = sorted(
                {
                    key
                    for row in rows
                    for key in row.keys()
                    if key not in base_columns
                }
            )

            ordered_columns = base_columns + extra_columns
            df = pd.DataFrame(rows)

            for col in ordered_columns:
                if col not in df.columns:
                    df[col] = None

            df = df[ordered_columns]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue().encode("utf-8")