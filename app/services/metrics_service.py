import io
import re
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple

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

    def get_uuid_records(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        return self.report_service.get_uuids(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )

    def resolve_uuid_list(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> Tuple[List[str], Dict[str, Optional[str]]]:
        uuid_records = self.get_uuid_records(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )

        uuid_timestamp_map = {
            item["uuid"]: item.get("timestamp")
            for item in uuid_records
            if item.get("uuid")
        }

        if single_uuid:
            uuids = [single_uuid]
        elif uuid_list:
            uuids = uuid_list
        else:
            uuids = [item["uuid"] for item in uuid_records if item.get("uuid")]

        return uuids, uuid_timestamp_map

    def extract_metric_range_by_name(
        self,
        data: Dict,
        test_idx: int,
        start_idx: int,
        end_idx: int,
    ) -> Dict[str, Any]:
        extracted: Dict[str, Any] = {}

        metrics = self.safe_get(
            data,
            ["report", "tests", test_idx, "metadata", "metrics"],
            default=[],
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

    def extract_metrics_failed(self, data: Dict, test_idx: int) -> Optional[str]:
        message = self.safe_get(
            data,
            ["report", "tests", test_idx, "call", "crash", "message"],
            default=None,
        )

        if not message or not isinstance(message, str):
            return None

        match = re.search(r"Metrics failed:\s*\[(.*?)\]", message)
        if not match:
            return None

        raw_items = match.group(1).strip()
        if not raw_items:
            return None

        metrics = [
            item.strip().strip("'").strip('"')
            for item in raw_items.split(",")
            if item.strip()
        ]

        return ", ".join(metrics) if metrics else None

    def get_centum_phase_config(self, station_type: str, phase: str) -> Tuple[int, int, int]:
        station_type = station_type.lower()
        phase = phase.upper()

        mapping = {
            "preeol": {
                "G0": (10, 70, 106),
                "G4": (12, 122, 241),
                "G6": (14, 122, 241),
            },
            "eol": {
                "G0": (11, 70, 106),
                "G4": (13, 122, 241),
                "G6": (15, 122, 241),
            },
        }

        return mapping[station_type][phase]

    def get_sks_phase_config(self, station_type: str, phase: str) -> Tuple[int, int, int]:
        station_type = station_type.lower()
        phase = phase.upper()

        mapping = {
            "preeol": {
                "G0": (2, 4, 7),
            },
            "eol": {
                "G0": (12, 4, 7),
                "G4": (13, 110, 219),
                "G6": (14, 110, 219),
            },
        }

        return mapping[station_type][phase]

    def build_centum_phase_rows_from_gcs(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        phase: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        uuids, uuid_timestamp_map = self.resolve_uuid_list(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        test_idx, start_idx, end_idx = self.get_centum_phase_config(station_type, phase)
        rows: List[Dict[str, Any]] = []

        for uuid in uuids:
            data = self.gcs_service.download_report_json(uuid, source_flag)
            if not data:
                continue

            selected_test = self.safe_get(data, ["report", "tests", test_idx], default={})
            nodeid = selected_test.get("nodeid") if isinstance(selected_test, dict) else None
            outcome = selected_test.get("outcome") if isinstance(selected_test, dict) else None

            row = {
                "uuid": self.safe_get(data, ["uuid"]) or uuid,
                "timestamp": uuid_timestamp_map.get(uuid),
                "component_id": self.safe_get(data, ["component_id"]),
                "station_type": station_type,
                "phase": phase,
                "detected_strip": self.safe_get(
                    data,
                    ["report", "tests", test_idx, "metadata", "images", 0, "result", "verdict", "model", "detected_strip"],
                ),
                "metrics_failed": self.extract_metrics_failed(data, test_idx),
            }

            if nodeid:
                row[nodeid] = outcome

            row.update(
                self.extract_metric_range_by_name(
                    data,
                    test_idx=test_idx,
                    start_idx=start_idx,
                    end_idx=end_idx,
                )
            )
            rows.append(row)

        return rows

    def build_sks_phase_rows_from_gcs(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        phase: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        uuids, uuid_timestamp_map = self.resolve_uuid_list(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        test_idx, start_idx, end_idx = self.get_sks_phase_config(station_type, phase)
        rows: List[Dict[str, Any]] = []

        for uuid in uuids:
            data = self.gcs_service.download_report_json(uuid, source_flag)
            if not data:
                continue

            selected_test = self.safe_get(data, ["report", "tests", test_idx], default={})
            nodeid = selected_test.get("nodeid") if isinstance(selected_test, dict) else None
            outcome = selected_test.get("outcome") if isinstance(selected_test, dict) else None

            row = {
                "uuid": self.safe_get(data, ["uuid"]) or uuid,
                "timestamp": uuid_timestamp_map.get(uuid),
                "component_id": self.safe_get(data, ["component_id"]),
                "base_cover": self.safe_get(data, ["depends", "base_cover"]),
                "station_type": station_type,
                "phase": phase,
                "metrics_failed": self.extract_metrics_failed(data, test_idx),
            }

            if nodeid:
                row[nodeid] = outcome

            row.update(
                self.extract_metric_range_by_name(
                    data,
                    test_idx=test_idx,
                    start_idx=start_idx,
                    end_idx=end_idx,
                )
            )
            rows.append(row)

        return rows

    def camera_ev_to_test_index(self, camera_ev: str) -> int:
        mapping = {
            "-1EV": 0,
            "0EV": 1,
            "+1EV": 2,
        }
        return mapping[camera_ev]

    def build_centum_camera_rows_from_gcs(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        camera_ev: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        uuids, uuid_timestamp_map = self.resolve_uuid_list(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        test_idx = self.camera_ev_to_test_index(camera_ev)
        rows: List[Dict[str, Any]] = []

        for uuid in uuids:
            data = self.gcs_service.download_report_json(uuid, source_flag)
            if not data:
                continue

            measurements = self.safe_get(
                data,
                ["report", "tests", test_idx, "metadata", "images", 0, "result", "verdict", "metrics", "measurements"],
                default={},
            )

            if not isinstance(measurements, dict):
                measurements = {}

            row = {
                "uuid": self.safe_get(data, ["uuid"]) or uuid,
                "timestamp": uuid_timestamp_map.get(uuid),
                "camera_id": self.safe_get(data, ["component_id"]),
                "failed": self.safe_get(
                    data,
                    ["report", "tests", test_idx, "metadata", "images", 0, "result", "verdict", "verdict", "failed"],
                ),
                "ev": camera_ev,
            }

            row.update(measurements)
            rows.append(row)

        return rows

    def build_sks_camera_rows_from_gcs(
    self,
    station_type: str,
    start_time: str,
    end_time: str,
    source_flag: str,
    camera_ev: str,
    uuid_list: Optional[List[str]] = None,
    single_uuid: Optional[str] = None,
    size: int = 10000,
    ) -> List[Dict[str, Any]]:
        uuids, uuid_timestamp_map = self.resolve_uuid_list(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        test_idx = self.camera_ev_to_test_index(camera_ev)
        rows: List[Dict[str, Any]] = []

        for uuid in uuids:
            data = self.gcs_service.download_report_json(uuid, source_flag)
            if not data:
                continue

            measurements = self.safe_get(
                data,
                [
                    "report", "tests", test_idx, "metadata", "metrics", 0,
                    "image", "compute_server_report", "verdict", "metrics", "measurements"
                ],
                default={},
            )

            if not isinstance(measurements, dict):
                measurements = {}

            row = {
                "uuid": self.safe_get(data, ["uuid"]) or uuid,
                "timestamp": uuid_timestamp_map.get(uuid),
                "camera_id": self.safe_get(data, ["component_id"]),
                "failed": self.safe_get(
                    data,
                    [
                        "report", "tests", test_idx, "metadata", "metrics", 0,
                        "image", "compute_server_report", "verdict", "verdict", "failed"
                    ],
                ),
                "verdict": self.safe_get(
                    data,
                    [
                        "report", "tests", test_idx, "metadata", "metrics", 0,
                        "image", "compute_server_report", "verdict", "verdict", "value"
                    ],
                ),
                "ev": camera_ev,
            }

            row.update(measurements)
            rows.append(row)

        return rows

    def build_camera_csv(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        camera_ev: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> bytes:
        if source_flag == "sks":
            rows = self.build_sks_camera_rows_from_gcs(
                station_type=station_type,
                start_time=start_time,
                end_time=end_time,
                source_flag=source_flag,
                camera_ev=camera_ev,
                uuid_list=uuid_list,
                single_uuid=single_uuid,
                size=size,
            )
        else:
            rows = self.build_centum_camera_rows_from_gcs(
                station_type=station_type,
                start_time=start_time,
                end_time=end_time,
                source_flag=source_flag,
                camera_ev=camera_ev,
                uuid_list=uuid_list,
                single_uuid=single_uuid,
                size=size,
            )

        if not rows:
            df = pd.DataFrame(columns=[
                "uuid",
                "timestamp",
                "camera_id",
                "failed",
                "ev",
            ])
        else:
            base_columns = [
                "uuid",
                "timestamp",
                "camera_id",
                "failed",
                "ev",
            ]
            extra_columns = sorted(
                {k for row in rows for k in row.keys() if k not in base_columns}
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

    def build_phase_csv(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        phase: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> bytes:
        rows = self.build_centum_phase_rows_from_gcs(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            phase=phase,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        if not rows:
            df = pd.DataFrame(columns=[
                "uuid",
                "timestamp",
                "component_id",
                "station_type",
                "phase",
                "detected_strip",
                "metrics_failed",
            ])
        else:
            base_columns = [
                "uuid",
                "timestamp",
                "component_id",
                "station_type",
                "phase",
                "detected_strip",
                "metrics_failed",
            ]
            extra_columns = sorted(
                {k for row in rows for k in row.keys() if k not in base_columns}
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

    def build_sks_phase_csv(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        phase: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> bytes:
        rows = self.build_sks_phase_rows_from_gcs(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            phase=phase,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        if not rows:
            df = pd.DataFrame(columns=[
                "uuid",
                "timestamp",
                "component_id",
                "base_cover",
                "station_type",
                "phase",
                "metrics_failed",
            ])
        else:
            base_columns = [
                "uuid",
                "timestamp",
                "component_id",
                "base_cover",
                "station_type",
                "phase",
                "metrics_failed",
            ]
            extra_columns = sorted(
                {k for row in rows for k in row.keys() if k not in base_columns}
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