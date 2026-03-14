from typing import Dict, List, Optional

from app.services.elastic_service import ElasticService
from app.services.gcs_service import GCSService


class ReportService:
    def __init__(self) -> None:
        self.elastic_service = ElasticService()
        self.gcs_service = GCSService()

    def get_uuids(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        size: int = 10000,
    ) -> List[Dict]:
        rows = self.elastic_service.fetch_uuids(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )

        for row in rows:
            row["img_av"] = self.gcs_service.has_image(row["uuid"], source_flag)

        return rows

    def resolve_uuids(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ) -> List[str]:
        if single_uuid:
            return [single_uuid]

        if uuid_list:
            return uuid_list

        rows = self.get_uuids(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )
        return [row["uuid"] for row in rows]

    def build_download_zip(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        source_flag: str,
        download_type: str,
        uuid_list: Optional[List[str]] = None,
        single_uuid: Optional[str] = None,
        size: int = 10000,
    ):
        uuids = self.resolve_uuids(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            uuid_list=uuid_list,
            single_uuid=single_uuid,
            size=size,
        )

        return self.gcs_service.build_zip_for_reports(
            uuids=uuids,
            source_flag=source_flag,
            download_type=download_type,
        )