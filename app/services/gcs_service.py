import io
import json
import zipfile
import logging
from typing import Dict, List, Optional, Tuple

from google.cloud import storage
from google.oauth2 import service_account

from app.config import settings
from app.constants import IMAGE_EXTENSIONS

logger = logging.getLogger(__name__)


class GCSService:
    def __init__(self) -> None:
        credentials = service_account.Credentials.from_service_account_file(
            settings.GCP_CREDENTIALS_JSON
        )
        self.client = storage.Client(
            project=settings.GCP_PROJECT,
            credentials=credentials,
        )

    def _get_bucket_name(self, source_flag: str) -> str:
        return settings.SOURCE_CONFIG[source_flag]["gcs_bucket"]

    def _bucket(self, source_flag: str):
        return self.client.bucket(self._get_bucket_name(source_flag))

    def download_report_json(self, uuid: str, source_flag: str) -> Optional[dict]:
        try:
            blob = self._bucket(source_flag).blob(f"{uuid}/report")
            raw = blob.download_as_bytes()
            return json.loads(raw)
        except Exception as e:
            logger.exception("Failed to download report JSON | uuid=%s error=%s", uuid, e)
            return None

    def list_image_blobs(self, uuid: str, source_flag: str) -> List[str]:
        bucket_name = self._get_bucket_name(source_flag)
        prefix = f"{uuid}/"
        image_paths: List[str] = []

        for blob in self.client.list_blobs(bucket_name, prefix=prefix):
            if blob.name == f"{uuid}/report":
                continue
            if blob.name.lower().endswith(IMAGE_EXTENSIONS):
                image_paths.append(blob.name)

        return image_paths

    def download_blob_bytes(self, blob_name: str, source_flag: str) -> bytes:
        blob = self._bucket(source_flag).blob(blob_name)
        return blob.download_as_bytes()

    def has_image(self, uuid: str, source_flag: str) -> bool:
        return len(self.list_image_blobs(uuid, source_flag)) > 0

    def select_image_blobs(
        self,
        image_blobs: List[str],
        station_type: str,
        image_variant: Optional[str],
    ) -> List[str]:
        if not image_blobs:
            return []

        station_type = station_type.lower()

        # camera -> always all images
        if station_type == "camera":
            return image_blobs

        # preeol/eol -> allow G0/G4/G6/all
        if station_type in ("preeol", "eol"):
            if not image_variant or image_variant.lower() == "all":
                return image_blobs

            token = image_variant.upper()

            for blob_name in image_blobs:
                file_name = blob_name.split("/")[-1].upper()
                if token in file_name:
                    return [blob_name]

            return []

        return image_blobs

    def build_zip_for_reports(
    self,
    uuids: List[str],
    station_type: str,
    source_flag: str,
    download_type: str,
    image_variant: Optional[str] = None,
    root_folder: Optional[str] = None,
    ) -> Tuple[bytes, List[Dict]]:
        logger.info(
            "Starting ZIP build | uuid_count=%s station_type=%s source=%s download_type=%s image_variant=%s root_folder=%s",
            len(uuids),
            station_type,
            source_flag,
            download_type,
            image_variant,
            root_folder,
        )

        zip_buffer = io.BytesIO()
        summary: List[Dict] = []

        root_prefix = f"{root_folder}/" if root_folder else ""

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for uuid in uuids:
                item_summary = {
                    "uuid": uuid,
                    "report_added": False,
                    "images_added": 0,
                }

                logger.info("Processing UUID | uuid=%s", uuid)

                if download_type in ("report", "both"):
                    report_json = self.download_report_json(uuid, source_flag)
                    if report_json is not None:
                        zf.writestr(
                            f"{root_prefix}{uuid}/report.json",
                            json.dumps(report_json, indent=2)
                        )
                        item_summary["report_added"] = True
                        logger.info("Report added | uuid=%s", uuid)

                if download_type in ("image", "both"):
                    image_blobs = self.list_image_blobs(uuid, source_flag)
                    selected_blobs = self.select_image_blobs(
                        image_blobs=image_blobs,
                        station_type=station_type,
                        image_variant=image_variant,
                    )

                    logger.info(
                        "Images selected | uuid=%s total_found=%s selected=%s station=%s variant=%s",
                        uuid,
                        len(image_blobs),
                        len(selected_blobs),
                        station_type,
                        image_variant,
                    )

                    for blob_name in selected_blobs:
                        file_bytes = self.download_blob_bytes(blob_name, source_flag)
                        relative_name = blob_name.replace(f"{uuid}/", "")
                        zf.writestr(
                            f"{root_prefix}{uuid}/{relative_name}",
                            file_bytes,
                        )
                        item_summary["images_added"] += 1

                summary.append(item_summary)

        zip_buffer.seek(0)
        return zip_buffer.getvalue(), summary