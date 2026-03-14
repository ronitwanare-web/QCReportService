import io
import json
import zipfile
from typing import Dict, List, Optional, Tuple

from google.cloud import storage
from google.oauth2 import service_account

from app.config import settings
from app.constants import IMAGE_EXTENSIONS


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
        except Exception:
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

    def build_zip_for_reports(
        self,
        uuids: List[str],
        source_flag: str,
        download_type: str,
    ) -> Tuple[bytes, List[Dict]]:
        zip_buffer = io.BytesIO()
        summary: List[Dict] = []

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for uuid in uuids:
                item_summary = {
                    "uuid": uuid,
                    "report_added": False,
                    "images_added": 0,
                }

                if download_type in ("report", "both"):
                    report_json = self.download_report_json(uuid, source_flag)
                    if report_json is not None:
                        zf.writestr(f"{uuid}/report.json", json.dumps(report_json, indent=2))
                        item_summary["report_added"] = True

                if download_type in ("image", "both"):
                    image_blobs = self.list_image_blobs(uuid, source_flag)
                    for blob_name in image_blobs:
                        file_bytes = self.download_blob_bytes(blob_name, source_flag)
                        relative_name = blob_name.replace(f"{uuid}/", "")
                        zf.writestr(f"{uuid}/{relative_name}", file_bytes)
                        item_summary["images_added"] += 1

                summary.append(item_summary)

        zip_buffer.seek(0)
        return zip_buffer.getvalue(), summary