class Settings:
    APP_NAME = "report-downloader"
    APP_HOST = "0.0.0.0"
    APP_PORT = 8000

    # -----------------------
    # Elasticsearch
    # -----------------------
    ELASTIC_CLOUD_ID = "Inito_Logs:YXAtc291dGgtMS5hd3MuZWxhc3RpYy1jbG91ZC5jb206NDQzJGUwMzJmOWY2NjNlODQzOWJhNjIxMjk0YWUxMDEyOWZjJDFiY2E4NWFhZjM2MDQzMjk5N2FlNGE3MGFjYjgyNDAw"
    ELASTIC_USERNAME = "ronit.wanare@inito.com"
    ELASTIC_PASSWORD = "123456789"

    ELASTIC_TIME_FIELD = "@timestamp"

    # -----------------------
    # GCP / GCS
    # -----------------------
    GCP_PROJECT = "ng-manufacturing"
    GCP_CREDENTIALS_JSON = "/app/credentials/ng-manufacturing-c132f4a2ca85-readonly.json"

    # -----------------------
    # Per source config
    # -----------------------
    SOURCE_CONFIG = {
        "centum": {
            "elastic_index": "ngqc-test-reports-centum",
            "gcs_bucket": "ng_qc_reports_centum",
        },
        "sks": {
            "elastic_index": "ngqc-test-reports",
            "gcs_bucket": "ng_qc_reports",
        },
    }

    # -----------------------
    # Streamlit -> FastAPI
    # -----------------------
    FASTAPI_BASE_URL = "http://backend:8000"


settings = Settings()