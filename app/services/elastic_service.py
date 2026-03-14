from typing import Any, Dict, List
from elasticsearch import Elasticsearch

from app.config import settings


class ElasticService:
    def __init__(self) -> None:
        self.es = Elasticsearch(
            cloud_id=settings.ELASTIC_CLOUD_ID,
            http_auth=(settings.ELASTIC_USERNAME, settings.ELASTIC_PASSWORD),
        )

    def _get_index(self, source_flag: str) -> str:
        return settings.SOURCE_CONFIG[source_flag]["elastic_index"]

    def fetch_reports_by_filters(
    self,
    station_type: str,
    start_time: str,
    end_time: str,
    source_flag: str,
    size: int = 10000,
    ) -> List[Dict[str, Any]]:
        index_name = self._get_index(source_flag)
        time_field = settings.ELASTIC_TIME_FIELD

        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"station.jig.type.keyword": station_type}},
                        {
                            "range": {
                                time_field: {
                                    "gte": start_time,
                                    "lte": end_time,
                                }
                            }
                        },
                    ]
                }
            },
            "_source": [
                "uuid",
                "station.jig.type",
                time_field,
                "report.tests",
            ],
            "sort": [{time_field: {"order": "desc"}}],
            "size": size,
        }

        res = self.es.search(
            index=index_name,
            body=query,
            request_timeout=60,
        )
        return res.get("hits", {}).get("hits", [])

    def fetch_uuids(
    self,
    station_type: str,
    start_time: str,
    end_time: str,
    source_flag: str,
    size: int = 10000,
    ) -> List[Dict[str, Any]]:
        docs = self.fetch_reports_by_filters(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )

        rows: List[Dict[str, Any]] = []
        seen = set()

        for hit in docs:
            src = hit.get("_source", {})
            uuid = src.get("uuid")
            jig_type = src.get("station.jig.type")
            timestamp = src.get(settings.ELASTIC_TIME_FIELD)

            if not uuid or uuid in seen:
                continue

            seen.add(uuid)
            rows.append(
                {
                    "uuid": uuid,
                    "station_type": jig_type,
                    "timestamp": timestamp,
                    "img_av": False,
                }
            )

        return rows

    def fetch_metrics_rows(
        self,
        station_type: str,
        start_time: str,
        end_time: str,
        metric_names: List[str],
        source_flag: str,
        size: int = 10000,
    ) -> List[Dict[str, Any]]:
        docs = self.fetch_reports_by_filters(
            station_type=station_type,
            start_time=start_time,
            end_time=end_time,
            source_flag=source_flag,
            size=size,
        )

        rows: List[Dict[str, Any]] = []

        for hit in docs:
            src = hit.get("_source", {})
            uuid = src.get("uuid")
            jig_type = src.get("station.jig.type")
            timestamp = src.get(settings.ELASTIC_TIME_FIELD)
            tests = src.get("report.tests")

            if tests is None:
                tests = []
            elif isinstance(tests, dict):
                tests = [dict(v, test_name=k) for k, v in tests.items() if isinstance(v, dict)]
            elif not isinstance(tests, list):
                tests = [tests]

            for i, test in enumerate(tests):
                row = {
                    "uuid": uuid,
                    "station_type": jig_type,
                    "timestamp": timestamp,
                    "test_index": i,
                }

                if isinstance(test, dict):
                    row["test_name"] = test.get("test_name")
                    for metric in metric_names:
                        row[metric] = test.get(metric)
                else:
                    row["test_raw"] = str(test)

                rows.append(row)

        return rows