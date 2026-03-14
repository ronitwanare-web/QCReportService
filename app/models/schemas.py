from enum import Enum
from typing import List, Optional
from pydantic import BaseModel


class SourceFlag(str, Enum):
    centum = "centum"
    sks = "sks"


class DownloadType(str, Enum):
    report = "report"
    image = "image"
    both = "both"


class UUIDFetchRequest(BaseModel):
    station_type: str
    start_time: str
    end_time: str
    source_flag: SourceFlag
    size: int = 10000


class DownloadReportsRequest(BaseModel):
    station_type: str
    start_time: str
    end_time: str
    source_flag: SourceFlag
    download_type: DownloadType = DownloadType.both
    uuid_list: Optional[List[str]] = None
    single_uuid: Optional[str] = None


class MetricsCsvRequest(BaseModel):
    station_type: str
    start_time: str
    end_time: str
    source_flag: SourceFlag
    uuid_list: Optional[List[str]] = None
    single_uuid: Optional[str] = None
    size: int = 10000