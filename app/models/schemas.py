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


class ImageVariant(str, Enum):
    g0 = "G0"
    g4 = "G4"
    g6 = "G6"
    all = "all"


class CameraEV(str, Enum):
    minus1 = "-1EV"
    zero = "0EV"
    plus1 = "+1EV"


class PreeolPhase(str, Enum):
    g0 = "G0"
    g4 = "G4"
    g6 = "G6"


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
    image_variant: Optional[ImageVariant] = None
    uuid_list: Optional[List[str]] = None
    single_uuid: Optional[str] = None


class MetricsCsvRequest(BaseModel):
    station_type: str
    start_time: str
    end_time: str
    source_flag: SourceFlag
    preeol_phase: Optional[PreeolPhase] = None
    camera_ev: Optional[CameraEV] = None
    uuid_list: Optional[List[str]] = None
    single_uuid: Optional[str] = None
    size: int = 10000