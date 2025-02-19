from dataclasses import dataclass
from enum import Enum


class ItemStatus(Enum):
    AVAILABLE = "available"
    SOLD = "sold"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


@dataclass
class JobConfig:
    id: str
    index: int
    only_top_brands: bool
    sort_by_likes: bool
    sort_by_date: bool