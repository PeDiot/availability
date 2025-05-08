from typing import List, Iterator, Dict, Literal, Optional
from dataclasses import dataclass, field
from enum import Enum

from random import random
from google.cloud import bigquery
from pinecone import Pinecone, ScoredVector
from supabase import Client
from selenium.webdriver.chrome.webdriver import WebDriver

from .bigquery import get_job_index
from .vinted.client import Vinted


RunnerMode = Literal["api", "driver"]


class ItemStatus(Enum):
    AVAILABLE = "available"
    SOLD = "sold"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


@dataclass
class JobConfig:
    bq_client: bigquery.Client
    pinecone_index: Pinecone.Index
    vinted_client: Vinted
    only_top_brands: bool
    only_vintage_dressing: bool
    sort_by_likes: bool
    sort_by_date: bool
    from_interactions: bool
    from_saved: bool
    is_women: bool
    driver: Optional[WebDriver] = None
    supabase_client: Optional[Client] = None

    def __post_init__(self):
        if self.only_vintage_dressing and self.only_top_brands:
            if random() < 0.5:
                self.only_vintage_dressing = False
            else:
                self.only_top_brands = False

        if self.sort_by_date and self.sort_by_likes:
            if random() < 0.5:
                self.sort_by_date = False
            else:
                self.sort_by_likes = False

        self._get_id()

    def set_index(self, index: Optional[int] = None):
        if index is None:
            self.index = get_job_index(self.bq_client, self.id)
        else:
            self.index = index

    def _get_id(self):
        if self.from_interactions:
            self.id = "interactions"
        elif self.only_top_brands:
            self.id = "top_brands"
        elif self.only_vintage_dressing:
            self.id = "vintage_dressing"
        elif self.from_saved:
            self.id = "saved"
        else:
            self.id = "all"

        if self.sort_by_likes:
            self.id += "_likes"
        elif self.sort_by_date:
            self.id += "_date"

        if self.is_women is not None:
            self.id += f"_women_{self.is_women}"


@dataclass
class PineconeEntry:
    id: str
    point_id: str
    vinted_id: str
    url: str

    @classmethod
    def from_vector(cls, vector: ScoredVector) -> "PineconeEntry":
        return cls(
            id=vector.metadata["id"],
            point_id=vector.id,
            vinted_id=vector.metadata["vinted_id"],
            url=vector.metadata["url"],
        )

    @classmethod
    def from_dict(cls, data: Dict) -> "PineconeEntry":
        return cls(
            id=data["id"],
            point_id=data["point_id"],
            vinted_id=data["vinted_id"],
            url=data["url"],
        )


@dataclass
class PineconeDataLoader:
    entries: List[PineconeEntry] = field(default_factory=list)

    def add(self, entry: PineconeEntry) -> None:
        self.entries.append(entry)

    def __iter__(self) -> Iterator[PineconeEntry]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    @property
    def total_rows(self) -> int:
        return len(self.entries)
