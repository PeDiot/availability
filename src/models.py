from typing import List, Dict, Iterator
from dataclasses import dataclass, field
from enum import Enum

from pinecone.core.openapi.data.model.scored_vector import ScoredVector


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
    from_interactions: bool


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
