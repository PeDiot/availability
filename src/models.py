from typing import List, Iterator
from dataclasses import dataclass, field
from enum import Enum
from random import random
from pinecone.core.openapi.data.model.scored_vector import ScoredVector


class ItemStatus(Enum):
    AVAILABLE = "available"
    SOLD = "sold"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


@dataclass
class JobConfig:
    only_top_brands: bool
    only_vintage_dressing: bool
    sort_by_likes: bool
    sort_by_date: bool
    from_interactions: bool

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
        self.index = -1

    def set_index(self, index: int):
        self.index = index

    def _get_id(self):
        if self.from_interactions:
            self.id = "interactions"
        elif self.only_top_brands:
            self.id = "top_brands"
        elif self.only_vintage_dressing:
            self.id = "vintage_dressing"
        elif self.sort_by_likes:
            self.id = "likes"
        elif self.sort_by_date:
            self.id = "date"
        else:
            self.id = "all"


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
