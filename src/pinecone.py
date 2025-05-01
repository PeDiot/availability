from typing import List, Optional

from pinecone.data.index import Index, ScoredVector
from .models import PineconeEntry, PineconeDataLoader


BATCH_SIZE = 1000


def delete_points(index: Index, ids: List[str]) -> bool:
    if len(ids) == 0:
        return False

    try:
        for i in range(0, len(ids), BATCH_SIZE):
            batch = ids[i : i + BATCH_SIZE]
            index.delete(ids=batch)
        return True

    except Exception as e:
        print(e)
        return False


def get_vectors(index: Index, point_ids: List[str]) -> List[ScoredVector]:
    response = index.fetch(ids=point_ids)

    return response.vectors.values()


def get_neighbors(
    index: Index, vectors: List[ScoredVector], n: int
) -> PineconeDataLoader:
    loader = PineconeDataLoader()

    for vector in vectors:
        results = index.query(
            vector=vector.values,
            top_k=n,
            include_values=False,
            include_metadata=True,
        )

        for vector in results.matches:
            try:
                entry = PineconeEntry.from_vector(vector)
                loader.add(entry)
            except:
                pass

    return loader
