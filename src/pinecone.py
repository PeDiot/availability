from typing import List

from tqdm import tqdm
from pinecone.data.index import Index, ScoredVector
from .models import PineconeEntry, PineconeDataLoader


BATCH_SIZE = 1000


def delete_points(index: Index, ids: List[str], loop: bool = False) -> float:
    if len(ids) == 0:
        return False

    iterator = range(0, len(ids), BATCH_SIZE)
    n, n_success = 0, 0

    if loop:
        iterator = tqdm(iterable=iterator, total=int(len(ids) // BATCH_SIZE))

    try:
        for i in iterator:
            batch = ids[i : i + BATCH_SIZE]
            response = index.delete(ids=batch)

            n_success += len(response) == 0
            n += 1
            success = n_success / n

            if isinstance(iterator, tqdm):
                iterator.set_description(f"Success rate: {success:.2f}")

        return success

    except Exception as e:
        print(e)
        return -1.0


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
