from typing import List
from pinecone.data.index import Index


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
