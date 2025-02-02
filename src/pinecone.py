from typing import List
from pinecone.data.index import Index


def delete_points(index: Index, ids: List[str]) -> bool:
    if len(ids) == 0:
        return False

    try:
        index.delete(ids=ids)
        return True

    except Exception as e:
        print(e)
        return False