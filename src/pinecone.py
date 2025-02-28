from typing import List, Dict, Optional
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


def get_neighbors(index: Index, point_ids: List[str], n: int) -> Optional[List[Dict]]:
    points = []
    response = index.fetch(ids=point_ids)
    
    if not response.vectors:
        return
        
    for vector in response.vectors.values():
        results = index.query(
            vector=vector.values,
            top_k=n,
            include_values=False,
            include_metadata=True,
        )
        
        points.extend(results.matches)
 
    return points