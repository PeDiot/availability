from typing import List, Tuple

import time
from tqdm import tqdm
from google.cloud import bigquery

from pinecone.data.index import Index, ScoredVector
from .models import PineconeEntry, PineconeDataLoader


BATCH_SIZE = 1000
SLEEP_TIME = 30


def delete_points_from_ids(
    index: Index, ids: List[str], verbose: bool = False
) -> Tuple[float, List[str]]:
    if len(ids) == 0:
        return False

    iterator = range(0, len(ids), BATCH_SIZE)
    n_success, failed = 0, []

    if verbose:
        iterator = tqdm(iterable=iterator, total=int(len(ids) // BATCH_SIZE))

    for i in iterator:
        try:
            batch = ids[i : i + BATCH_SIZE]
            response = index.delete(ids=batch)
            success = int(len(response) == 0)

        except Exception as e:
            print(e)
            success = 0
            failed.extend(batch)

        n_success += success
        success_rate = n_success / (i + 1)

        if not success:
            time.sleep(SLEEP_TIME)

        if verbose:
            iterator.set_description(f"Success rate: {success_rate:.2f}")

    return success_rate, failed


def delete_points_from_bigquery_iterator(
    index: Index,
    iterator: bigquery.table.RowIterator,
    id_field: str,
    batch_size: int = BATCH_SIZE,
    verbose: bool = False,
) -> Tuple[float, List[str]]:
    total_rows = iterator.total_rows
    if verbose:
        iterator = tqdm(
            iterable=enumerate(iterator),
            total=total_rows,
        )

    current_batch, failed, n, n_success = [], [], 0, 0

    for ix, row in iterator:
        row = dict(row)
        entry = row.get(id_field)
        current_batch.append(entry)

        if len(current_batch) >= batch_size or n == total_rows - 1:
            try:
                response = index.delete(ids=current_batch)
                success = int(len(response) == 0)

            except Exception as e:
                failed.extend(current_batch)
                success = 0

            n += 1
            n_success += success
            success_rate = n_success / n
            current_batch = []

            if not success:
                time.sleep(SLEEP_TIME)

            if verbose:
                iterator.set_description(
                    f"Batch: {n} | "
                    f"Processed: {ix+1} | "
                    f"Success: {n_success} | "
                    f"Success rate: {success_rate:.2f}"
                )

    return success_rate, failed


def get_vectors(index: Index, point_ids: List[str]) -> List[ScoredVector]:
    response = index.fetch(ids=point_ids)

    return response.vectors.values()


def get_neighbors(index: Index, point_id: str, n: int) -> PineconeDataLoader:
    loader = PineconeDataLoader()

    results = index.query(
        id=point_id,
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
