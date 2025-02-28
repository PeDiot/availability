import sys
sys.path.append("/app")

from typing import List, Dict
import json, os

import src


BATCH_SIZE = 200
NUM_NEIGHBORS = 20


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    return src.runner.Runner(
        secrets=secrets,
        use_api=False,
        from_interactions=True,
        top_brands_alpha=0.0,
        sort_by_likes_alpha=0.0,
        sort_by_date_alpha=0.0,
    )


def load_point_ids(runner: src.runner.Runner) -> List[List[Dict]]:
    query = src.bigquery.query_user_interactions()
    loader = src.bigquery.run_query(client=runner.bq_client, query=query, to_list=False)

    return src.bigquery.create_batches(loader, BATCH_SIZE)


def get_loader(runner: src.runner.Runner, point_ids: List[str]) -> List[List[Dict]]:
    neighbors = src.pinecone.get_neighbors(
        runner.pinecone_index, 
        point_ids, 
        NUM_NEIGHBORS, 
    )

    item_ids = [entry.metadata["id"] for entry in neighbors]
    query = src.bigquery.query_items(item_ids=item_ids)

    return src.bigquery.run_query(client=runner.bq_client, query=query, to_list=False)


if __name__ == "__main__":
    runner = init_runner()
    batch_point_ids = load_point_ids(runner)

    for ix, batch in enumerate(batch_point_ids):
        print(f"Processing batch {ix + 1}/{len(batch_point_ids)}")
        
        point_ids = [row.point_id for row in batch]
        data_loader = get_loader(runner, point_ids)
        runner.run(data_loader)