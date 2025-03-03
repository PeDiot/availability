import sys
sys.path.append("/app")

from typing import List
import json, os
from google.cloud import bigquery

import src


NUM_ITEMS = 200
NUM_NEIGHBORS = 30


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


def load_point_ids(runner: src.runner.Runner) -> List[str]:
    query = src.bigquery.query_user_interactions(
        n=NUM_ITEMS,
        index=runner.config.index,
    )

    loader = src.bigquery.run_query(
        client=runner.bq_client, 
        query=query, 
        to_list=False
    )

    if loader.total_rows == 0:
        runner.config.index = 0
        loader = load_point_ids(runner)

    return [row.point_id for row in loader]


def get_loader(runner: src.runner.Runner, point_ids: List[str]) -> List[bigquery.table.RowIterator]:
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

    if src.bigquery.update_job_index(
        runner.bq_client, 
        runner.config.id, 
        runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")

    point_ids = load_point_ids(runner)       
    data_loader = get_loader(runner, point_ids)
    runner.run(data_loader)