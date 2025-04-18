import sys

sys.path.append("/app")

from typing import List
import json, os

import src

NUM_ITEMS = 1000
NUM_NEIGHBORS = 50
USE_API = True
SHUFFLE = True


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    return src.runner.Runner(
        secrets=secrets,
        use_api=USE_API,
        from_interactions=True,
        update_every=NUM_NEIGHBORS,
    )


def load_point_ids(runner: src.runner.Runner) -> List[str]:
    query = src.bigquery.query_interaction_items(
        n=NUM_ITEMS,
        shuffle=SHUFFLE,
    )

    loader = src.bigquery.run_query(client=runner.bq_client, query=query, to_list=False)

    if loader.total_rows == 0:
        runner.config.index = 0
        loader = load_point_ids(runner)

    point_ids = []
    for row in loader:
        if row.point_id not in point_ids:
            point_ids.append(row.point_id)

    return point_ids


if __name__ == "__main__":
    runner = init_runner()

    if not SHUFFLE and src.bigquery.update_job_index(
        runner.bq_client, runner.config.id, runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")

    point_ids = load_point_ids(runner)
    vectors = src.pinecone.get_vectors(runner.pinecone_index, point_ids)
    num_vectors = len(vectors)

    for ix, vector in enumerate(vectors):
        print(f"Processing vector {ix+1}/{num_vectors}")

        data_loader = src.pinecone.get_neighbors(
            index=runner.pinecone_index,
            vectors=[vector],
            n=NUM_NEIGHBORS,
        )

        runner.run(data_loader)
        runner.restart_driver()
