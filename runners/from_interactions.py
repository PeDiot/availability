import sys

sys.path.append("/app")

from typing import List
import json, os, tqdm

import src


NUM_ITEMS = 1000
NUM_NEIGHBORS = 50
SHUFFLE = True
RUNNER_MODE = "api"


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    bq_client, pinecone_index, vinted_client, driver, _ = src.config.init_clients(
        secrets=secrets,
        mode=RUNNER_MODE,
    )

    config = src.config.init_config(
        bq_client=bq_client,
        pinecone_index=pinecone_index,
        vinted_client=vinted_client,
        driver=driver,
        from_interactions=True,
    )

    return src.runner.Runner(
        mode=RUNNER_MODE,
        config=config,
    )


def load_point_ids(runner: src.runner.Runner) -> List[str]:
    query = src.bigquery.query_interaction_items(
        n=NUM_ITEMS,
        shuffle=SHUFFLE,
    )

    loader = src.bigquery.run_query(
        client=runner.config.bq_client, query=query, to_list=False
    )

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
        runner.config.bq_client, runner.config.id, runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")

    point_ids = load_point_ids(runner)
    loop = tqdm.tqdm(iterable=point_ids, total=len(point_ids))

    for point_id in loop:
        data_loader = src.pinecone.get_neighbors(
            index=runner.config.pinecone_index,
            point_id=point_id,
            n=NUM_NEIGHBORS,
        )

        runner.run(data_loader, loop)