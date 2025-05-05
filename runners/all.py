import sys

sys.path.append("/app")

from typing import List
import json, os

import src

NUM_ITEMS = 100000
VINTED_DRESSING_ALPHA = 0.3
TOP_BRANDS_ALPHA = 0.3
IS_WOMEN_ALPHA = 0.7
SORT_BY_DATE_ALPHA = 0.5
RUNNER_MODE = "api"


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    bq_client, pinecone_index, vinted_client, driver = src.config.init_clients(
        secrets=secrets,
        mode=RUNNER_MODE,
    )

    config = src.config.init_config(
        bq_client=bq_client,
        pinecone_index=pinecone_index,
        vinted_client=vinted_client,
        driver=driver,
        vintage_dressing_alpha=VINTED_DRESSING_ALPHA,
        top_brands_alpha=TOP_BRANDS_ALPHA,
        is_women_alpha=IS_WOMEN_ALPHA,
        sort_by_date_alpha=SORT_BY_DATE_ALPHA,
    )

    return src.runner.Runner(
        mode=RUNNER_MODE,
        config=config,
    )


def get_loader(runner: src.runner.Runner) -> List[dict]:
    query_kwargs = {
        "n": NUM_ITEMS,
        "only_top_brands": runner.config.only_top_brands,
        "only_vintage_dressing": runner.config.only_vintage_dressing,
        "is_women": runner.config.is_women,
        "sort_by_date": runner.config.sort_by_date,
    }

    query = src.bigquery.query_items(index=runner.config.index, **query_kwargs)

    loader = src.bigquery.run_query(
        client=runner.config.bq_client, query=query, to_list=False
    )

    if loader.total_rows == 0:
        runner.config.index = 0
        query = src.bigquery.query_items(index=runner.config.index, **query_kwargs)
        loader = src.bigquery.run_query(
            client=runner.config.bq_client, query=query, to_list=False
        )

    return loader


if __name__ == "__main__":
    runner = init_runner()
    print(f"Config: {runner.config.id} | Index: {runner.config.index}")

    data_loader = get_loader(runner)

    if src.bigquery.update_job_index(
        runner.config.bq_client, runner.config.id, runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")

    runner.run(data_loader)
