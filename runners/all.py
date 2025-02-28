import sys
sys.path.append("/app")

from google.cloud import bigquery
import json, os

import src


NUM_ITEMS = 10000
TOP_BRANDS_ALPHA = .5
SORT_BY_LIKES_ALPHA = 0.0
SORT_BY_DATE_ALPHA = 0.0


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    
    return src.runner.Runner(
        secrets=secrets, 
        use_api=False, 
        from_interactions=False,
        top_brands_alpha=TOP_BRANDS_ALPHA
    )


def get_loader(client: bigquery.Client, config: src.models.JobConfig) -> bigquery.table.RowIterator:
    query_kwargs = {
        "n": NUM_ITEMS,
        "only_top_brands": config.only_top_brands,
        "sort_by_date": config.sort_by_date,
        "sort_by_likes": config.sort_by_likes,
    }

    query = src.bigquery.query_items(index=config.index, **query_kwargs)
    loader = src.bigquery.run_query(client, query, to_list=False)

    if loader.total_rows == 0:
        config.index = 0
        query = src.bigquery.query_items(index=config.index, **query_kwargs)
        loader = src.bigquery.run_query(client, query, to_list=False)

    return loader


if __name__ == "__main__":
    runner = init_runner()
    data_loader= get_loader(runner.bq_client, runner.config)
    
    if src.bigquery.update_job_index(
        runner.bq_client, 
        runner.config.id, 
        runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")
        
    runner.run(data_loader) 