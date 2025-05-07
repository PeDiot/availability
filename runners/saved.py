import sys

sys.path.append("/app")

from typing import List
import json, os

import src


NUM_ITEMS = 30000
RUNNER_MODE = "api"


def init_runner() -> src.runner.Runner:
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    (
        bq_client,
        pinecone_index,
        vinted_client,
        driver,
        supabase_client,
    ) = src.config.init_clients(
        secrets=secrets,
        mode=RUNNER_MODE,
        with_supabase=True,
    )

    config = src.config.init_config(
        bq_client=bq_client,
        supabase_client=supabase_client,
        pinecone_index=pinecone_index,
        vinted_client=vinted_client,
        driver=driver,
        from_saved=True,
    )

    return src.runner.Runner(
        mode=RUNNER_MODE,
        config=config,
    )


def get_loader(runner: src.runner.Runner) -> src.models.PineconeDataLoader:
    entries = src.supabase.get_saved_items(
        client=runner.config.supabase_client,
        n=NUM_ITEMS,
        index=runner.config.index,
    )

    if len(entries) == 0:
        runner.config.index = 0

        entries = src.supabase.get_saved_items(
            client=runner.config.supabase_client,
            n=NUM_ITEMS,
            index=runner.config.index,
        )

    return src.models.PineconeDataLoader(entries)


if __name__ == "__main__":
    runner = init_runner()
    runner.config.index = 0
    print(f"Config: {runner.config.id} | Index: {runner.config.index}")

    data_loader = get_loader(runner)

    if src.bigquery.update_job_index(
        runner.config.bq_client, runner.config.id, runner.config.index + 1
    ):
        print(f"Updated job index for {runner.config.id} to {runner.config.index+1}.")

    runner.run(data_loader)