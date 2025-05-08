import sys

sys.path.append("/app")

import json, os

import src


NUM_ITEMS = 1000
RUNNER_MODE = "api"
JOB_ID = "saved"


def init_runner() -> src.runner.Runner:
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
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    global bq_client, pinecone_index, vinted_client, driver, supabase_client

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

    runner = init_runner()
    index = runner.config.index

    while True:        
        print(f"Config: {runner.config.id} | Index: {runner.config.index}")

        data_loader = get_loader(runner)

        if len(data_loader.entries) == 0:
            raise Exception("No entries found")
        
        runner.run(data_loader)
        runner.config.set_index(index+1)
        
        src.bigquery.update_job_index(
            client=bq_client,
            job_id=JOB_ID,
            index=runner.config.index,
        )