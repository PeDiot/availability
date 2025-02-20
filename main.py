import sys

sys.path.append("../")

from typing import List, Tuple

import tqdm, json, os, random
from datetime import datetime
from pinecone import Pinecone, data
from google.cloud import bigquery

import src


DOMAIN = "fr"
USE_API = True
JOB_PREFIX = "availability"
UPDATE_EVERY = 100
NUM_ITEMS = 1000
TOP_BRANDS_ALPHA = 0.0
SORT_BY_LIKES_ALPHA = 0.0
SORT_BY_DATE_ALPHA = 0.0


def init_clients(
    secrets: dict, domain: str
) -> Tuple[bigquery.Client, Pinecone, src.vinted.client.Vinted]:
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    vinted_client = src.vinted.client.Vinted(domain=domain)

    return bq_client, pinecone_index, vinted_client


def init_job_config(client: bigquery.Client) -> src.models.JobConfig:
    only_top_brands = random.random() < TOP_BRANDS_ALPHA
    sort_by_likes = random.random() < SORT_BY_LIKES_ALPHA
    sort_by_date = random.random() < SORT_BY_DATE_ALPHA

    if only_top_brands:
        job_id = f"{JOB_PREFIX}_top_brands"
    elif sort_by_likes:
        job_id = f"{JOB_PREFIX}_likes"
    elif sort_by_date:
        job_id = f"{JOB_PREFIX}_date"
    else:
        job_id = f"{JOB_PREFIX}_all"

    index = src.bigquery.get_job_index(client, job_id)

    return src.models.JobConfig(
        id=job_id,
        index=index,
        only_top_brands=only_top_brands,
        sort_by_likes=sort_by_likes,
        sort_by_date=sort_by_date,
    )


def get_data_loader(
    client: bigquery.Client, config: src.models.JobConfig
) -> bigquery.table.RowIterator:
    query = src.bigquery.query_active_items(
        n=NUM_ITEMS,
        job_prefix=JOB_PREFIX,
        index=config.index,
        only_top_brands=config.only_top_brands,
        sort_by_date=config.sort_by_date,
        sort_by_likes=config.sort_by_likes,
    )

    return src.bigquery.run_query(client, query, to_list=False)


def process_item(
    client: src.vinted.client.Vinted, row: bigquery.Row
) -> Tuple[bool, str, str]:
    try:
        is_available = src.status.is_available(
            client=client,
            item_id=int(row.vinted_id),
            item_url=row.url,
            use_api=USE_API,
        )

        if is_available is None:
            return False, None, None

        if is_available is False:
            return True, row.id, row.vinted_id

        return True, None, None

    except Exception:
        return False, None, None


def check_update(item_ids: List[str], vinted_ids: List[str]) -> bool:
    return item_ids and len(item_ids) == len(vinted_ids)


def update(
    client: bigquery.Client,
    index: data.index.Index,
    item_ids: List[str],
    vinted_ids: List[str],
) -> bool:
    success = False
    current_time = datetime.now().isoformat()

    pinecone_points_query = src.bigquery.query_pinecone_points(item_ids)
    pinecone_points = src.bigquery.run_query(
        client, pinecone_points_query, to_list=False
    )
    pinecone_point_ids = [row.point_id for row in pinecone_points]

    if not src.pinecone.delete_points(index, pinecone_point_ids):
        pinecone_point_ids = []
    else:
        try:
            rows = []

            for vinted_id in vinted_ids:
                rows.append({"vinted_id": vinted_id, "updated_at": current_time})

            errors = client.insert_rows_json(
                table=f"{src.enums.DATASET_ID}.{src.enums.SOLD_TABLE_ID}",
                json_rows=rows,
            )
            success = not errors
        except:
            success = False
            pinecone_point_ids = []

    return success, pinecone_point_ids


def main() -> None:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    bq_client, pinecone_index, vinted_client = init_clients(secrets, DOMAIN)

    config = init_job_config(bq_client)
    print(config)
    loader = get_data_loader(bq_client, config)

    if loader.total_rows == 0:
        config.index = 0
        loader = get_data_loader(bq_client, config)

    # if src.bigquery.update_job_index(bq_client, config.id, config.index + 1):
    #     print(f"Updated job index for {config.id} to {config.index+1}.")
    # else:
    #     print(f"Failed to update job index for {config.id}.")

    item_ids, vinted_ids, pinecone_point_ids = [], [], []
    n = n_success = n_available = n_unavailable = n_updated = 0
    loop = tqdm.tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1
        success, item_id, vinted_id = process_item(vinted_client, row)

        if success:
            n_success += 1

            if item_id:
                item_ids.append(item_id)
                vinted_ids.append(vinted_id)
                n_unavailable += 1
            else:
                n_available += 1

        if n % UPDATE_EVERY == 0 and check_update(item_ids, vinted_ids):
            success, pinecone_point_ids_ = update(
                bq_client, pinecone_index, item_ids, vinted_ids
            )

            if success:
                n_updated += len(item_ids)
                pinecone_point_ids.extend(pinecone_point_ids_)

            item_ids, vinted_ids = [], []

        loop.set_description(
            f"Processed: {n} | "
            f"Success: {n_success} | "
            f"Success rate: {n_success / n:.2f} | "
            f"Available: {n_available} | "
            f"Unavailable: {n_unavailable} | "
            f"Updated: {n_updated}"
        )

    if check_update(item_ids, vinted_ids):
        success, pinecone_point_ids_ = update(
            bq_client, pinecone_index, item_ids, vinted_ids
        )

        if success:
            n_updated += len(item_ids)

        if pinecone_point_ids_:
            if src.pinecone.delete_points(pinecone_index, pinecone_point_ids_):
                pinecone_point_ids.extend(pinecone_point_ids_)

    print(f"Deleted {len(pinecone_point_ids)} points.")


if __name__ == "__main__":
    main()
