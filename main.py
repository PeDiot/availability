import sys

sys.path.append("../")

from typing import List, Tuple, Optional

import tqdm, json, os, random
from datetime import datetime

from pinecone import Pinecone, data
from google.cloud import bigquery
from selenium.webdriver.chrome.webdriver import WebDriver

import src


DOMAIN = "fr"
USE_API = False
JOB_PREFIX = "availability"
NUM_ITEMS = 1000
DRIVER_SLEEP_EVERY = 200
UPDATE_EVERY = 50
TOP_BRANDS_ALPHA = 0.0
SORT_BY_LIKES_ALPHA = 0.0
SORT_BY_DATE_ALPHA = 0.0


def init_clients(
    secrets: dict,
) -> Tuple[
    bigquery.Client, Pinecone, Optional[src.vinted.client.Vinted], Optional[WebDriver]
]:
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    if USE_API:
        vinted_client = src.vinted.client.Vinted(domain=DOMAIN)
        driver = None
    else:
        driver = src.driver.init_webdriver()
        vinted_client = None

    return bq_client, pinecone_index, vinted_client, driver


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
    row: bigquery.Row,
    client: src.vinted.client.Vinted,
    driver: WebDriver,
    vinted_ids: List[str],
    item_ids: List[str],
    n_available: int,
    n_unavailable: int,
    n_success: int,
) -> Tuple[List[str], List[str], int, int, int, bool]:
    restart_driver = False
    status = src.status.get_item_status_from_web(row.url, driver)

    if status == src.models.ItemStatus.NOT_FOUND:
        status = src.status.get_item_status_from_api(client, int(row.vinted_id))
        restart_driver = True

    is_available = src.status.is_available(status)

    if is_available is None:
        pass
    elif is_available:
        n_available += 1
        n_success += 1
    else:
        n_unavailable += 1
        n_success += 1
        item_ids.append(row.id)
        vinted_ids.append(row.vinted_id)

    return vinted_ids, item_ids, n_available, n_unavailable, n_success, restart_driver


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
    bq_client, pinecone_index, vinted_client, driver = init_clients(secrets)

    config = init_job_config(bq_client)
    print(config)
    loader = get_data_loader(bq_client, config)

    if loader.total_rows == 0:
        config.index = 0
        loader = get_data_loader(bq_client, config)

    if src.bigquery.update_job_index(bq_client, config.id, config.index + 1):
        print(f"Updated job index for {config.id} to {config.index+1}.")
    else:
        print(f"Failed to update job index for {config.id}.")

    item_ids, vinted_ids, pinecone_point_ids = [], [], []
    n, n_success, n_available, n_unavailable, n_updated = 0, 0, 0, 0, 0
    loop = tqdm.tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1

        (
            vinted_ids,
            item_ids,
            n_available,
            n_unavailable,
            n_success,
            restart_driver,
        ) = process_item(
            row,
            vinted_client,
            driver,
            vinted_ids,
            item_ids,
            n_available,
            n_unavailable,
            n_success,
        )

        if (driver and n % DRIVER_SLEEP_EVERY == 0) or restart_driver:
            driver.quit()
            driver = src.driver.init_webdriver()
            src.driver.gaussian_sleep(driver)

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
