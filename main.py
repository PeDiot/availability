import sys

sys.path.append("../")

from typing import List

import tqdm, json, os
from datetime import datetime
from vinted import Vinted
from pinecone import Pinecone

import src


UPDATE_EVERY = 100
NUM_ITEMS = 10000
DOMAIN = "fr"


def update(available_items: List[str], unavailable_items: List[str]) -> bool:
    unavailable_items_str = ", ".join([f"'{item}'" for item in unavailable_items])
    available_items_str = ", ".join([f"'{item}'" for item in available_items])

    iterator = zip(
        [unavailable_items_str, available_items_str],
        [False, True]
    )

    update_success = dict()

    for item_ids_str, is_available in iterator:
        success = src.bigquery.update_table(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=src.enums.ITEM_TABLE_ID,
            fields=["is_available", "updated_at"],
            new_values=[is_available, f"'{datetime.now().isoformat()}'"],
            conditions=[f"id IN ({item_ids_str})"],
        )

        update_success[is_available] = success

    if update_success.get(False):
        pinecone_points = src.bigquery.load_table(
            client=bq_client,
            table=f"`{src.enums.DATASET_ID}.{src.enums.PINECONE_TABLE_ID}`",
            conditions=[f"item_id in ({unavailable_items_str})"],
            fields=["point_id"],
            to_list=True,
        )

        pinecone_point_ids = [point["point_id"] for point in pinecone_points]

        if src.pinecone.delete_points(pinecone_index, pinecone_point_ids):
            return src.bigquery.delete_from_table(
                client=bq_client,
                dataset_id=src.enums.DATASET_ID,
                table_id=src.enums.PINECONE_TABLE_ID,
                conditions=[f"item_id in ({unavailable_items_str})"],
            )

    return False


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    shard_id = int(os.getenv("SHARD_ID", "0"))
    total_shards = int(os.getenv("TOTAL_SHARDS", "1"))

    global bq_client
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    global pinecone_index
    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    vinted_client = Vinted(domain=DOMAIN)

    query_conditions = [
        f"{src.enums.AVAILABLE_FIELD} = true",
        f"MOD(FARM_FINGERPRINT(CAST({src.enums.VINTED_ID_FIELD} AS STRING)), {total_shards}) = {shard_id}",
    ]

    order_by_clauses = [
        src.bigquery.OrderBy(field="updated_at", ascending=True),
        src.bigquery.OrderBy(field="num_likes", ascending=False),
    ]

    loader = src.bigquery.load_table(
        client=bq_client,
        table=src.bigquery.ITEMS_AND_LIKES_QUERY,
        conditions=query_conditions,
        order_by=order_by_clauses,
        limit=NUM_ITEMS,
        to_list=False,
    )

    available_items, unavailable_items = [], []
    n, n_success, n_unavailable, n_updated = 0, 0, 0, 0
    loop = tqdm.tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1

        try:
            item_id = int(row.vinted_id)
            is_available = src.status.is_available(
                client=vinted_client, item_id=item_id, item_url=row.url
            )

            if is_available is None:
                continue

            n_success += 1

            if is_available is False:
                unavailable_items.append(str(row.id))
                n_unavailable += 1
            else:
                available_items.append(str(row.id))

        except Exception as e:
            pass

        if n % UPDATE_EVERY == 0 and len(unavailable_items) > 0:
            if update(available_items, unavailable_items):
                n_updated += len(unavailable_items) + len(available_items)

            available_items, unavailable_items = [], []

        loop.set_description(
            f"Processed: {n} | "
            f"Success: {n_success} | "
            f"Success rate: {n_success / n:.2f} | "
            f"Unavailable: {n_unavailable} | "
            f"Updated: {n_updated}"
        )

    if unavailable_items:
        if update(available_items, unavailable_items):
            n_updated += len(unavailable_items) + len(available_items)

    loop.set_description(
        f"Unavailable: {n_unavailable} | " f"Processed: {n} | " f"Updated: {n_updated}"
    )


if __name__ == "__main__":
    main()
