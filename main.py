import sys

sys.path.append("../")

from typing import Dict, List

import tqdm, json, os
from datetime import datetime
from vinted import Vinted
from pinecone import Pinecone

import src.vinted as src


SECRETS_PATH = "../secrets/secrets.json"
UPDATE_EVERY = 100
NUM_ITEMS = 1e4
DOMAIN = "fr"


def update(unavailable_items: List[str]) -> bool:
    unavailable_items_str = ", ".join([f"'{item}'" for item in unavailable_items])

    src.bigquery.update_table(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=src.enums.ITEM_TABLE_ID,
        fields=["is_available", "updated_at"],
        new_values=[False, f"'{datetime.now().isoformat()}'"],
        conditions=[f"id IN ({unavailable_items_str})"],
    )

    pinecone_points = src.bigquery.load_table(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=src.enums.PINECONE_TABLE_ID,
        conditions=[f"item_id in ({unavailable_items_str})"],
        fields=["point_id"],
        to_list=True,
    )

    pinecone_point_ids = [point["point_id"] for point in pinecone_points]

    try:
        pinecone_index.delete(ids=pinecone_point_ids)
        pinecone_update = True
    except Exception as e:
        print(e)
        bq_update = False
        pinecone_update = False

    if pinecone_update:
        bq_update = src.bigquery.delete_from_table(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=src.enums.PINECONE_TABLE_ID,
            conditions=[f"item_id in ({unavailable_items_str})"],
        )

    return bq_update and pinecone_update


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    global bq_client
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    global pinecone_index
    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    vinted_client = Vinted(domain=DOMAIN)

    loader = src.bigquery.load_table(
        client=bq_client,
        table_id=src.enums.ITEM_TABLE_ID,
        dataset_id=src.enums.DATASET_ID,
        conditions=["is_available = true"],
        order_by="updated_at",
        limit=NUM_ITEMS,
        to_list=False,
    )

    unavailable_items, n, n_success = [], 0, 0
    loop = tqdm.tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1

        try:
            info = vinted_client.item_info(row.vinted_id)

            if not info.item.can_be_sold:
                unavailable_items.append(str(row.id))

        except Exception as e:
            pass

        if n % UPDATE_EVERY == 0:
            if update(unavailable_items):
                n_success += len(unavailable_items)

            unavailable_items = []

        loop.set_description(
            f"Success rate: {n_success / n:.2f} | "
            f"Updated: {n_success} | "
            f"Processed: {n} | "
            f"Unavailable: {len(unavailable_items)}"
        )

    if unavailable_items:
        if update(unavailable_items):
            n_success += len(unavailable_items)

    loop.set_description(
        f"Success rate: {n_success / n:.2f} | "
        f"Updated: {n_success} | "
        f"Processed: {n} | "
        f"Unavailable: {len(unavailable_items)}"
    )


if __name__ == "__main__":
    main()