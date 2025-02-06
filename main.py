import sys

sys.path.append("../")

from typing import List, Tuple

import tqdm, json, os
from datetime import datetime
from vinted import Vinted
from pinecone import Pinecone
from google.cloud import bigquery
import src


UPDATE_EVERY = 200
NUM_ITEMS = 10000
DOMAIN = "fr"


def update(client: bigquery.Client, unavailable_items: List[Tuple[str, str]]) -> bool:
    current_time = datetime.now().isoformat()

    try:
        rows = [
            {"vinted_id": vinted_id, "updated_at": current_time}
            for _, vinted_id in unavailable_items
        ]
        errors = client.insert_rows_json(
            table=f"{src.enums.DATASET_ID}.{src.enums.SOLD_TABLE_ID}",
            json_rows=rows,
        )
        success = not errors

    except Exception as e:
        print(e)
        success = False

    if success:
        item_ids_str = ", ".join([f"'{item_id}'" for item_id, _ in unavailable_items])

        pinecone_points = src.bigquery.load_table(
            client=bq_client,
            table=f"`{src.enums.DATASET_ID}.{src.enums.PINECONE_TABLE_ID}`",
            conditions=[f"item_id in ({item_ids_str})"],
            fields=["point_id"],
            to_list=True,
        )

        pinecone_point_ids = [point["point_id"] for point in pinecone_points]

        success = src.pinecone.delete_points(pinecone_index, pinecone_point_ids)

    return success


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

    loader = src.bigquery.load_table(
        client=bq_client,
        table=src.bigquery.BASE_QUERY,
        conditions=query_conditions,
        limit=NUM_ITEMS,
        to_list=False,
    )

    unavailable_items = []
    n, n_success, n_available, n_unavailable, n_updated = 0, 0, 0, 0, 0
    loop = tqdm.tqdm(iterable=loader, total=loader.total_rows)

    for row in loop:
        n += 1

        try:
            is_available = src.status.is_available(
                client=vinted_client, item_id=int(row.vinted_id), item_url=row.url
            )

            if is_available is None:
                continue

            n_success += 1

            if is_available is False:
                unavailable_items.append((row.id, row.vinted_id))
                n_unavailable += 1
            else:
                n_available += 1

        except Exception as e:
            pass

        if n % UPDATE_EVERY == 0 and unavailable_items:
            if update(bq_client, unavailable_items):
                n_updated += len(unavailable_items)

            unavailable_items = []

        loop.set_description(
            f"Processed: {n} | "
            f"Success: {n_success} | "
            f"Success rate: {n_success / n:.2f} | "
            f"Available: {n_available} | "
            f"Unavailable: {n_unavailable} | "
            f"Updated: {n_updated}"
        )

    if unavailable_items:
        if update(bq_client, unavailable_items):
            n_updated += len(unavailable_items)

        loop.set_description(
            f"Processed: {n} | "
            f"Success: {n_success} | "
            f"Success rate: {n_success / n:.2f} | "
            f"Available: {n_available} | "
            f"Unavailable: {n_unavailable} | "
            f"Updated: {n_updated}"
        )


if __name__ == "__main__":
    main()
