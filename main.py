import sys
import itertools

sys.path.append("../")

from typing import List, Tuple, Optional

import tqdm, json, os
from datetime import datetime
from pinecone import Pinecone, data
from google.cloud import bigquery

import src


UPDATE_EVERY = 100
NUM_ITEMS = 1000
LIKES_ITEMS_ALPHA = 0.4
DOMAIN = "fr"


def update(client: bigquery.Client, index: data.index.Index, unavailable_items: List[Tuple[str, str]]) -> bool:
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
            client=client,
            table=f"`{src.enums.DATASET_ID}.{src.enums.PINECONE_TABLE_ID}`",
            conditions=[f"item_id in ({item_ids_str})"],
            fields=["point_id"],
            to_list=True,
        )

        pinecone_point_ids = [point["point_id"] for point in pinecone_points]
        success = src.pinecone.delete_points(index, pinecone_point_ids)

    return success, pinecone_point_ids


def init_clients(secrets: dict, domain: str) -> Tuple[bigquery.Client, Pinecone, src.vinted.client.Vinted]:
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")
    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)

    pinecone_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    pinecone_index = pinecone_client.Index(src.enums.PINECONE_INDEX_NAME)

    vinted_client = src.vinted.client.Vinted(domain=domain)
    
    return bq_client, pinecone_index, vinted_client


def get_data_loaders(
    client: bigquery.Client,
    shard_id: int,
    total_shards: int,
) -> Tuple[bigquery.table.RowIterator, bigquery.table.RowIterator, int]:
    query_conditions = [
        f"{src.enums.AVAILABLE_FIELD} = true",
        f"MOD(FARM_FINGERPRINT(CAST({src.enums.VINTED_ID_FIELD} AS STRING)), {total_shards}) = {shard_id}",
    ]

    num_likes_items = int(NUM_ITEMS *  LIKES_ITEMS_ALPHA)
    num_base_items = NUM_ITEMS - num_likes_items

    base_loader = src.bigquery.load_table(
        client=client,
        table=src.bigquery.BASE_QUERY,
        conditions=query_conditions,
        limit=num_base_items,
        to_list=False,
    )

    likes_loader = src.bigquery.load_table(
        client=client,
        table=src.bigquery.LIKES_QUERY,
        conditions=query_conditions,
        limit=num_likes_items,
        order_by=[src.bigquery.OrderBy(field="likes_count", ascending=False)],
        to_list=False,
    )

    total_rows = base_loader.total_rows + likes_loader.total_rows
    return base_loader, likes_loader, total_rows


def process_item(client: src.vinted.client.Vinted, row: bigquery.Row) -> Tuple[bool, Optional[Tuple[str, str]]]:
    try:
        is_available = src.status.is_available(
            client=client,
            item_id=int(row.vinted_id),
            item_url=row.url
        )
        
        if is_available is None:
            return False, None
            
        if is_available is False:
            return True, (row.id, row.vinted_id)
        
        return True, None
            
    except Exception:
        return False, None


def main() -> None:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    shard_id = int(os.getenv("SHARD_ID", "0"))
    total_shards = int(os.getenv("TOTAL_SHARDS", "1"))

    bq_client, pinecone_index, vinted_client = init_clients(secrets, DOMAIN)
    base_loader, likes_loader, total_rows = get_data_loaders(
        bq_client, shard_id, total_shards, NUM_ITEMS // 2, NUM_ITEMS - (NUM_ITEMS // 2)
    )

    unavailable_items: List[Tuple[str, str]] = []
    pinecone_point_ids: List[str] = []
    n = n_success = n_available = n_unavailable = n_updated = 0
    
    combined_loader = itertools.chain(base_loader, likes_loader)
    loop = tqdm.tqdm(iterable=combined_loader, total=total_rows)

    for row in loop:
        n += 1
        success, item = process_item(vinted_client, row)
        
        if success:
            n_success += 1
            if item:
                unavailable_items.append(item)
                n_unavailable += 1
            else:
                n_available += 1

        if n % UPDATE_EVERY == 0 and unavailable_items:
            success, pinecone_point_ids_ = update(bq_client, pinecone_index, unavailable_items)
            if success:
                n_updated += len(unavailable_items)
                pinecone_point_ids.extend(pinecone_point_ids_)
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
        success, pinecone_point_ids_ = update(bq_client, unavailable_items)
        if success:
            n_updated += len(unavailable_items)
            pinecone_point_ids.extend(pinecone_point_ids_)

    if pinecone_point_ids:
        if src.pinecone.delete_points(pinecone_index, pinecone_point_ids):
            print(f"Deleted {len(pinecone_point_ids)} points.")


if __name__ == "__main__":
    main()
