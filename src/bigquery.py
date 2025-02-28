from typing import List, Dict, Union, Optional

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace("\\n", "\n")
    
    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def run_query(
    client: bigquery.Client, query: str, to_list: bool = True
) -> Union[List[Dict], bigquery.table.RowIterator]:
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    if to_list:
        return [dict(row) for row in results]
    else:
        return results


def get_job_index(client: bigquery.Client, job_id: str) -> int:
    query = f"""
    MERGE `{PROJECT_ID}.{VINTED_DATASET_ID}.{INDEX_TABLE_ID}` T
    USING (SELECT '{job_id}' as job_id) S
    ON T.job_id = S.job_id
    WHEN NOT MATCHED THEN
        INSERT (job_id, value) VALUES ('{job_id}', 0)
    WHEN MATCHED THEN
        UPDATE SET value = value;
    
    SELECT value
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{INDEX_TABLE_ID}`
    WHERE job_id = '{job_id}';
    """
    result = client.query(query).result()

    for row in result:
        return row.value

    return 0


def update_job_index(client: bigquery.Client, job_id: str, index: int) -> bool:
    query = f"""
    UPDATE `{PROJECT_ID}.{VINTED_DATASET_ID}.{INDEX_TABLE_ID}`
    SET value = {index}
    WHERE job_id = '{job_id}'
    """
    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def query_items(
    only_top_brands: bool = False,
    sort_by_date: bool = False,
    sort_by_likes: bool = False,
    item_ids: Optional[List[str]] = None,
    n: Optional[int] = None,
    index: Optional[int] = None,
) -> str:
    order_by_prefix = " ORDER BY"
    where_prefix = "\nWHERE"

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}`
    """

    if item_ids: 
        item_ids_str = ", ".join(f"'{item_id}'" for item_id in item_ids)
        query += f"{where_prefix} id IN ({item_ids_str})"
        where_prefix = " AND"

    if only_top_brands:
        top_brands_str = ", ".join(f'"{brand}"' for brand in TOP_BRANDS)
        query += f"{where_prefix} brand IN ({top_brands_str})"

    if sort_by_date:
        query += f"\nORDER BY created_at"
        order_by_prefix = " AND"

    if sort_by_likes:
        query += f" {order_by_prefix} num_likes DESC"

    if n and index:
        query += f"\nLIMIT {n} OFFSET {index * n}"

    return query


def query_pinecone_points(item_ids: List[int]) -> str:
    item_ids_str = ", ".join([f"'{item_id}'" for item_id in item_ids])

    return f"""
    SELECT point_id 
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` 
    WHERE item_id IN ({item_ids_str})
    """


def query_user_interactions(shuffle: bool = False) -> str:
    query = f"""
    SELECT DISTINCT p.point_id
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}` AS i
    LEFT JOIN (
    SELECT item_id FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{CLICK_OUT_TABLE_ID}`
    UNION ALL
    SELECT item_id FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{SAVED_TABLE_ID}`
    ) AS interactions ON i.id = interactions.item_id
    INNER JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS p ON i.id = p.item_id
    LEFT JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{SOLD_TABLE_ID}` AS s USING (vinted_id)
    WHERE interactions.item_id IS NOT NULL AND s.vinted_id IS NULL
    """

    if shuffle:
        query += "\nORDER BY RAND()"

    return query


def create_batches(loader: bigquery.table.RowIterator, batch_size: int) -> List[List[bigquery.table.Row]]:
    batches, current_batch = [], []

    for row in loader:
        current_batch.append(row)

        if len(current_batch) == batch_size:
            batches.append(current_batch)
            current_batch = []

    return batches