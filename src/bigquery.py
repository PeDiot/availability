from typing import List, Dict, Union, Optional

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


def init_bigquery_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        "\\n", "\n"
    )

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
    only_vintage_dressing: bool = False,
    sort_by_date: bool = False,
    sort_by_likes: bool = False,
    item_ids: Optional[List[str]] = None,
    n: Optional[int] = None,
    index: Optional[int] = None,
) -> str:
    if only_vintage_dressing and only_top_brands:
        raise ValueError(
            "Cannot set both only_vintage_dressing and only_top_brands to True"
        )

    if sort_by_date and sort_by_likes:
        raise ValueError("Cannot set both sort_by_date and sort_by_likes to True")

    order_by_prefix = " ORDER BY"
    where_prefix = "\nAND"

    query = f"""
    SELECT i.id, p.point_id, i.vinted_id, i.url
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}` i
    INNER JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS p ON i.id = p.item_id
    LEFT JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{SOLD_TABLE_ID}` AS s USING (vinted_id)
    WHERE s.vinted_id IS NULL
    """

    if item_ids:
        item_ids_str = ", ".join(f"'{item_id}'" for item_id in item_ids)
        query += f"{where_prefix} id IN ({item_ids_str})"
        where_prefix = " AND"

    if only_top_brands:
        top_brands_str = ", ".join(f'"{brand}"' for brand in TOP_BRANDS)
        query += f"{where_prefix} brand IN ({top_brands_str})"

    if only_vintage_dressing:
        query += f"{where_prefix} brand = '{VINTAGE_DRESSING_BRAND}'"

    if sort_by_date:
        query += f"\nORDER BY created_at DESC"
        order_by_prefix = " AND"

    if sort_by_likes:
        query += f" {order_by_prefix} num_likes DESC"

    if n and index is not None:
        query += f"\nLIMIT {n} OFFSET {index * n}"

    return query


def query_interaction_items(
    n: Optional[int] = None, index: Optional[int] = None, shuffle: bool = False
) -> str:
    query = f"""
    SELECT DISTINCT p.point_id
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}` AS i
    LEFT JOIN (
    SELECT DISTINCT item_id FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{CLICK_OUT_TABLE_ID}`
    UNION ALL
    SELECT DISTINCT item_id FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{SAVED_TABLE_ID}`
    ) AS interactions ON i.id = interactions.item_id
    INNER JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS p ON i.id = p.item_id
    LEFT JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{SOLD_TABLE_ID}` AS s USING (vinted_id)
    WHERE s.vinted_id IS NULL AND interactions.item_id IS NOT NULL
    """

    if shuffle:
        query += "\nORDER BY RAND()"

    if n:
        query += f"\nLIMIT {n}"

        if index:
            query += f"\nOFFSET {index * n}"

    return query


def query_pinecone_points(item_ids: List[int]) -> str:
    item_ids_str = ", ".join([f"'{item_id}'" for item_id in item_ids])

    return f"""
    SELECT point_id 
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` 
    WHERE item_id IN ({item_ids_str})
    """


def query_points_to_delete(lookback_days: int) -> str:
    return f"""
    SELECT DISTINCT p.point_id
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS p
    INNER JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}` AS i ON p.item_id = i.id
    WHERE DATE(i.created_at) < DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY);
    """


def query_delete_points(lookback_days: int) -> bool:
    return f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS
    SELECT p.*
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` p
    INNER JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_TABLE_ID}` i ON p.item_id = i.id
    WHERE DATE(i.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
    """


def query_delete_items(lookback_days: int) -> bool:
    return f"""
    DELETE FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_TABLE_ID}`
    WHERE DATE(created_at) < DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY);
    """


def query_delete_sold(lookback_days: int) -> bool:
    return f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{VINTED_DATASET_ID}.{SOLD_TABLE_ID}` AS
    SELECT *
    FROM `{PROJECT_ID}.{VINTED_DATASET_ID}.{SOLD_TABLE_ID}`
    WHERE DATE(updated_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY);
    """