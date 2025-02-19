from typing import List, Dict, Union

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


def init_client(credentials_dict: Dict) -> bigquery.Client:
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
    MERGE `{PROJECT_ID}.{DATASET_ID}.{INDEX_TABLE_ID}` T
    USING (SELECT '{job_id}' as job_id) S
    ON T.job_id = S.job_id
    WHEN NOT MATCHED THEN
        INSERT (job_id, value) VALUES ('{job_id}', 0)
    WHEN MATCHED THEN
        UPDATE SET value = value;
    
    SELECT value
    FROM `{PROJECT_ID}.{DATASET_ID}.{INDEX_TABLE_ID}`
    WHERE job_id = '{job_id}';
    """
    result = client.query(query).result()

    for row in result:
        return row.value + 1

    return 0


def update_job_index(client: bigquery.Client, job_id: str, index: int) -> bool:
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{INDEX_TABLE_ID}`
    SET value = {index}
    WHERE job_id = '{job_id}'
    """
    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def query_active_items(
    n: int, 
    job_prefix: str, 
    index: int, 
    only_top_brands: bool, 
    sort_by_date: bool, 
    sort_by_likes: bool
) -> str:
    order_by_prefix = " ORDER BY"
    top_brands_str = ", ".join(f'"{brand}"' for brand in TOP_BRANDS)

    query = f"""
    SELECT * 
    FROM `{PROJECT_ID}.{DATASET_ID}.{ITEM_ACTIVE_TABLE_ID}`
    WHERE job_prefix = '{job_prefix}'
    """

    if only_top_brands:
        query += f" AND brand IN ({top_brands_str})"

    if sort_by_date:
        query += f"\nORDER BY created_at"
        order_by_prefix = " AND"
    
    if sort_by_likes:
        query += f" {order_by_prefix} num_likes DESC"

    query += f"\nLIMIT {n} OFFSET {index * n}"

    return query


def query_pinecone_points(item_ids: List[int]) -> str:
    item_ids_str = ", ".join([f"'{item_id}'" for item_id in item_ids])

    return f"""
    SELECT point_id 
    FROM `{PROJECT_ID}.{DATASET_ID}.{PINECONE_TABLE_ID}` 
    WHERE item_id IN ({item_ids_str})
    """