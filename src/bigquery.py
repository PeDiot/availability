from typing import List, Dict, Any, Union, Optional
from dataclasses import dataclass

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


BASE_QUERY = f"""
(
SELECT *
FROM `{PROJECT_ID}.{DATASET_ID}.{ITEM_TABLE_ID}`
WHERE vinted_id NOT IN (SELECT vinted_id FROM `{PROJECT_ID}.{DATASET_ID}.{SOLD_TABLE_ID}`)
) AS tmp
"""

LIKES_QUERY = f"""
(
SELECT item.*, likes.count AS likes_count
FROM `{PROJECT_ID}.{DATASET_ID}.{ITEM_TABLE_ID}` item
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{LIKES_TABLE_ID}` likes USING (vinted_id)
WHERE vinted_id NOT IN (SELECT vinted_id FROM `{PROJECT_ID}.{DATASET_ID}.{SOLD_TABLE_ID}`)
) AS tmp
"""

DEFAULT_ORDER_BY = "RAND()"


@dataclass
class OrderBy:
    field: Optional[str] = None
    ascending: Optional[bool] = None

    def __post_init__(self):
        self.direction = "ASC" if self.ascending else "DESC"

    def __str__(self):
        if self.field:
            return f"{self.field} {self.direction}"
        else:
            return DEFAULT_ORDER_BY


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def load_table(
    client: bigquery.Client,
    table: str,
    conditions: List[str] = None,
    fields: List[str] = None,
    order_by: List[OrderBy] = None,
    limit: int = None,
    to_list: bool = True,
) -> Union[List[Dict], bigquery.table.RowIterator]:
    field_str = ", ".join(fields) if fields else "*"
    query = f"SELECT {field_str} FROM {table}"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if order_by:
        order_clauses = [str(element) for element in order_by]
        query += f" ORDER BY {', '.join(order_clauses)}"

    if limit:
        query += f" LIMIT {limit}"

    query_job = client.query(query)
    results = query_job.result()

    if to_list:
        return [dict(row) for row in results]
    else:
        return results


def update_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    fields: List[str],
    new_values: List[Any],
    conditions: List[str] = None,
) -> bool:
    query = ""

    for field, value in zip(fields, new_values):
        query += f"""
        UPDATE `{PROJECT_ID}.{dataset_id}.{table_id}`
        SET {field} = {value}
        WHERE {' AND '.join(conditions)}; 
        """

    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def delete_from_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    conditions: List[str] = None,
) -> bool:
    query = f"""
    DELETE FROM `{PROJECT_ID}.{dataset_id}.{table_id}`
    """

    if not conditions:
        query += " WHERE TRUE"
    else:
        query += f" WHERE {' AND '.join(conditions)}"

    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def get_top_brands(client: bigquery.Client, top_n: int) -> List[str]:
    invalid_brands_str = ", ".join(f"'{brand}'" for brand in INVALID_BRANDS)

    query = f"""
    (
    SELECT brand, COUNT(*) AS n
    FROM `{PROJECT_ID}.{DATASET_ID}.{ITEM_TABLE_ID}`
    WHERE brand NOT IN ({invalid_brands_str})
    GROUP BY brand
    ORDER BY n DESC
    LIMIT {top_n}
    ) AS tmp
    """

    loader = load_table(client=client, table=query, fields=["brand"], to_list=False)
    return [row.brand for row in loader]
