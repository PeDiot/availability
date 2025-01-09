from typing import List, Dict, Any, Union

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


def load_table(
    client: bigquery.Client,
    table_id: str,
    dataset_id: str = DATASET_ID,
    conditions: List[str] = None,
    fields: List[str] = None,
    order_by: str = None,
    descending: bool = False,
    limit: int = None,
    to_list: bool = True,
) -> Union[List[Dict], bigquery.table.RowIterator]:
    field_str = ", ".join(fields) if fields else "*"
    query = f"SELECT {field_str} FROM `{PROJECT_ID}.{dataset_id}.{table_id}`"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if order_by:
        query += f" ORDER BY {order_by} {'DESC' if descending else 'ASC'}"

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
