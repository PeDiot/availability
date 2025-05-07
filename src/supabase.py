from typing import List, Optional

from supabase import create_client, Client
from .enums import SUPABASE_SAVED_TABLE_ID
from .models import PineconeEntry


def init_supabase_client(url: str, key: str) -> Client:
    return create_client(supabase_url=url, supabase_key=key)


def get_saved_items(
    client: Client,
    n: Optional[int] = None,
    index: Optional[int] = 0,
) -> List[PineconeEntry]:
    try:
        query = (
            client.table(SUPABASE_SAVED_TABLE_ID).select("*").eq("is_available", True)
        )

        if n is not None:
            start = int(index * n)
            end = int(start + n - 1)
            query = query.range(start, end)

        response = query.execute()
        entries = []

        for row in response.data:
            row_data = row["metadata"]
            entry = PineconeEntry.from_dict(row_data)
            entries.append(entry)

        return entries

    except Exception as e:
        print(e)
        return []


def set_items_unavailable(client: Client, item_ids: list[str]) -> bool:
    try:
        response = (
            client.table(SUPABASE_SAVED_TABLE_ID)
            .update({"is_available": False})
            .in_("item_id", item_ids)
            .execute()
        )

        return True

    except Exception as e:
        print(e)
        return False
