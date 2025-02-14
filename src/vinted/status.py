from .client import Vinted


DEFAULT_STATUS = True


def check_is_available(client: Vinted, item_id: int) -> bool:
    response = client.item_info(item_id)

    if response.status_code == 404:
        return False

    elif response.status_code == 200 and response.data:
        item_info = response.data.get("item")
        if not item_info:
            return False

        is_available = item_info.get("can_be_sold")
        if is_available is not None:
            return is_available

        is_closed = item_info.get("is_closed")
        if is_closed is not None:
            return not is_closed

    else:
        return
