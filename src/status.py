from enum import Enum

import requests
from bs4 import BeautifulSoup
from vinted.vinted import Vinted

from .enums import *


class ItemStatus(Enum):
    AVAILABLE = "available"
    SOLD = "sold"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


def is_available(client: Vinted, item_id: int, item_url: str) -> bool | None:
    item_status = _get_item_status_from_api(client, item_id)

    if item_status == ItemStatus.UNKNOWN:
        item_status = _get_item_status_from_web(item_url)

        if item_status == ItemStatus.AVAILABLE:
            return True
        elif item_status in [ItemStatus.SOLD, ItemStatus.NOT_FOUND]:
            return False
        else:
            return


def _get_item_status_from_api(client: Vinted, item_id: int) -> ItemStatus:
    try:
        info = client.item_info(item_id)
        return ItemStatus.AVAILABLE if info.item.can_be_sold else ItemStatus.SOLD

    except Exception as e:
        return ItemStatus.UNKNOWN


def _get_item_status_from_web(item_url: str) -> ItemStatus:
    response = requests.get(item_url, headers=REQUESTS_HEADERS)

    if response.status_code == 404:
        return ItemStatus.NOT_FOUND

    if response.url != item_url:
        return ItemStatus.NOT_FOUND

    try:
        soup = BeautifulSoup(response.content, BS4_PARSER)

        return _get_item_status(soup)

    except Exception as e:
        return ItemStatus.UNKNOWN


def _get_item_status(soup: BeautifulSoup) -> ItemStatus:
    try:
        status_element = soup.find(name="div", attrs=SOLD_CONTAINER_ATTRS)

        if status_element:
            status_text = status_element.text.strip()

            if status_text == SOLD_STATUS_CONTENT:
                return ItemStatus.SOLD

            return ItemStatus.AVAILABLE

        return ItemStatus.AVAILABLE

    except Exception as e:
        return ItemStatus.UNKNOWN
