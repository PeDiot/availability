from typing import Optional

import requests, time
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.webdriver import WebDriver

from .vinted.client import Vinted
from .vinted.status import check_is_available
from .enums import *
from .models import ItemStatus


SLEEP_TIME = 10


def is_available(item_status: ItemStatus) -> bool | None:
    if item_status == ItemStatus.AVAILABLE:
        return True
    else:
        return False


def get_item_status_from_web(
    item_url: str, driver: Optional[WebDriver] = None
) -> ItemStatus:
    if driver:
        return _get_status_using_selenium(driver, item_url)
    return _get_status_using_requests(item_url)


def get_item_status_from_api(client: Vinted, item_id: int) -> ItemStatus:
    try:
        is_available, status_code = check_is_available(client, item_id)

        if status_code == 429:
            time.sleep(SLEEP_TIME)

        if is_available is None:
            return ItemStatus.UNKNOWN

        return ItemStatus.AVAILABLE if is_available else ItemStatus.SOLD

    except Exception as e:
        return ItemStatus.UNKNOWN


def _get_status_using_selenium(driver: WebDriver, item_url: str) -> ItemStatus:
    try:
        driver.get(item_url)
        return _get_item_status(driver.page_source)

    except Exception:
        return ItemStatus.UNKNOWN


def _get_status_using_requests(item_url: str) -> ItemStatus:
    response = requests.get(item_url, headers=REQUESTS_HEADERS)
    status_code = response.status_code

    if status_code == 429:
        time.sleep(SLEEP_TIME)
        response = requests.get(item_url, headers=REQUESTS_HEADERS)

    if status_code == 404:
        return ItemStatus.NOT_FOUND

    if response.url != item_url:
        return ItemStatus.NOT_FOUND

    try:
        return _get_item_status(response.content)
    except Exception:
        return ItemStatus.UNKNOWN


def _get_item_status(raw_content: str) -> ItemStatus:
    try:
        soup = BeautifulSoup(raw_content, BS4_PARSER)

        if _extract_sold_component(soup):
            return ItemStatus.SOLD

        if _extract_not_found_component(soup):
            return ItemStatus.NOT_FOUND

        return ItemStatus.AVAILABLE

    except Exception as e:
        return ItemStatus.UNKNOWN


def _extract_not_found_component(soup: BeautifulSoup) -> bool:
    try:
        heading = soup.find("h1", class_=NOT_FOUND_CONTAINER_CLASS)
        return bool(heading and heading.text.strip() == NOT_FOUND_STATUS_CONTENT)
    except Exception:
        return False


def _extract_sold_component(soup: BeautifulSoup) -> bool:
    try:
        status_element = soup.find(name="div", attrs=SOLD_CONTAINER_ATTRS)

        if status_element:
            status_text = status_element.text.strip()

            return status_text == SOLD_STATUS_CONTENT

        return False
    except Exception:
        return False
