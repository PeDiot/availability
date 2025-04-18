from typing import Callable, Any

import time, requests
from bs4 import BeautifulSoup

from .enums import *
from .models import ItemStatus


def retry_with_backoff(func: Callable, *args, **kwargs) -> Any:
    sleep_time = INITIAL_SLEEP_TIME
    retries = 0

    while retries < MAX_RETRIES:
        try:
            result = func(*args, **kwargs)

            if isinstance(result, tuple) and len(result) == 2:
                status_code = result[1]

                if status_code in INVALID_STATUS_CODES:
                    time.sleep(sleep_time)
                    sleep_time = min(sleep_time * 2, MAX_SLEEP_TIME)
                    retries += 1
                    continue

                return result[0]

            return result

        except requests.exceptions.HTTPError as e:
            if e.response.status_code in INVALID_STATUS_CODES:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, MAX_SLEEP_TIME)
                retries += 1
                continue

            raise

        except:
            if retries < MAX_RETRIES - 1:
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, MAX_SLEEP_TIME)
                retries += 1
            else:
                return None

    return None


def parse_web_content(raw_content: str) -> ItemStatus:
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
