from typing import Literal
import random, requests

from .endpoints import Endpoints
from .enums import Domain, REQUESTS_HEADERS, USER_AGENTS
from .models import VintedResponse


class Vinted:
    def __init__(self, domain: Domain = "fr") -> None:
        self.base_url = f"https://www.vinted.{domain}"
        self.api_url = f"{self.base_url}/api/v2"
        self.headers = REQUESTS_HEADERS.copy()
        self.session = requests.Session()
        self.cookies = self.fetch_cookies()

    def fetch_cookies(self):
        response = self.session.get(self.base_url, headers=self.headers)
        return response.cookies

    def _call(self, method: Literal["get"], *args, **kwargs):
        if hasattr(self, "headers") and "User-Agent" in self.headers:
            self.headers["User-Agent"] = random.choice(USER_AGENTS)

        return self.session.request(
            method=method, headers=self.headers, cookies=self.cookies, *args, **kwargs
        )

    def _get(
        self, endpoint: Endpoints, format_values=None, *args, **kwargs
    ) -> VintedResponse:
        if format_values:
            url = self.api_url + endpoint.value.format(format_values)
        else:
            url = self.api_url + endpoint.value

        response = self._call(method="get", url=url, *args, **kwargs)
        status_code = response.status_code

        if status_code == 200:
            try:
                data = response.json()
                return VintedResponse(status_code, data)
            except requests.exceptions.JSONDecodeError:
                return VintedResponse(status_code)

        return VintedResponse(status_code=status_code)

    def item_info(self, item_id: int) -> VintedResponse:
        try:
            return self._get(Endpoints.ITEMS, item_id)
        except Exception as e:
            print(e)
            return VintedResponse(status_code=500)
