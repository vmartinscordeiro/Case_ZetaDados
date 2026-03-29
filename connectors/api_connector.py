"""
REST API connector — supports single requests and cursor/page-based pagination.

Usage examples
--------------
# Simple endpoint
connector = APIConnector(
    url="https://api.example.com/orders",
    headers={"Authorization": "Bearer <token>"},
    data_key="results",
)

# Paginated endpoint
connector = APIConnector(
    url="https://api.example.com/orders",
    paginate=True,
    page_param="page",
    max_pages=50,
    data_key="data",
)

df = connector.extract()
"""

from typing import Any
import requests
import pandas as pd

from connectors.base_connector import BaseConnector


class APIConnector(BaseConnector):
    """Fetches data from a REST API and returns a DataFrame.

    Parameters
    ----------
    url:
        Full endpoint URL.
    headers:
        HTTP headers (e.g. authentication tokens).
    params:
        Query-string parameters sent with every request.
    data_key:
        If the API wraps its list inside a JSON key (e.g. ``{"data": [...]}``),
        set this to that key name.  If ``None`` the root JSON is used.
    paginate:
        When ``True`` the connector increments ``page_param`` until an empty
        page is returned or ``max_pages`` is reached.
    page_param:
        Name of the query-string parameter used for pagination (default "page").
    max_pages:
        Safety cap on the number of pages fetched (default 100).
    timeout:
        Seconds before a request times out (default 30).
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        data_key: str | None = None,
        paginate: bool = False,
        page_param: str = "page",
        max_pages: int = 100,
        timeout: int = 30,
    ):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.data_key = data_key
        self.paginate = paginate
        self.page_param = page_param
        self.max_pages = max_pages
        self.timeout = timeout

    def extract(self, **kwargs) -> pd.DataFrame:
        records = self._fetch_paginated() if self.paginate else self._fetch_page(self.params)
        return pd.DataFrame(records)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _fetch_page(self, params: dict) -> list[dict]:
        response = requests.get(
            self.url, headers=self.headers, params=params, timeout=self.timeout
        )
        response.raise_for_status()
        data = response.json()

        if self.data_key:
            return data[self.data_key]
        if isinstance(data, list):
            return data
        return [data]

    def _fetch_paginated(self) -> list[dict]:
        all_records: list[dict] = []
        for page in range(1, self.max_pages + 1):
            params = {**self.params, self.page_param: page}
            records = self._fetch_page(params)
            if not records:
                break
            all_records.extend(records)
        return all_records
