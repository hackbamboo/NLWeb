import time
import json
from typing import List, Dict, Any, Optional, Union
from urllib.parse import urlparse

import httpx

from core.config import CONFIG
from core.retriever import RetrievalClientBase
from misc.logger.logging_config_helper import get_configured_logger

logger = get_configured_logger("google_custom_search_client")


class GoogleCustomSearchClient(RetrievalClientBase):
    """
    Simple client for Google Custom Search JSON API (CSE).
    See: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
    """

    def __init__(self, endpoint_name: Optional[str] = None):
        super().__init__()
        self.endpoint_name = endpoint_name or "google_custom_search"
        self.endpoint_config = CONFIG.retrieval_endpoints.get(self.endpoint_name)
        if not self.endpoint_config:
            raise ValueError(f"No configuration found for endpoint {self.endpoint_name}")

        # API key (plain string)
        self.api_key = getattr(self.endpoint_config, "api_key", None)
        # Custom Search Engine id (cx). Try several common field names.
        self.cx = getattr(self.endpoint_config, "cx", None) or getattr(self.endpoint_config, "custom_search_engine_id", None) or getattr(self.endpoint_config, "index_name", None)

        # API endpoint (default to the official CSE endpoint)
        self.api_endpoint = getattr(self.endpoint_config, "api_endpoint", None) or "https://customsearch.googleapis.com/customsearch/v1"

        if not self.api_key:
            raise ValueError(f"API key not configured for {self.endpoint_name}")
        if not self.cx:
            raise ValueError(f"Custom Search Engine id (cx) not configured for {self.endpoint_name}")

        logger.info(f"Initialized GoogleCustomSearchClient for endpoint: {self.endpoint_name} apikey={self.api_key} cx={self.cx}")

    def _extract_domain_from_url(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc or "unknown"
            if domain.startswith("www."):
                domain = domain[4:]
            return domain
        except Exception:
            return "unknown"

    async def search(self, query: str, site: Union[str, List[str]],
                     num_results: int = 10, **kwargs) -> List[List[str]]:
        """
        Perform a Google CSE search. Note: Google CSE returns up to 10 results per request.
        """
        # If site filter provided, prefer using siteSearch param (supported by CSE)
        params = {
            "key": self.api_key,
            "cx": self.cx,
            "q": query,
            "num": max(1, min(num_results, 10))
        }

        logger.info(f"Google CSE search: query='{query}', site='{site}', num_results={num_results}")

        if site and isinstance(site, str) and site != "all":
            # Use siteSearch to restrict to a domain/host
            params["q"] = f"{params.get('q', '')} site:{site}"

        # Allow callers to override params via kwargs.query_params
        query_params = kwargs.get("query_params", {})
        if isinstance(query_params, dict):
            params.update(query_params)

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(self.api_endpoint, params=params, timeout=30.0)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling Google CSE: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error calling Google CSE: {e}")
            return []

        items = data.get("items", [])
        results: List[List[str]] = []
        print(f"[DEBUG CSE] Google CSE returned {len(items)} items")
        for it in items[:num_results]:
            link = it.get("link", "")
            title = it.get("title", "")
            snippet = it.get("snippet", "")
            site_domain = self._extract_domain_from_url(link)

            schema_obj = {
                "@type": "WebPage",
                "name": title,
                "description": snippet,
                "url": link,
                "displayLink": it.get("displayLink")
            }

            json_str = json.dumps(schema_obj)
            results.append([link, json_str, title, site_domain])
            print(f"[DEBUG CSE]Found result: {link} (title: {title})")

        return results

    async def search_all_sites(self, query: str, num_results: int = 10, **kwargs) -> List[List[str]]:
        # Google CSE: just don't pass site filter
        return await self.search(query, "all", num_results, **kwargs)

    async def search_by_url(self, url: str, **kwargs) -> Optional[List[str]]:
        """
        Try to find an exact URL by searching for it. Google CSE does not support exact id lookup,
        so search for the URL and compare returned links.
        """
        results = await self.search(url, "all", num_results=1, **kwargs)
        if not results:
            return None
        # If first result exact matches URL, return it
        if results[0][0].rstrip("/") == url.rstrip("/"):
            return results[0]
        return None

    async def get_sites(self, **kwargs) -> Optional[List[str]]:
        # Not applicable for web search provider
        return None

    async def upload_documents(self, documents: List[Dict[str, Any]], **kwargs) -> int:
        raise NotImplementedError("GoogleCustomSearchClient is a read-only provider - upload is not supported")

    async def delete_documents_by_site(self, site: str, **kwargs) -> int:
        raise NotImplementedError("GoogleCustomSearchClient is a read-only provider - delete is not supported")