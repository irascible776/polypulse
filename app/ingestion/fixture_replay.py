from pathlib import Path
from typing import Any

from app.ingestion.identifiers import parse_json_with_decimal
from app.ingestion.source import EventSource, PageResult


class FixtureReplaySource(EventSource):
    """
    EventSource implementation that replays pre-recorded or synthetic fixture pages from disk.
    Feeds events through the exact same processing pipeline as live sources.
    """

    def __init__(
        self,
        fixtures_dir: Path | str = "fixtures",
        status_file: Path | str | None = None,
    ) -> None:
        self.fixtures_dir = Path(fixtures_dir)
        self.status_file = (
            Path(status_file) if status_file else self.fixtures_dir / "v2_status_freshness.json"
        )
        # Mapping from wallet -> list of loaded pages
        self._wallet_pages: dict[str, list[dict[str, Any]]] = {}

    @property
    def source_tag(self) -> str:
        return "fixture_replay"

    def register_pages(self, wallet: str, pages: list[dict[str, Any]]) -> None:
        """Manually register raw page dictionaries for a wallet (e.g. for testing)."""
        self._wallet_pages[wallet.lower()] = pages

    def _load_pages_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        w = wallet.lower()
        if w in self._wallet_pages:
            return self._wallet_pages[w]

        pages: list[dict[str, Any]] = []

        # 1. Prefer activity_{w}_page_*.json (canonical ASC) then label pattern
        label_patterns = [f"activity_{w}_page_*.json"]
        if "951634357346bb291df117f9ac410100feea2104" in w:
            label_patterns.append("appletiiime_page_*.json")
        elif "a98a9d23e7211c22d2259b68b1bc9d47b3013b92" in w:
            label_patterns.append("jnstprdctns_page_*.json")

        matched_files: list[Path] = []
        for pat in label_patterns:
            files = sorted(self.fixtures_dir.glob(pat))
            if files:
                matched_files = files
                break

        if matched_files:
            for p in matched_files:
                with open(p, "r", encoding="utf-8") as f:
                    doc = parse_json_with_decimal(f.read())
                    if isinstance(doc, dict) and "response" in doc:
                        resp_obj = doc["response"]
                        pages.append(resp_obj if isinstance(resp_obj, dict) else {"data": resp_obj})
                    else:
                        pages.append(doc)
            self._wallet_pages[w] = pages
            return pages

        # 2. Check for synthetic wallet fixture
        synthetic_path = self.fixtures_dir / "synthetic_wallet.json"
        if synthetic_path.exists():
            with open(synthetic_path, "r", encoding="utf-8") as f:
                raw_synthetic = parse_json_with_decimal(f.read())
            # If synthetic is a list of events, wrap into a page
            events = (
                raw_synthetic["data"]
                if isinstance(raw_synthetic, dict) and "data" in raw_synthetic
                else raw_synthetic
            )
            # Filter by wallet if events contain wallet field
            wallet_events = [
                e
                for e in events
                if str(e.get("proxy_wallet") or e.get("wallet") or "").lower() == w
                or not e.get("proxy_wallet")
            ]
            if wallet_events:
                pages.append({"data": wallet_events, "pagination": {"has_more": False}})
                self._wallet_pages[w] = pages
                return pages

        self._wallet_pages[w] = []
        return []

    async def fetch_page(
        self,
        wallet: str,
        cursor: str | None = None,
        start: int | None = None,
        limit: int = 500,
        sort_direction: str = "ASC",
    ) -> PageResult:
        pages = self._load_pages_for_wallet(wallet)
        if not pages:
            return PageResult(events=[], next_cursor=None, has_more=False)

        # Cursor format for fixture replay: 'page:<index>'
        page_idx = 0
        if cursor and cursor.startswith("page:"):
            try:
                page_idx = int(cursor.split(":")[1])
            except (ValueError, IndexError):
                page_idx = 0

        if page_idx >= len(pages):
            return PageResult(events=[], next_cursor=None, has_more=False)

        page_data = pages[page_idx]
        events = page_data.get("data", [])
        if isinstance(page_data, list):
            events = page_data

        has_more = page_idx + 1 < len(pages)
        next_cursor = f"page:{page_idx + 1}" if has_more else None

        return PageResult(
            events=events,
            next_cursor=next_cursor,
            has_more=has_more,
            status_code=200,
            raw_response=page_data if isinstance(page_data, dict) else {"data": page_data},
        )

    async def fetch_status(self) -> dict[str, Any] | None:
        if self.status_file.exists():
            with open(self.status_file, "r", encoding="utf-8") as f:
                return parse_json_with_decimal(f.read())
        return None
