"""Shopify connector - indexes the store's products and policies (read-only).

Credentials live in ``~/.openjarvis/connectors/shopify.json`` and are set with
``jarvis connect shopify``. Two ways to authenticate are supported:

* **Dev Dashboard app** (every app created since 1 Jan 2026): store domain +
  Client ID + Client secret. A short-lived Admin API token is obtained with the
  client-credentials grant and refreshed automatically.
* **Legacy custom app** (created in the admin before 2026): store domain + the
  ``shpat_...`` Admin API access token.

Required Admin API scopes: ``read_products`` and ``read_legal_policies``.
``read_inventory`` is optional (adds stock levels). The connector never writes
to the store.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

import httpx

from openjarvis.connectors._stubs import BaseConnector, Document, SyncStatus
from openjarvis.core.config import DEFAULT_CONFIG_DIR
from openjarvis.core.registry import ConnectorRegistry

_DEFAULT_PATH = str(DEFAULT_CONFIG_DIR / "connectors" / "shopify.json")
_API_VERSION = "2026-07"
_SHOP_RE = re.compile(r"^[a-z0-9][a-z0-9-]*\.myshopify\.com$")

_SHOP_QUERY = """
{ shop { name email currencyCode primaryDomain { url }
  shopPolicies { title type body url } } }
"""

_PRODUCTS_QUERY = """
query($cursor: String) {
  products(first: 50, after: $cursor, query: "status:active") {
    pageInfo { hasNextPage endCursor }
    nodes {
      id title handle description productType vendor tags onlineStoreUrl updatedAt
      %(inventory_total)s
      priceRangeV2 {
        minVariantPrice { amount currencyCode }
        maxVariantPrice { amount currencyCode }
      }
      variants(first: 50) {
        nodes { title price sku availableForSale %(inventory_variant)s }
      }
    }
  }
}
"""


def normalize_shop(value: str) -> str:
    """Turn 'minhaloja', 'https://minhaloja.myshopify.com/admin' etc. into a domain."""
    v = value.strip().lower()
    v = re.sub(r"^https?://", "", v).split("/")[0]
    if v and "." not in v:
        v = f"{v}.myshopify.com"
    if not _SHOP_RE.match(v):
        raise ValueError(
            "Use the store's .myshopify.com domain (e.g. minhaloja.myshopify.com), "
            "not the custom domain."
        )
    return v


def _strip_html(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", html or "")
    text = re.sub(r"(?i)<br\s*/?>|</p>|</li>|</h\d>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    return re.sub(r"\n\s*\n+", "\n\n", text).strip()


def _parse_ts(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(tz=timezone.utc)


@ConnectorRegistry.register("shopify")
class ShopifyConnector(BaseConnector):
    """Read-only Shopify Admin API connector (products + store policies)."""

    connector_id = "shopify"
    display_name = "Shopify"
    auth_type = "token"

    def __init__(self, *, config_path: str = _DEFAULT_PATH) -> None:
        self._path = Path(config_path)
        self._status = SyncStatus()

    # -- credentials ---------------------------------------------------------

    def _load(self) -> Dict[str, Any]:
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save(self, data: Dict[str, Any]) -> None:
        from openjarvis.security.file_utils import secure_write_json

        secure_write_json(self._path, data)

    def _fetch_client_token(self, shop: str, client_id: str, secret: str) -> Dict[str, Any]:
        resp = httpx.post(
            f"https://{shop}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": secret,
            },
            headers={"Accept": "application/json"},
            timeout=30.0,
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"Shopify refused the client credentials (HTTP {resp.status_code}). "
                "Check the Client ID/secret and that the app is installed on this store."
            )
        body = resp.json()
        if not body.get("access_token"):
            raise RuntimeError("Shopify did not return an access token.")
        return body

    def _access_token(self) -> str:
        data = self._load()
        if data.get("token"):
            return str(data["token"])
        if not (data.get("client_id") and data.get("client_secret")):
            raise RuntimeError("Shopify is not configured. Run: jarvis connect shopify")
        cached = data.get("_access_token")
        if cached and float(data.get("_expires_at", 0)) > time.time() + 300:
            return str(cached)
        body = self._fetch_client_token(data["shop"], data["client_id"], data["client_secret"])
        data["_access_token"] = body["access_token"]
        data["_expires_at"] = time.time() + float(body.get("expires_in", 86399))
        self._save(data)
        return str(body["access_token"])

    def _graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        shop = self._load().get("shop", "")
        resp = httpx.post(
            f"https://{shop}/admin/api/{_API_VERSION}/graphql.json",
            json={"query": query, "variables": variables or {}},
            headers={"X-Shopify-Access-Token": self._access_token()},
            timeout=60.0,
        )
        if resp.status_code in (401, 403):
            raise RuntimeError(f"Shopify denied access (HTTP {resp.status_code}). Check the app's scopes.")
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"Shopify GraphQL error: {json.dumps(body['errors'])[:400]}")
        return body["data"]

    def set_credentials(self, *, shop: str, token: str = "", client_id: str = "",
                        client_secret: str = "") -> str:
        """Validate and persist credentials. Returns the store name."""
        data: Dict[str, Any] = {"shop": normalize_shop(shop)}
        if token.strip():
            data["token"] = token.strip()
        elif client_id.strip() and client_secret.strip():
            data["client_id"] = client_id.strip()
            data["client_secret"] = client_secret.strip()
        else:
            raise ValueError("Provide an Admin API token, or a Client ID and Client secret.")
        previous = self._load()
        self._save(data)
        try:
            name = self._graphql("{ shop { name } }")["shop"]["name"]
        except Exception:
            if previous:
                self._save(previous)
            else:
                self._path.unlink(missing_ok=True)
            raise
        return str(name)

    def cli_setup(self, prompt: Callable[..., Any], echo: Callable[[str], None]) -> str:
        """Interactive setup used by ``jarvis connect shopify``."""
        echo("Shopify: use the store's .myshopify.com domain.")
        shop = prompt("Store domain (ex.: minhaloja.myshopify.com)")
        echo("Paste the Client ID and Client secret of your Dev Dashboard app.")
        echo("Legacy app with an shpat_ token? Leave Client ID empty.")
        client_id = prompt("Client ID", default="", show_default=False)
        if client_id.strip():
            secret = prompt("Client secret", hide_input=True)
            return self.set_credentials(shop=shop, client_id=client_id, client_secret=secret)
        token = prompt("Admin API access token (shpat_...)", hide_input=True)
        return self.set_credentials(shop=shop, token=token)

    # -- BaseConnector -------------------------------------------------------

    def is_connected(self) -> bool:
        data = self._load()
        return bool(data.get("shop") and (data.get("token") or data.get("client_id")))

    def disconnect(self) -> None:
        self._path.unlink(missing_ok=True)

    def sync_status(self) -> SyncStatus:
        return self._status

    def _products(self) -> Iterator[Dict[str, Any]]:
        fields = {"inventory_total": "totalInventory", "inventory_variant": "inventoryQuantity"}
        cursor: Optional[str] = None
        while True:
            try:
                data = self._graphql(_PRODUCTS_QUERY % fields, {"cursor": cursor})
            except RuntimeError as exc:
                if fields["inventory_total"] and "ACCESS_DENIED" in str(exc).upper():
                    fields = {"inventory_total": "", "inventory_variant": ""}
                    continue
                raise
            page = data["products"]
            yield from page["nodes"]
            if not page["pageInfo"]["hasNextPage"]:
                return
            cursor = page["pageInfo"]["endCursor"]

    def sync(self, *, since: Optional[datetime] = None,
             cursor: Optional[str] = None) -> Iterator[Document]:  # noqa: ARG002
        self._status = SyncStatus(state="syncing")
        count = 0
        shop = self._graphql(_SHOP_QUERY)["shop"]
        store = shop.get("name", "Loja")
        site = (shop.get("primaryDomain") or {}).get("url", "")

        for pol in shop.get("shopPolicies") or []:
            body = _strip_html(pol.get("body", ""))
            if not body:
                continue
            count += 1
            yield Document(
                doc_id=f"shopify:policy:{pol.get('type', count)}",
                source="shopify",
                doc_type="policy",
                title=f"{store} - {pol.get('title', 'Politica')}",
                content=body,
                url=pol.get("url") or None,
                metadata={"store": store, "policy_type": pol.get("type")},
            )

        for p in self._products():
            if since and _parse_ts(p.get("updatedAt", "")) < since:
                continue
            pr = p.get("priceRangeV2") or {}
            lo, hi = pr.get("minVariantPrice") or {}, pr.get("maxVariantPrice") or {}
            preco = f"{lo.get('amount')} {lo.get('currencyCode', '')}".strip()
            if hi.get("amount") and hi.get("amount") != lo.get("amount"):
                preco += f" a {hi.get('amount')} {hi.get('currencyCode', '')}"
            linhas = [
                f"Produto: {p.get('title')}",
                f"Preco: {preco}",
                f"Tipo: {p.get('productType') or '-'} | Marca: {p.get('vendor') or '-'}",
            ]
            if p.get("tags"):
                linhas.append("Tags: " + ", ".join(p["tags"]))
            if p.get("totalInventory") is not None:
                linhas.append(f"Estoque total: {p['totalInventory']}")
            url = p.get("onlineStoreUrl") or (f"{site}/products/{p.get('handle')}" if site else "")
            if url:
                linhas.append(f"Link: {url}")
            variantes = (p.get("variants") or {}).get("nodes") or []
            if len(variantes) > 1 or (variantes and variantes[0].get("title") != "Default Title"):
                linhas.append("Variantes:")
                for v in variantes:
                    est = v.get("inventoryQuantity")
                    disp = "disponivel" if v.get("availableForSale") else "indisponivel"
                    extra = f", estoque {est}" if est is not None else ""
                    linhas.append(f"- {v.get('title')}: {v.get('price')} ({disp}{extra})")
            desc = (p.get("description") or "").strip()
            if desc:
                linhas += ["", desc]
            count += 1
            yield Document(
                doc_id=f"shopify:product:{str(p.get('id', '')).rsplit('/', 1)[-1]}",
                source="shopify",
                doc_type="product",
                title=str(p.get("title", "")),
                content="\n".join(linhas),
                timestamp=_parse_ts(p.get("updatedAt", "")),
                url=url or None,
                metadata={"store": store, "handle": p.get("handle")},
            )

        self._status = SyncStatus(state="idle", items_synced=count, items_total=count,
                                  last_sync=datetime.now(tz=timezone.utc))


__all__ = ["ShopifyConnector", "normalize_shop"]
