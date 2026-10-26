"""
Alara Dental Repricer Ingestion Pipeline
----------------------------------------
Production-grade data ingestion & repricer synchronization engine.
Supports both direct CLI execution and Render Web Service mode with HTTP endpoints.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Set

# Browser TLS & JA3/JA4 impersonation
from curl_cffi.requests import AsyncSession, Response

# ==============================================================================
# Logging Configuration
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AlaraIngestion")

# ==============================================================================
# Global State Tracker (Thread-safe for Web Service queries)
# ==============================================================================

class PipelineStateManager:
    def __init__(self):
        self._lock = Lock()
        self.state: Dict[str, Any] = {
            "status": "idle",              # "idle" | "running" | "completed" | "failed"
            "total_products": 0,
            "processed_count": 0,
            "progress_percent": 0.0,
            "started_at": None,
            "completed_at": None,
            "duration_seconds": None,
            "successful_batches": 0,
            "failed_batches": 0,
            "last_error": None,
        }

    def start(self, total: int):
        with self._lock:
            self.state["status"] = "running"
            self.state["total_products"] = total
            self.state["processed_count"] = 0
            self.state["progress_percent"] = 0.0
            self.state["started_at"] = datetime.now(timezone.utc).isoformat()
            self.state["completed_at"] = None
            self.state["duration_seconds"] = None
            self.state["successful_batches"] = 0
            self.state["failed_batches"] = 0
            self.state["last_error"] = None

    def update_progress(self, processed: int):
        with self._lock:
            self.state["processed_count"] = processed
            total = self.state["total_products"]
            if total > 0:
                self.state["progress_percent"] = round((processed / total) * 100, 2)

    def record_batch_success(self):
        with self._lock:
            self.state["successful_batches"] += 1

    def record_batch_failure(self):
        with self._lock:
            self.state["failed_batches"] += 1

    def complete(self, status: str = "completed"):
        with self._lock:
            self.state["status"] = status
            self.state["completed_at"] = datetime.now(timezone.utc).isoformat()
            if self.state["started_at"]:
                start = datetime.fromisoformat(self.state["started_at"])
                end = datetime.fromisoformat(self.state["completed_at"])
                self.state["duration_seconds"] = round((end - start).total_seconds(), 1)

    def fail(self, error_message: str):
        with self._lock:
            self.state["status"] = "failed"
            self.state["last_error"] = error_message
            self.state["completed_at"] = datetime.now(timezone.utc).isoformat()
            if self.state["started_at"]:
                start = datetime.fromisoformat(self.state["started_at"])
                end = datetime.fromisoformat(self.state["completed_at"])
                self.state["duration_seconds"] = round((end - start).total_seconds(), 1)

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self.state)


state_manager = PipelineStateManager()

# ==============================================================================
# Pipeline Configuration
# ==============================================================================

@dataclass
class PipelineConfig:
    # Authentication
    surgimac_api_key: Optional[str] = field(
        default_factory=lambda: os.getenv(
            "SURGIMAC_API_KEY",
            "ebf0fc6d3c552391f82072ab831e04bea2a6d3ef32a0eb588176a3e05cfc2cf9",
        )
    )
    surgimac_bearer_token: Optional[str] = field(
        default_factory=lambda: os.getenv("SURGIMAC_BEARER_TOKEN", None)
    )

    # API Endpoints
    surgimac_products_url: str = field(
        default_factory=lambda: os.getenv(
            "SURGIMAC_PRODUCTS_URL",
            "https://reporting.surgimacdental.com/api/zenone-dashboard/alara-repricer/products",
        )
    )
    surgimac_post_url: str = field(
        default_factory=lambda: os.getenv(
            "SURGIMAC_POST_URL",
            "https://reporting.surgimacdental.com/api/zenone-dashboard/alara-repricer/data",
        )
    )
    alara_product_base_url: str = field(
        default_factory=lambda: os.getenv(
            "ALARA_PRODUCT_BASE_URL",
            "https://alaradental.com/api/products",
        )
    )

    # Concurrency & Request Jitter
    max_concurrency: int = field(
        default_factory=lambda: int(os.getenv("MAX_CONCURRENCY", "4"))
    )
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "50"))
    )
    request_delay_min: float = 0.3
    request_delay_max: float = 0.7

    # Exponential Backoff Parameters
    max_retries: int = 5
    base_backoff: float = 1.0
    max_backoff: float = 30.0

    # Browser TLS Impersonation & Proxy
    impersonate_browser: str = "chrome124"
    proxy_url: Optional[str] = field(
        default_factory=lambda: os.getenv("PROXY_URL", None)
    )
    scraperapi_key: Optional[str] = field(
        default_factory=lambda: os.getenv(
            "SCRAPERAPI_KEY", "7db34b84c59354d8d3b57c7f25a23333"
        )
    )
    timeout_seconds: int = 60

    # Local Persistence
    checkpoint_file: Path = Path("alara_processed_ids.txt")
    output_backup_file: Path = Path("alara_ingested_data.jsonl")


def compute_full_jitter_delay(attempt: int, base: float, ceiling: float) -> float:
    """Computes sleep using Full Jitter: rand(0, min(ceiling, base * 2^attempt))."""
    temp = min(ceiling, base * (2 ** attempt))
    return random.uniform(0.0, temp)


# ==============================================================================
# Core Ingestion Engine
# ==============================================================================

class AlaraRepricerPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrency)
        self.processed_ids: Set[str] = self._load_checkpoint()
        self.lock = asyncio.Lock()

    def _load_checkpoint(self) -> Set[str]:
        if self.config.checkpoint_file.exists():
            with open(self.config.checkpoint_file, "r", encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        return set()

    def _save_checkpoint_id(self, alara_product_id: str) -> None:
        with open(self.config.checkpoint_file, "a", encoding="utf-8") as f:
            f.write(f"{alara_product_id}\n")

    def _append_local_backup(self, item: Dict[str, Any]) -> None:
        with open(self.config.output_backup_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(item) + "\n")

    def _get_auth_headers(self) -> Dict[str, str]:
        if self.config.surgimac_bearer_token:
            return {"Authorization": f"Bearer {self.config.surgimac_bearer_token}"}
        if self.config.surgimac_api_key:
            return {"x-api-key": self.config.surgimac_api_key}
        return {}

    async def _fetch_with_backoff(
        self, session: AsyncSession, url: str
    ) -> Optional[Dict[str, Any]]:
        for attempt in range(self.config.max_retries):
            try:
                await asyncio.sleep(
                    random.uniform(self.config.request_delay_min, self.config.request_delay_max)
                )
                fetch_url = url
                params = None
                if self.config.scraperapi_key:
                    fetch_url = "http://api.scraperapi.com"
                    params = {
                        "api_key": self.config.scraperapi_key,
                        "url": url,
                    }

                response: Response = await session.get(
                    fetch_url, params=params, timeout=self.config.timeout_seconds
                )

                if response.status_code == 200:
                    return response.json()

                if response.status_code in (429, 503):
                    backoff = compute_full_jitter_delay(
                        attempt, self.config.base_backoff, self.config.max_backoff
                    )
                    logger.warning(
                        f"Rate limited ({response.status_code}) on {url}. Retrying in {backoff:.2f}s."
                    )
                    await asyncio.sleep(backoff)
                    continue

                if response.status_code == 404:
                    logger.debug(f"Product not found (404) at {url}")
                    return None

                logger.warning(
                    f"Unexpected HTTP {response.status_code} on {url}: {response.text[:120]}"
                )

            except Exception as err:
                backoff = compute_full_jitter_delay(
                    attempt, self.config.base_backoff, self.config.max_backoff
                )
                logger.warning(f"Network error on {url}: {err}. Retrying in {backoff:.2f}s.")
                await asyncio.sleep(backoff)

        logger.error(f"Failed to fetch {url} after {self.config.max_retries} attempts.")
        return None

    async def fetch_product_catalog(self, session: AsyncSession) -> List[Dict[str, Any]]:
        """
        Retrieves catalog from Surgimac. Automatically tries alternative route if
        the primary path returns 404.
        """
        urls_to_try = [self.config.surgimac_products_url]
        if "zenone-dashboard" in self.config.surgimac_products_url:
            urls_to_try.append(
                self.config.surgimac_products_url.replace("zenone-dashboard", "zenone")
            )

        headers = self._get_auth_headers()
        params = {"onlyWithProductId": "1"}

        for url in urls_to_try:
            logger.info(f"Querying product catalog from: {url}")
            try:
                response = await session.get(
                    url, params=params, headers=headers, timeout=self.config.timeout_seconds
                )
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        if "data" in data and isinstance(data["data"], list):
                            return data["data"]
                        if "products" in data and isinstance(data["products"], list):
                            return data["products"]
                    return []

                if response.status_code == 404:
                    logger.warning(f"Route 404 on {url}, checking alternative...")
                    continue

                raise RuntimeError(
                    f"HTTP {response.status_code} from {url}: {response.text[:200]}"
                )
            except Exception as e:
                if url == urls_to_try[-1]:
                    raise e
                logger.warning(f"Failed on {url} ({e}), trying fallback...")

        raise RuntimeError("Could not retrieve catalog from any configured Surgimac route.")

    async def post_repricer_batch(
        self, session: AsyncSession, batch: List[Dict[str, Any]]
    ) -> bool:
        if not batch:
            return True

        payload = {"items": batch}
        headers = self._get_auth_headers()

        for attempt in range(self.config.max_retries):
            try:
                response = await session.post(
                    self.config.surgimac_post_url,
                    json=payload,
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                )

                if response.status_code in (200, 201):
                    logger.info(f"Committed batch of {len(batch)} items successfully.")
                    state_manager.record_batch_success()
                    return True

                if response.status_code in (429, 503):
                    backoff = compute_full_jitter_delay(
                        attempt, self.config.base_backoff, self.config.max_backoff
                    )
                    logger.warning(f"Rate limited while posting batch. Retrying in {backoff:.2f}s.")
                    await asyncio.sleep(backoff)
                    continue

                logger.error(
                    f"Batch POST rejected ({response.status_code}): {response.text[:200]}"
                )

            except Exception as err:
                backoff = compute_full_jitter_delay(
                    attempt, self.config.base_backoff, self.config.max_backoff
                )
                logger.warning(f"Error during batch POST: {err}. Retrying in {backoff:.2f}s.")
                await asyncio.sleep(backoff)

        state_manager.record_batch_failure()
        logger.error(f"Failed to post batch of {len(batch)} items.")
        return False

    async def _process_single_product(
        self, session: AsyncSession, product: Dict[str, Any], batch_queue: asyncio.Queue
    ) -> None:
        raw_id = product.get("alaraProductId") or ""
        # Sanitize ID: strip whitespace and any leading/trailing dots
        alara_id = str(raw_id).strip().strip(".")
        if not alara_id or alara_id in self.processed_ids:
            return

        async with self.semaphore:
            target_url = f"{self.config.alara_product_base_url}/{alara_id}"
            product_data = await self._fetch_with_backoff(session, target_url)

            if product_data is not None:
                item_payload = {"alaraProductId": alara_id, "data": product_data}
                self._append_local_backup(item_payload)
                self._save_checkpoint_id(alara_id)

                async with self.lock:
                    self.processed_ids.add(alara_id)

                await batch_queue.put(item_payload)

    async def _batch_dispatcher(
        self, session: AsyncSession, batch_queue: asyncio.Queue, total_expected: int
    ) -> None:
        current_batch: List[Dict[str, Any]] = []
        processed_count = 0

        while True:
            item = await batch_queue.get()
            if item is None:
                batch_queue.task_done()
                break

            current_batch.append(item)
            processed_count += 1
            state_manager.update_progress(processed_count)

            if len(current_batch) >= self.config.batch_size:
                await self.post_repricer_batch(session, current_batch)
                current_batch = []
                logger.info(f"Progress: {processed_count}/{total_expected} products synced.")

            batch_queue.task_done()

        if current_batch:
            await self.post_repricer_batch(session, current_batch)
            logger.info(f"Final batch flush: {len(current_batch)} items posted.")

    async def run(self) -> None:
        proxies = (
            {"http": self.config.proxy_url, "https": self.config.proxy_url}
            if self.config.proxy_url
            else None
        )

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        try:
            async with AsyncSession(
                impersonate=self.config.impersonate_browser,
                headers=headers,
                proxies=proxies,
                verify=True,
            ) as session:
                logger.info("Initializing product catalog query...")
                products = await self.fetch_product_catalog(session)
                logger.info(f"Received {len(products)} products from Surgimac catalog.")

                pending_products = [
                    p for p in products
                    if p.get("alaraProductId") and p.get("alaraProductId") not in self.processed_ids
                ]

                logger.info(
                    f"Pending products to fetch: {len(pending_products)} "
                    f"(Already checkpointed: {len(self.processed_ids)})."
                )

                if not pending_products:
                    logger.info("All products are already synchronized. Pipeline complete.")
                    state_manager.start(len(products))
                    state_manager.update_progress(len(products))
                    state_manager.complete("completed (all up-to-date)")
                    return

                state_manager.start(len(pending_products))

                batch_queue: asyncio.Queue = asyncio.Queue()
                dispatcher_task = asyncio.create_task(
                    self._batch_dispatcher(session, batch_queue, len(pending_products))
                )

                tasks = [
                    asyncio.create_task(
                        self._process_single_product(session, p, batch_queue)
                    )
                    for p in pending_products
                ]

                await asyncio.gather(*tasks)

                await batch_queue.put(None)
                await dispatcher_task

            state_manager.complete("completed successfully")
            logger.info("Pipeline execution completed successfully.")

        except Exception as err:
            logger.error(f"Fatal error in pipeline execution: {err}", exc_info=True)
            state_manager.fail(str(err))


# ==============================================================================
# Built-in Lightweight HTTP Server for Render
# ==============================================================================

class RenderRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, data: Dict[str, Any]):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))

    def do_GET(self):
        if self.path in ("/", "/healthz", "/status"):
            snapshot = state_manager.get_snapshot()
            self._send_json(200, {
                "service": "Alara Repricer Sync Engine",
                "state": snapshot,
                "endpoints": {
                    "status": "GET /status",
                    "trigger_sync": "GET /sync",
                }
            })
        elif self.path.startswith("/sync"):
            current_state = state_manager.get_snapshot()
            if current_state["status"] == "running":
                self._send_json(409, {
                    "message": "Sync is already actively in progress",
                    "state": current_state,
                })
            else:
                force = "force=1" in self.path or "reset=1" in self.path
                def run_background():
                    cfg = PipelineConfig()
                    if force and cfg.checkpoint_file.exists():
                        cfg.checkpoint_file.unlink(missing_ok=True)
                    asyncio.run(AlaraRepricerPipeline(cfg).run())

                worker_thread = Thread(target=run_background, daemon=True)
                worker_thread.start()

                self._send_json(202, {
                    "message": f"Repricer sync job queued (force_reset={force})",
                    "state": state_manager.get_snapshot(),
                })
        elif self.path.startswith("/debug"):
            import urllib.request
            diagnostic = {}
            # 1. Egress IP
            try:
                ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode().strip()
                diagnostic["egress_ip"] = ip
            except Exception as e:
                diagnostic["egress_ip"] = f"Error: {e}"

            # 2. Curl Cffi test to Alara
            try:
                from curl_cffi import requests as cffi_requests
                resp = cffi_requests.get("https://alaradental.com", impersonate="chrome124", timeout=10)
                diagnostic["alara_homepage"] = {
                    "status_code": resp.status_code,
                    "server": resp.headers.get("server"),
                    "retry_after": resp.headers.get("retry-after"),
                    "cf_ray": resp.headers.get("cf-ray"),
                    "x_vercel_id": resp.headers.get("x-vercel-id"),
                    "body_preview": resp.text[:400]
                }
            except Exception as e:
                diagnostic["alara_homepage"] = {"error": str(e)}

            # 3. Product API test via ScraperAPI
            try:
                from curl_cffi import requests as cffi_requests
                cfg = PipelineConfig()
                target_prod_url = "https://alaradental.com/api/products/01KEZKNMED1TVH41ZNZGJZ0NDS"
                fetch_url = target_prod_url
                params = None
                if cfg.scraperapi_key:
                    fetch_url = "http://api.scraperapi.com"
                    params = {"api_key": cfg.scraperapi_key, "url": target_prod_url}

                p_resp = cffi_requests.get(fetch_url, params=params, timeout=30)
                diagnostic["alara_product_api"] = {
                    "via_scraperapi": bool(cfg.scraperapi_key),
                    "status_code": p_resp.status_code,
                    "server": p_resp.headers.get("server"),
                    "x_vercel_id": p_resp.headers.get("x-vercel-id"),
                    "body_preview": p_resp.text[:300]
                }
            except Exception as e:
                diagnostic["alara_product_api"] = {"error": str(e)}

            self._send_json(200, {"diagnostic": diagnostic})
        else:
            self._send_json(404, {"error": "Route not found", "valid_routes": ["/", "/status", "/sync", "/debug"]})

    def log_message(self, format, *args):
        # Keep web server access logs clean
        pass


def run_web_service():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), RenderRequestHandler)
    logger.info(f"Render Web Service active on http://0.0.0.0:{port}")
    server.serve_forever()


# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alara Repricer Sync Engine")
    parser.add_argument("--serve", action="store_true", help="Force Web Service mode on local port")
    args = parser.parse_args()

    # Automatically enter Web Service mode if running in Render or container ($PORT set)
    if "PORT" in os.environ or args.serve:
        run_web_service()
    else:
        logger.info("Executing directly in CLI batch mode...")
        asyncio.run(AlaraRepricerPipeline(PipelineConfig()).run())
