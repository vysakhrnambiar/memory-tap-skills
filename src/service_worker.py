"""
Service Worker — daemon thread that processes inter-skill service requests.

Polls service_requests table every 1 second for PENDING requests.
Processes one at a time (FIFO). Provider's handle_request() opens/closes
its own tabs.

Communication is DB-only: consumer writes to DB, polls DB. Worker reads
DB, writes DB. No threading events, no signals, no callbacks.
"""
import json
import logging
import threading
import time

from .cdp_client import CDPClient
from .db.core_db import get_core_connection

logger = logging.getLogger("memory_tap.service_worker")


class ServiceWorker:
    """Polls service_requests every 1s, executes PENDING requests FIFO."""

    def __init__(self, skills: dict, core_db_path: str, chrome_manager=None):
        """
        Args:
            skills: dict of skill_name -> BaseSkill instance
            core_db_path: path to core.db
            chrome_manager: ChromeManager instance (unused directly, but
                available if needed for future extensions)
        """
        self._skills = skills
        self._db_path = core_db_path
        self._chrome = chrome_manager
        self._running = False
        self._thread = None

    def start(self):
        """Start the service worker daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Service worker started")

    def stop(self):
        """Stop the service worker."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._thread = None
        logger.info("Service worker stopped")

    def _loop(self):
        """Main worker loop — poll every 1 second."""
        while self._running:
            try:
                self._check_timeouts()
                self._process_one()
            except Exception as e:
                logger.error("Service worker error: %s", e)
            time.sleep(1)

    def _check_timeouts(self):
        """Check for PROCESSING requests that exceeded max_duration_seconds."""
        conn = get_core_connection(self._db_path)
        try:
            # Find requests stuck in PROCESSING
            rows = conn.execute(
                "SELECT sr.id, sr.to_skill, sr.service_name, sr.claimed_at "
                "FROM service_requests sr "
                "WHERE sr.state = 'PROCESSING' AND sr.claimed_at IS NOT NULL"
            ).fetchall()

            for row in rows:
                # Look up max_duration from service_registry
                reg = conn.execute(
                    "SELECT max_duration_seconds FROM service_registry "
                    "WHERE skill_name = ? AND service_name = ?",
                    (row["to_skill"], row["service_name"]),
                ).fetchone()

                max_duration = reg["max_duration_seconds"] if reg else 60

                # Calculate elapsed time since claimed
                from datetime import datetime
                claimed_at = datetime.fromisoformat(row["claimed_at"])
                elapsed = (datetime.now() - claimed_at).total_seconds()

                if elapsed > max_duration:
                    logger.warning(
                        "Service request %d TIMEOUT: %s.%s exceeded %ds (elapsed %.0fs)",
                        row["id"], row["to_skill"], row["service_name"],
                        max_duration, elapsed,
                    )
                    conn.execute(
                        "UPDATE service_requests SET state = 'TIMEOUT', "
                        "error = ?, completed_at = datetime('now'), duration_seconds = ? "
                        "WHERE id = ?",
                        (f"Provider exceeded max_duration_seconds ({max_duration}s)",
                         elapsed, row["id"]),
                    )
                    conn.commit()
        finally:
            conn.close()

    def _process_one(self):
        """Pick the oldest PENDING request and process it."""
        conn = get_core_connection(self._db_path)
        row = conn.execute(
            "SELECT * FROM service_requests WHERE state = 'PENDING' "
            "ORDER BY created_at LIMIT 1"
        ).fetchone()
        if not row:
            conn.close()
            return

        req_id = row["id"]
        to_skill = row["to_skill"]
        service_name = row["service_name"]
        logger.info(
            "Service request %d: PENDING -> CLAIMED (%s -> %s.%s)",
            req_id, row["from_skill"], to_skill, service_name,
        )

        # Mark CLAIMED
        conn.execute(
            "UPDATE service_requests SET state = 'CLAIMED', claimed_at = datetime('now') "
            "WHERE id = ?", (req_id,)
        )
        conn.commit()

        # Find provider skill
        provider = self._skills.get(to_skill)
        if not provider:
            logger.error("Service request %d: provider '%s' not found", req_id, to_skill)
            conn.execute(
                "UPDATE service_requests SET state = 'FAILED', "
                "error = ?, completed_at = datetime('now') "
                "WHERE id = ?",
                (f"Provider skill '{to_skill}' not found", req_id),
            )
            conn.commit()
            conn.close()
            return

        # Mark PROCESSING
        logger.info("Service request %d: CLAIMED -> PROCESSING", req_id)
        conn.execute(
            "UPDATE service_requests SET state = 'PROCESSING' WHERE id = ?",
            (req_id,),
        )
        conn.commit()
        conn.close()

        # Execute — provider manages its own tabs
        start_time = time.time()
        try:
            payload = json.loads(row["payload"]) if row["payload"] else {}
            with CDPClient() as cdp:
                result = provider.handle_request(service_name, payload, cdp)

            duration = time.time() - start_time
            logger.info(
                "Service request %d: PROCESSING -> COMPLETED (%.1fs)",
                req_id, duration,
            )
            conn = get_core_connection(self._db_path)
            conn.execute(
                "UPDATE service_requests SET state = 'COMPLETED', "
                "result = ?, completed_at = datetime('now'), duration_seconds = ? "
                "WHERE id = ?",
                (json.dumps(result, ensure_ascii=False), duration, req_id),
            )
            conn.commit()
            conn.close()

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"{type(e).__name__}: {e}"
            logger.error(
                "Service request %d: PROCESSING -> FAILED (%.1fs): %s",
                req_id, duration, error_msg,
            )
            conn = get_core_connection(self._db_path)
            conn.execute(
                "UPDATE service_requests SET state = 'FAILED', "
                "error = ?, completed_at = datetime('now'), duration_seconds = ? "
                "WHERE id = ?",
                (error_msg, duration, req_id),
            )
            conn.commit()
            conn.close()
