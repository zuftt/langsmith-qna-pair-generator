from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class GenerationRepository:
    def __init__(self, db_path: Optional[str] = None) -> None:
        default_path = os.path.join(os.path.dirname(__file__), "data", "generations.db")
        self.db_path = db_path or os.getenv("QNA_GENERATIONS_DB_PATH", default_path)
        self._write_lock = threading.Lock()
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def _init_db(self) -> None:
        conn = self._conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS generations (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    generation_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    source TEXT NOT NULL,
                    chunk_text TEXT NOT NULL DEFAULT '',
                    pair_status TEXT NOT NULL DEFAULT 'accepted',
                    reviewer_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(generation_id) REFERENCES generations(id)
                );

                CREATE INDEX IF NOT EXISTS idx_pairs_generation_id ON pairs(generation_id);

                CREATE TABLE IF NOT EXISTS agent_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    generation_id TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(generation_id) REFERENCES generations(id)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def create_generation(self, metadata: Dict) -> str:
        generation_id = str(uuid.uuid4())
        now = _utc_now_iso()
        with self._write_lock:
            conn = self._conn()
            try:
                conn.execute(
                    """
                    INSERT INTO generations (id, created_at, updated_at, status, metadata_json)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (generation_id, now, now, "running", json.dumps(metadata, ensure_ascii=False)),
                )
                conn.commit()
            finally:
                conn.close()
        return generation_id

    def update_generation_status(self, generation_id: str, status: str) -> None:
        with self._write_lock:
            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE generations SET status = ?, updated_at = ? WHERE id = ?",
                    (status, _utc_now_iso(), generation_id),
                )
                conn.commit()
            finally:
                conn.close()

    def get_generation(self, generation_id: str) -> Optional[Dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM generations WHERE id = ?", (generation_id,)).fetchone()
            if not row:
                return None
            metadata = json.loads(row["metadata_json"] or "{}")
            return {
                "id": row["id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "status": row["status"],
                "metadata": metadata,
            }
        finally:
            conn.close()

    def upsert_generation_metadata(self, generation_id: str, patch: Dict) -> None:
        record = self.get_generation(generation_id)
        if not record:
            return
        metadata = record.get("metadata", {})
        metadata.update(patch)
        with self._write_lock:
            conn = self._conn()
            try:
                conn.execute(
                    "UPDATE generations SET metadata_json = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(metadata, ensure_ascii=False), _utc_now_iso(), generation_id),
                )
                conn.commit()
            finally:
                conn.close()

    def add_pair(
        self,
        generation_id: str,
        question: str,
        answer: str,
        source: str,
        chunk_text: str,
        pair_status: str = "accepted",
        reviewer_reason: Optional[str] = None,
    ) -> int:
        now = _utc_now_iso()
        with self._write_lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    """
                    INSERT INTO pairs (
                        generation_id, question, answer, source, chunk_text,
                        pair_status, reviewer_reason, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        generation_id,
                        question,
                        answer,
                        source,
                        chunk_text,
                        pair_status,
                        reviewer_reason,
                        now,
                        now,
                    ),
                )
                conn.commit()
                return int(cur.lastrowid)
            finally:
                conn.close()

    def list_pairs(self, generation_id: str) -> List[Dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT id, generation_id, question, answer, source, chunk_text, pair_status, reviewer_reason,
                       created_at, updated_at
                FROM pairs
                WHERE generation_id = ?
                ORDER BY id ASC
                """,
                (generation_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_questions(self, generation_id: str) -> List[str]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT question FROM pairs WHERE generation_id = ?", (generation_id,)).fetchall()
            return [row["question"] for row in rows]
        finally:
            conn.close()

    def update_pair(self, generation_id: str, pair_id: int, question: str, answer: str) -> bool:
        with self._write_lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    """
                    UPDATE pairs
                    SET question = ?, answer = ?, pair_status = ?, updated_at = ?
                    WHERE id = ? AND generation_id = ?
                    """,
                    (question, answer, "user_edited", _utc_now_iso(), pair_id, generation_id),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def delete_pair(self, generation_id: str, pair_id: int) -> bool:
        with self._write_lock:
            conn = self._conn()
            try:
                cur = conn.execute(
                    "DELETE FROM pairs WHERE id = ? AND generation_id = ?",
                    (pair_id, generation_id),
                )
                conn.commit()
                return cur.rowcount > 0
            finally:
                conn.close()

    def add_event(self, generation_id: str, stage: str, message: str) -> None:
        with self._write_lock:
            conn = self._conn()
            try:
                conn.execute(
                    """
                    INSERT INTO agent_events (generation_id, stage, message, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (generation_id, stage, message, _utc_now_iso()),
                )
                conn.commit()
            finally:
                conn.close()

    def cleanup_old_generations(self, retention_days: int) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        deleted = 0
        with self._write_lock:
            conn = self._conn()
            try:
                ids = conn.execute(
                    "SELECT id FROM generations WHERE created_at < ?",
                    (cutoff,),
                ).fetchall()
                if not ids:
                    return 0
                generation_ids = [row["id"] for row in ids]
                for gid in generation_ids:
                    conn.execute("DELETE FROM pairs WHERE generation_id = ?", (gid,))
                    conn.execute("DELETE FROM agent_events WHERE generation_id = ?", (gid,))
                    conn.execute("DELETE FROM generations WHERE id = ?", (gid,))
                conn.commit()
                deleted = len(generation_ids)
            finally:
                conn.close()
        return deleted
