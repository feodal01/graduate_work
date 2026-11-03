import sqlite3
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class SQLiteFileDatabase:
    """Локальная SQLite-база: соединение, схема и низкоуровневые примитивы."""

    def __init__(self, file_path: str) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def execute(self, query: str, params: Sequence[Any] = ()) -> int:
        with self._connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount

    def fetch_one(
        self,
        query: str,
        params: Sequence[Any] = (),
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row is not None else None

    def fetch_all(
        self,
        query: str,
        params: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        with self._connect() as conn:
            yield conn

    def _init_database(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS producer_cursors (
                    source_table TEXT PRIMARY KEY,
                    last_modified TEXT NOT NULL,
                    last_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS etl_jobs (
                    id TEXT PRIMARY KEY,
                    source_table TEXT NOT NULL,
                    first_id TEXT NOT NULL,
                    first_modified TEXT NOT NULL,
                    last_id TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    locked_by TEXT,
                    locked_until TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE INDEX IF NOT EXISTS etl_jobs_status_created_at_idx
                    ON etl_jobs (status, created_at);

                CREATE INDEX IF NOT EXISTS etl_jobs_locked_until_idx
                    ON etl_jobs (locked_until);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.file_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn


class ProducerCursorsTable:
    """Прогресс producer-а по каждой исходной таблице."""

    def __init__(self, database: SQLiteFileDatabase) -> None:
        self.database = database

    def get_cursor(self, source_table: str) -> dict[str, Any] | None:
        return self.database.fetch_one(
            "SELECT * FROM producer_cursors WHERE source_table = ?",
            (source_table,),
        )

    def advance_cursor(
        self,
        *,
        source_table: str,
        last_modified: str,
        last_id: str,
        now: str,
    ) -> None:
        # UPSERT без блокировки: гонка двух producer-ов безвредна, т.к. оба
        # читают один и тот же курсор и пишут одно и то же значение.
        self.database.execute(
            """
            INSERT INTO producer_cursors (source_table, last_modified, last_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_table) DO UPDATE SET
                last_modified = excluded.last_modified,
                last_id = excluded.last_id,
                updated_at = excluded.updated_at
            """,
            (source_table, last_modified, last_id, now),
        )


class ETLJobsTable:
    """Durable-очередь работ ETL с лизингом и ретраями."""

    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_DONE = "done"
    STATUS_FAILED = "failed"

    def __init__(self, database: SQLiteFileDatabase) -> None:
        self.database = database

    def enqueue(
        self,
        *,
        job_id: str,
        source_table: str,
        first_id: str,
        first_modified: str,
        last_id: str,
        last_modified: str,
        row_count: int,
        now: str,
    ) -> None:
        """Поставить работу в очередь. Идемпотентно по job_id.

        INSERT OR IGNORE: повторный enqueue того же батча (например, от
        второго параллельного producer-а) — не дублирует job, а молча
        ничего не делает.
        """
        self.database.execute(
            """
            INSERT OR IGNORE INTO etl_jobs (
                id, source_table,
                first_id, first_modified, last_id, last_modified,
                row_count, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                source_table,
                first_id,
                first_modified,
                last_id,
                last_modified,
                row_count,
                self.STATUS_PENDING,
                now,
                now,
            ),
        )

    def claim_next(
        self,
        *,
        worker_id: str,
        now: str,
        locked_until: str,
    ) -> dict[str, Any] | None:
        """Атомарно взять старейшую pending-работу либо processing с истёкшим лизом."""
        return self.database.fetch_one(
            """
            UPDATE etl_jobs
            SET status = ?,
                locked_by = ?,
                locked_until = ?,
                attempts = attempts + 1,
                updated_at = ?
            WHERE id = (
                SELECT id FROM etl_jobs
                WHERE status = ?
                   OR (status = ? AND locked_until < ?)
                ORDER BY created_at
                LIMIT 1
            )
            RETURNING *
            """,
            (
                self.STATUS_PROCESSING,
                worker_id,
                locked_until,
                now,
                self.STATUS_PENDING,
                self.STATUS_PROCESSING,
                now,
            ),
        )

    def mark_done(self, *, job_id: str, now: str) -> None:
        self.database.execute(
            """
            UPDATE etl_jobs
            SET status = ?,
                finished_at = ?,
                updated_at = ?,
                locked_by = NULL,
                locked_until = NULL
            WHERE id = ?
            """,
            (self.STATUS_DONE, now, now, job_id),
        )

    def mark_failed(
        self,
        *,
        job_id: str,
        error: str,
        now: str,
        max_attempts: int,
    ) -> None:
        """Вернуть в pending для ретрая, либо терминальный failed при исчерпании попыток."""
        self.database.execute(
            """
            UPDATE etl_jobs
            SET status = CASE WHEN attempts >= ? THEN ? ELSE ? END,
                finished_at = CASE WHEN attempts >= ? THEN ? ELSE finished_at END,
                last_error = ?,
                updated_at = ?,
                locked_by = NULL,
                locked_until = NULL
            WHERE id = ?
            """,
            (
                max_attempts,
                self.STATUS_FAILED,
                self.STATUS_PENDING,
                max_attempts,
                now,
                error,
                now,
                job_id,
            ),
        )
