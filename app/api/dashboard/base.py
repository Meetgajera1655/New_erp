"""
Base class that every dashboard builder must extend.

A "builder" encapsulates all data-fetching for one named dashboard.
The unified router calls these methods and combines their output.

Subclass contract
-----------------
Each method receives:
  - db       : SQLAlchemy Session
  - schema   : tenant schema string
  - table_id : (tables only) which table to fetch
  - params   : TablePaginationParams for that table
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.api.dashboard.models import TablePaginationParams, TableResult


class BaseDashboardBuilder(ABC):
    """Abstract base for all dashboard builders."""

    # ------------------------------------------------------------------
    # Subclasses MUST implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def get_kpis(self, db: Session, schema: str) -> Dict[str, Any]:
        """Return a flat dict of KPI name → value."""

    @abstractmethod
    def get_charts(self, db: Session, schema: str) -> Dict[str, Any]:
        """Return chart data keyed by chart name."""

    # ------------------------------------------------------------------
    # Subclasses SHOULD implement these (defaults return empty)
    # ------------------------------------------------------------------

    def available_tables(self) -> List[str]:
        """Return list of table IDs this dashboard exposes."""
        return []

    def get_table(
        self,
        db: Session,
        schema: str,
        table_id: str,
        params: TablePaginationParams,
    ) -> Tuple[List[Dict], int]:
        """
        Return (rows, total_count) for `table_id`.

        Default raises NotImplementedError – subclasses override per table.
        """
        raise NotImplementedError(f"Table '{table_id}' is not implemented.")

    def get_alerts(self, db: Session, schema: str) -> Optional[Dict[str, Any]]:
        """Optional – return alert data or None."""
        return None

    # ------------------------------------------------------------------
    # Framework helpers (do not override)
    # ------------------------------------------------------------------

    def build_tables(
        self,
        db: Session,
        schema: str,
        requested_tables: List[str],
        table_params: Dict[str, TablePaginationParams],
    ) -> List[TableResult]:
        """
        Fetch all requested tables and wrap in TableResult.
        Unknown table IDs are silently skipped.
        """
        valid = set(self.available_tables())
        results: List[TableResult] = []

        for tid in requested_tables:
            if tid not in valid:
                continue  # skip unknown table IDs gracefully

            params = table_params.get(tid, TablePaginationParams())
            try:
                rows, total = self.get_table(db, schema, tid, params)
            except NotImplementedError:
                continue

            from app.api.dashboard.models import PaginationMeta

            results.append(
                TableResult(
                    id=tid,
                    data=rows,
                    pagination=PaginationMeta.build(
                        page=params.page,
                        limit=params.limit,
                        total=total,
                    ),
                )
            )

        return results
