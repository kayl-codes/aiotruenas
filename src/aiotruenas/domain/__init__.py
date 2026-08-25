"""Domain-normalization layer for TrueNAS JSON-RPC responses.

Turns raw ``TrueNASClient.call()`` results into normalized, typed-ish dicts,
mirroring the shape historically produced by consumer integrations' own
``apiparser.py``/``coordinator.py`` (see PROMPT.md and MIGRATION_PLAN.md for
the migration this package implements).
"""

from __future__ import annotations
