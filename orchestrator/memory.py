"""
memory.py — Camelot OS Orchestrator Memory Layer

Provides persistent conversation context using Supabase as the backend.
Supports multi-session storage, message history retrieval, session metadata,
and entity tracking (addresses, companies, deal amounts mentioned in chat).

Required Supabase table DDL:

    CREATE TABLE orchestrator_sessions (
        id            BIGSERIAL PRIMARY KEY,
        session_id    TEXT NOT NULL,
        role          TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
        content       TEXT NOT NULL,
        metadata      JSONB DEFAULT '{}',
        created_at    TIMESTAMPTZ DEFAULT now()
    );

    CREATE INDEX idx_orchestrator_sessions_session_id
        ON orchestrator_sessions (session_id, created_at DESC);

    -- Optional: session-level metadata table
    CREATE TABLE orchestrator_session_meta (
        session_id    TEXT PRIMARY KEY,
        entities      JSONB DEFAULT '{}',
        summary       TEXT,
        created_at    TIMESTAMPTZ DEFAULT now(),
        updated_at    TIMESTAMPTZ DEFAULT now()
    );

Environment variables:
    SUPABASE_URL          — e.g., https://xyzabc.supabase.co
    SUPABASE_SERVICE_KEY  — Service role key (bypasses RLS for server use)
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Supabase client setup — lazy initialization to avoid import errors
# ---------------------------------------------------------------------------

_supabase_client = None


def _get_supabase():
    """
    Lazily initialize and return a Supabase client.

    Raises:
        RuntimeError: If SUPABASE_URL or SUPABASE_SERVICE_KEY are not set.
        ImportError:  If the supabase-py package is not installed.
    """
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables must be set. "
            "Check your .env file or environment configuration."
        )

    try:
        from supabase import create_client, Client  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "supabase-py package not installed. Run: pip install supabase"
        ) from exc

    _supabase_client = create_client(url, key)
    logger.info("Supabase client initialized for URL: %s", url[:40])
    return _supabase_client


# ---------------------------------------------------------------------------
# Message model
# ---------------------------------------------------------------------------

class Message:
    """
    Represents a single conversation message.

    Attributes:
        session_id:  Session identifier string
        role:        'user' | 'assistant' | 'system' | 'tool'
        content:     Message text content
        metadata:    Optional structured data (routing decisions, bot outputs, etc.)
        created_at:  ISO timestamp string
        id:          Database row ID (set after storage)
    """

    VALID_ROLES = {"user", "assistant", "system", "tool"}

    def __init__(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
        id: Optional[int] = None,
    ):
        if role not in self.VALID_ROLES:
            raise ValueError(
                f"Invalid role '{role}'. Must be one of: {self.VALID_ROLES}"
            )
        self.session_id = session_id
        self.role = role
        self.content = content
        self.metadata = metadata or {}
        self.created_at = created_at or datetime.utcnow().isoformat()
        self.id = id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "role": self.role,
            "content": self.content,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }

    def to_llm_format(self) -> Dict[str, str]:
        """Format for use in LLM API call (role + content only)."""
        return {"role": self.role, "content": self.content}

    def __repr__(self) -> str:
        preview = self.content[:60].replace("\n", " ")
        return f"Message(role={self.role!r}, content={preview!r}...)"


# ---------------------------------------------------------------------------
# Entity Extractor
# ---------------------------------------------------------------------------

class EntityExtractor:
    """
    Heuristically extracts named entities from conversation text.
    Used to maintain session-level context of what's been discussed.
    """

    @staticmethod
    def extract(text: str) -> Dict[str, List[str]]:
        """
        Extract addresses, companies, dollar amounts, and NYC concepts.

        Returns:
            Dict with keys: addresses, companies, amounts, nyc_concepts
        """
        entities: Dict[str, List[str]] = {
            "addresses": [],
            "companies": [],
            "amounts": [],
            "nyc_concepts": [],
        }

        # Street addresses
        addr_pattern = (
            r'\b\d+\s+[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Boulevard|Blvd|'
            r'Road|Rd|Drive|Dr|Lane|Ln|Place|Pl|Court|Ct|Parkway|Pkwy|'
            r'Broadway|Park|Plaza)\b'
        )
        entities["addresses"] = list(set(
            re.findall(addr_pattern, text, re.IGNORECASE)
        ))

        # Company names (capitalized multi-word + entity suffix)
        company_pattern = (
            r'\b[A-Z][A-Za-z\s&\-\']+(?:LLC|Inc|Corp|Management|Properties|'
            r'Realty|Group|Partners|Associates|Co\.?)\b'
        )
        entities["companies"] = list(set(
            re.findall(company_pattern, text)
        ))

        # Dollar amounts
        amount_pattern = r'\$[\d,]+(?:\.\d{1,2})?(?:\s*[Mm]illion|\s*[Kk])?'
        entities["amounts"] = list(set(
            re.findall(amount_pattern, text)
        ))

        # NYC-specific concepts
        nyc_terms = [
            "HPD", "DOB", "ECB", "ACRIS", "Local Law 97", "LL97",
            "Section 8", "HCV", "multifamily", "co-op", "condo",
            "Queens", "Brooklyn", "Bronx", "Manhattan", "Staten Island",
            "Westchester", "HUD", "NYCHA",
        ]
        found_concepts = [term for term in nyc_terms if term.lower() in text.lower()]
        entities["nyc_concepts"] = list(set(found_concepts))

        return entities


# ---------------------------------------------------------------------------
# OrchestratorMemory
# ---------------------------------------------------------------------------

class OrchestratorMemory:
    """
    Persistent memory for Camelot OS Orchestrator conversations.

    Stores and retrieves conversation history from Supabase. Each session
    is isolated by session_id. Supports entity tracking at the session level.

    Usage:
        memory = OrchestratorMemory()

        # Store messages
        memory.add_message("session_abc", "user", "Find PM companies in Queens")
        memory.add_message("session_abc", "assistant", "Routing to Scout bot...")

        # Retrieve history
        history = memory.get_history("session_abc", last_n=10)
        for msg in history:
            print(f"{msg.role}: {msg.content}")

        # Clear a session
        memory.clear_session("session_abc")
    """

    TABLE_MESSAGES = "orchestrator_sessions"
    TABLE_META = "orchestrator_session_meta"

    def __init__(self, use_local_fallback: bool = True):
        """
        Args:
            use_local_fallback: If True, falls back to in-memory storage when
                                Supabase is unavailable (useful for dev/testing).
        """
        self.use_local_fallback = use_local_fallback
        self._local_store: Dict[str, List[Message]] = {}  # In-memory fallback
        self._meta_store: Dict[str, Dict[str, Any]] = {}   # In-memory meta fallback
        self._use_supabase: bool = True  # Will flip to False on first connection failure

    # ------------------------------------------------------------------
    # Core Message Operations
    # ------------------------------------------------------------------

    def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Message]:
        """
        Store a new message in the session history.

        Also extracts entities from user/assistant messages and updates
        the session metadata in the background.

        Args:
            session_id: Unique session identifier
            role:       Message role (user/assistant/system/tool)
            content:    Message text content
            metadata:   Optional structured data to persist alongside the message

        Returns:
            The stored Message object, or None on failure.
        """
        msg = Message(
            session_id=session_id,
            role=role,
            content=content,
            metadata=metadata or {},
        )

        if self._use_supabase:
            try:
                sb = _get_supabase()
                row = msg.to_dict()
                # Serialize metadata to JSON string if needed
                if isinstance(row.get("metadata"), dict):
                    row["metadata"] = json.dumps(row["metadata"])

                sb.table(self.TABLE_MESSAGES).insert(row).execute()
                logger.debug(
                    "Stored message [session=%s role=%s len=%d]",
                    session_id, role, len(content)
                )

                # Update entity tracking
                if role in ("user", "assistant"):
                    self._update_entities(session_id, content)

                return msg

            except Exception as exc:
                logger.warning(
                    "Supabase write failed: %s. Falling back to local store.", exc
                )
                if not self.use_local_fallback:
                    raise
                self._use_supabase = False

        # Local fallback
        if session_id not in self._local_store:
            self._local_store[session_id] = []
        self._local_store[session_id].append(msg)
        logger.debug("Stored message in local store [session=%s]", session_id)
        return msg

    def get_history(
        self,
        session_id: str,
        last_n: int = 10,
        include_system: bool = False,
    ) -> List[Message]:
        """
        Retrieve the most recent N messages for a session.

        Args:
            session_id:     Session to retrieve
            last_n:         Maximum number of messages to return (newest first,
                            then reversed to chronological order)
            include_system: Whether to include 'system' role messages

        Returns:
            List of Message objects in chronological order (oldest first).
        """
        if self._use_supabase:
            try:
                sb = _get_supabase()
                query = (
                    sb.table(self.TABLE_MESSAGES)
                    .select("*")
                    .eq("session_id", session_id)
                    .order("created_at", desc=True)
                    .limit(last_n)
                )
                response = query.execute()
                rows = response.data or []

                messages = []
                for row in reversed(rows):  # Restore chronological order
                    if not include_system and row.get("role") == "system":
                        continue
                    # Deserialize metadata
                    meta = row.get("metadata", {})
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except json.JSONDecodeError:
                            meta = {}
                    messages.append(
                        Message(
                            session_id=row["session_id"],
                            role=row["role"],
                            content=row["content"],
                            metadata=meta,
                            created_at=row.get("created_at"),
                            id=row.get("id"),
                        )
                    )
                return messages

            except Exception as exc:
                logger.warning(
                    "Supabase read failed: %s. Using local store.", exc
                )
                self._use_supabase = False

        # Local fallback
        msgs = self._local_store.get(session_id, [])
        filtered = [m for m in msgs if include_system or m.role != "system"]
        return filtered[-last_n:]

    def get_history_for_llm(
        self,
        session_id: str,
        last_n: int = 10,
        system_prompt: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        Return conversation history formatted for direct use in LLM API calls.

        Args:
            session_id:    Session to retrieve
            last_n:        Max messages to include
            system_prompt: If provided, prepended as a system message

        Returns:
            List of {"role": ..., "content": ...} dicts for OpenAI/Anthropic APIs.
        """
        messages = self.get_history(session_id, last_n=last_n, include_system=False)
        formatted = [m.to_llm_format() for m in messages if m.role != "tool"]

        if system_prompt:
            formatted.insert(0, {"role": "system", "content": system_prompt})

        return formatted

    def clear_session(self, session_id: str) -> bool:
        """
        Delete all messages for a session.

        Args:
            session_id: Session to clear

        Returns:
            True on success, False on failure.
        """
        if self._use_supabase:
            try:
                sb = _get_supabase()
                sb.table(self.TABLE_MESSAGES).delete().eq(
                    "session_id", session_id
                ).execute()
                # Also clear meta
                sb.table(self.TABLE_META).delete().eq(
                    "session_id", session_id
                ).execute()
                logger.info("Cleared session: %s", session_id)
                return True
            except Exception as exc:
                logger.warning("Failed to clear session from Supabase: %s", exc)
                return False

        # Local fallback
        self._local_store.pop(session_id, None)
        self._meta_store.pop(session_id, None)
        return True

    # ------------------------------------------------------------------
    # Session Metadata
    # ------------------------------------------------------------------

    def get_session_meta(self, session_id: str) -> Dict[str, Any]:
        """
        Retrieve session-level metadata (entity tracking, summaries, etc.).

        Returns:
            Dict with 'entities', 'summary', 'created_at', 'updated_at'.
        """
        if self._use_supabase:
            try:
                sb = _get_supabase()
                response = (
                    sb.table(self.TABLE_META)
                    .select("*")
                    .eq("session_id", session_id)
                    .limit(1)
                    .execute()
                )
                rows = response.data or []
                if rows:
                    meta = rows[0]
                    if isinstance(meta.get("entities"), str):
                        meta["entities"] = json.loads(meta["entities"])
                    return meta
                return {"session_id": session_id, "entities": {}, "summary": ""}
            except Exception as exc:
                logger.warning("Failed to fetch session meta: %s", exc)

        return self._meta_store.get(
            session_id,
            {"session_id": session_id, "entities": {}, "summary": ""}
        )

    def _update_entities(self, session_id: str, text: str) -> None:
        """Update entity tracking for a session based on new message content."""
        new_entities = EntityExtractor.extract(text)
        existing_meta = self.get_session_meta(session_id)
        existing_entities = existing_meta.get("entities", {})

        # Merge (deduplicate)
        merged = {}
        for key in set(list(new_entities.keys()) + list(existing_entities.keys())):
            merged[key] = list(set(
                existing_entities.get(key, []) + new_entities.get(key, [])
            ))

        now = datetime.utcnow().isoformat()

        if self._use_supabase:
            try:
                sb = _get_supabase()
                upsert_data = {
                    "session_id": session_id,
                    "entities": json.dumps(merged),
                    "updated_at": now,
                }
                sb.table(self.TABLE_META).upsert(
                    upsert_data, on_conflict="session_id"
                ).execute()
                return
            except Exception as exc:
                logger.debug("Entity meta update to Supabase failed: %s", exc)

        # Local fallback
        if session_id not in self._meta_store:
            self._meta_store[session_id] = {
                "session_id": session_id,
                "entities": {},
                "summary": "",
                "created_at": now,
            }
        self._meta_store[session_id]["entities"] = merged
        self._meta_store[session_id]["updated_at"] = now

    def set_summary(self, session_id: str, summary: str) -> None:
        """
        Store a natural-language summary of the session so far.

        Useful for compressing long sessions: generate a summary with an LLM,
        store it here, then pass it in the system prompt for future turns.
        """
        now = datetime.utcnow().isoformat()

        if self._use_supabase:
            try:
                sb = _get_supabase()
                sb.table(self.TABLE_META).upsert(
                    {"session_id": session_id, "summary": summary, "updated_at": now},
                    on_conflict="session_id",
                ).execute()
                return
            except Exception as exc:
                logger.warning("Failed to store session summary: %s", exc)

        if session_id not in self._meta_store:
            self._meta_store[session_id] = {
                "session_id": session_id,
                "entities": {},
                "created_at": now,
            }
        self._meta_store[session_id]["summary"] = summary
        self._meta_store[session_id]["updated_at"] = now

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def session_exists(self, session_id: str) -> bool:
        """Check if a session has any stored messages."""
        history = self.get_history(session_id, last_n=1)
        return len(history) > 0

    def list_sessions(self, limit: int = 50) -> List[str]:
        """
        List the most recently active session IDs.

        Args:
            limit: Max number of session IDs to return

        Returns:
            List of session_id strings, ordered by most recent activity.
        """
        if self._use_supabase:
            try:
                sb = _get_supabase()
                response = (
                    sb.table(self.TABLE_MESSAGES)
                    .select("session_id")
                    .order("created_at", desc=True)
                    .limit(limit * 10)  # Over-fetch since we need distinct
                    .execute()
                )
                seen = []
                for row in response.data or []:
                    sid = row["session_id"]
                    if sid not in seen:
                        seen.append(sid)
                    if len(seen) >= limit:
                        break
                return seen
            except Exception as exc:
                logger.warning("Failed to list sessions: %s", exc)

        return list(self._local_store.keys())[:limit]

    def message_count(self, session_id: str) -> int:
        """Return the total number of messages stored for a session."""
        if self._use_supabase:
            try:
                sb = _get_supabase()
                response = (
                    sb.table(self.TABLE_MESSAGES)
                    .select("id", count="exact")
                    .eq("session_id", session_id)
                    .execute()
                )
                return response.count or 0
            except Exception as exc:
                logger.warning("Failed to count messages: %s", exc)

        return len(self._local_store.get(session_id, []))


# ---------------------------------------------------------------------------
# Supabase Schema Bootstrap
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Camelot OS Orchestrator: Supabase Schema Bootstrap
-- Run this in the Supabase SQL editor or via psql

CREATE TABLE IF NOT EXISTS orchestrator_sessions (
    id            BIGSERIAL PRIMARY KEY,
    session_id    TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content       TEXT NOT NULL,
    metadata      JSONB DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_session_id
    ON orchestrator_sessions (session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS orchestrator_session_meta (
    session_id    TEXT PRIMARY KEY,
    entities      JSONB DEFAULT '{}',
    summary       TEXT DEFAULT '',
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Enable Row Level Security (service key bypasses this)
ALTER TABLE orchestrator_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestrator_session_meta ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "Service role full access" ON orchestrator_sessions
    FOR ALL USING (auth.role() = 'service_role');
CREATE POLICY "Service role full access" ON orchestrator_session_meta
    FOR ALL USING (auth.role() = 'service_role');
"""


def bootstrap_schema() -> None:
    """
    Run the Supabase schema bootstrap SQL.
    Call this once during deployment or first-run setup.
    """
    logger.info("Bootstrapping Supabase schema...")
    sb = _get_supabase()
    sb.rpc("exec_sql", {"sql": SCHEMA_SQL}).execute()
    logger.info("Schema bootstrap complete.")
