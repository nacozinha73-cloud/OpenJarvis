"""LinkedIn connector - indexes the official LinkedIn data export (read-only).

LinkedIn has no API for personal profiles, job search or messages, and its
User Agreement forbids bots and scraping. The supported way to bring your own
LinkedIn data into Jarvis is the official export:

    LinkedIn > Settings > Data privacy > Get a copy of your data

Drop the downloaded ``.zip`` (or its extracted ``.csv`` files) into
``~/.openjarvis/linkedin_export``. This connector reads them and never talks
to LinkedIn. Re-export and replace the files to refresh.
"""

from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from openjarvis.connectors._stubs import BaseConnector, Document, SyncStatus
from openjarvis.core.config import DEFAULT_CONFIG_DIR
from openjarvis.core.registry import ConnectorRegistry

DEFAULT_EXPORT_DIR = DEFAULT_CONFIG_DIR / "linkedin_export"

# Friendly titles for the files LinkedIn ships (matched case-insensitively on
# the file stem). Anything else is still indexed under its own name.
_TITLES = {
    "profile": "Perfil",
    "profile summary": "Resumo do perfil",
    "positions": "Experiencia profissional",
    "education": "Formacao",
    "skills": "Competencias",
    "certifications": "Certificacoes",
    "languages": "Idiomas",
    "projects": "Projetos",
    "courses": "Cursos",
    "honors": "Premios",
    "recommendations_received": "Recomendacoes recebidas",
    "recommendations_given": "Recomendacoes dadas",
    "endorsement_received_info": "Endossos recebidos",
    "connections": "Conexoes",
    "invitations": "Convites",
    "company follows": "Empresas que sigo",
    "job applications": "Candidaturas a vagas",
    "saved jobs": "Vagas salvas",
    "job seeker preferences": "Preferencias de busca de emprego",
    "shares": "Publicacoes",
    "comments": "Comentarios",
    "messages": "Mensagens",
}

# Files that are noise for an assistant (ads targeting, logins, IP history).
_SKIP = {"ad_targeting", "logins", "registration", "security challenges",
         "email addresses", "phonenumbers", "phone numbers", "receipts", "receipts_v2",
         "inferences_about_you", "member_follows", "rich_media", "learning"}

_ROWS_PER_DOC = 200


def _read_text(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def parse_linkedin_csv(text: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """Parse a LinkedIn CSV, skipping the 'Notes:' preamble some files carry."""
    lines = text.splitlines()
    start = 0
    for i, line in enumerate(lines[:20]):
        stripped = line.strip()
        if not stripped or stripped.lower().startswith("notes:") or stripped.startswith('"When exporting'):
            continue
        if stripped.count(",") >= 1:
            start = i
            break
    reader = csv.DictReader(io.StringIO("\n".join(lines[start:])))
    header = [h.strip() for h in (reader.fieldnames or []) if h]
    rows = []
    for row in reader:
        clean = {(k or "").strip(): (v or "").strip() for k, v in row.items() if k}
        if any(clean.values()):
            rows.append(clean)
    return header, rows


def _row_text(row: Dict[str, str]) -> str:
    return " | ".join(f"{k}: {v}" for k, v in row.items() if v)


@ConnectorRegistry.register("linkedin")
class LinkedInExportConnector(BaseConnector):
    """Read-only connector over the official LinkedIn data export."""

    connector_id = "linkedin"
    display_name = "LinkedIn (export)"
    auth_type = "filesystem"

    def __init__(self, export_path: str = "", *, vault_path: str = "") -> None:
        chosen = export_path or vault_path
        self._dir = Path(chosen) if chosen else DEFAULT_EXPORT_DIR
        self._status = SyncStatus()

    # -- file discovery -------------------------------------------------------

    def _csv_files(self) -> Iterator[Tuple[str, bytes]]:
        """Yield (name, bytes) for every CSV in the folder or inside any zip."""
        if not self._dir.is_dir():
            return
        seen = set()
        for path in sorted(self._dir.rglob("*.csv")):
            seen.add(path.name.lower())
            yield path.name, path.read_bytes()
        for zpath in sorted(self._dir.rglob("*.zip")):
            try:
                with zipfile.ZipFile(zpath) as zf:
                    for info in zf.infolist():
                        name = Path(info.filename).name
                        if name.lower().endswith(".csv") and name.lower() not in seen:
                            seen.add(name.lower())
                            yield name, zf.read(info)
            except zipfile.BadZipFile:
                continue

    # -- BaseConnector --------------------------------------------------------

    def is_connected(self) -> bool:
        if not self._dir.is_dir():
            return False
        return any(self._dir.rglob("*.csv")) or any(self._dir.rglob("*.zip"))

    def disconnect(self) -> None:
        """No-op: the export files are the user's; nothing is deleted."""

    def sync_status(self) -> SyncStatus:
        return self._status

    def sync(self, *, since: Optional[datetime] = None,
             cursor: Optional[str] = None) -> Iterator[Document]:  # noqa: ARG002
        self._status = SyncStatus(state="syncing")
        count = 0
        now = datetime.now(tz=timezone.utc)
        for name, raw in self._csv_files():
            stem = Path(name).stem.lower()
            if stem in _SKIP:
                continue
            header, rows = parse_linkedin_csv(_read_text(raw))
            if not rows:
                continue
            title = _TITLES.get(stem, Path(name).stem)

            if stem == "messages" and "CONVERSATION ID" in header:
                groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
                for r in rows:
                    groups[r.get("CONVERSATION ID", "")].append(r)
                for conv_id, msgs in groups.items():
                    msgs.sort(key=lambda r: r.get("DATE", ""))
                    people = sorted({r.get("FROM", "") for r in msgs if r.get("FROM")})
                    body = "\n".join(
                        f"[{r.get('DATE', '')}] {r.get('FROM', '')}: {r.get('CONTENT', '')}"
                        for r in msgs
                    )
                    subject = next((r.get("SUBJECT") for r in msgs if r.get("SUBJECT")), "")
                    digest = hashlib.sha1(conv_id.encode(), usedforsecurity=False).hexdigest()[:16]
                    count += 1
                    yield Document(
                        doc_id=f"linkedin:msg:{digest}",
                        source="linkedin",
                        doc_type="message",
                        title=f"LinkedIn - conversa com {', '.join(people)}"
                        + (f" ({subject})" if subject else ""),
                        content=body,
                        participants=people,
                        timestamp=now,
                        metadata={"file": name, "messages": len(msgs)},
                    )
                continue

            for part in range(0, len(rows), _ROWS_PER_DOC):
                chunk = rows[part:part + _ROWS_PER_DOC]
                suffix = f" (parte {part // _ROWS_PER_DOC + 1})" if len(rows) > _ROWS_PER_DOC else ""
                content = f"LinkedIn - {title}\n" + "\n".join(_row_text(r) for r in chunk)
                count += 1
                yield Document(
                    doc_id=f"linkedin:{stem.replace(' ', '_')}:{part // _ROWS_PER_DOC}",
                    source="linkedin",
                    doc_type="profile" if stem.startswith(("profile", "positions", "education", "skills")) else "record",
                    title=f"LinkedIn - {title}{suffix}",
                    content=content,
                    timestamp=now,
                    metadata={"file": name, "rows": len(chunk)},
                )
        self._status = SyncStatus(state="idle", items_synced=count, items_total=count, last_sync=now)


__all__ = ["LinkedInExportConnector", "parse_linkedin_csv", "DEFAULT_EXPORT_DIR"]
