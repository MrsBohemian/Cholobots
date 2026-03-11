import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

import discord
from discord.ext import commands

from db.database import (
    now_iso,
    insert_guard_event,
    fetch_guard_session_events,
    get_guard_last_known,
    build_guard_last_known_index,
)

active_org_sessions: Dict[int, "OrgSession"] = {}

# =============================================================================
#                               GUARDA (GUARDABOT)
# =============================================================================

ECO_HM = "HM"
ECO_CIRC = "CIRC"
ECO_STORAGE = "STORAGE"

AFFORDABLE_HOUSING_SYNS = [
    "affordable housing",
    "missing middle",
    "middle developers",
    "developer",
]

LOCATION_SYNONYMS: Dict[str, List[str]] = {
    "VAN": ["van", "in the van", "work van"],
    "MIC": ["mic", "materials innovation center", "materials innovation centre"],
    "GARAGE_ELECTRICAL": ["electrical zone", "electrical section", "electrical area", "electrical"],
    "GARAGE_SHELVES": ["shelves", "garage shelves"],
    "GARAGE_AUTOMOTIVE": ["automotive zone", "automotive area", "automotive"],
    "GARAGE_TOOLS": ["tools area", "tool area", "tools"],
    "GARAGE_WOOD": ["wood area", "lumber area", "wood", "lumber"],
    "GARAGE_CAMPING": ["camping zone", "camping area", "camping"],
    "GARAGE_BENCH_DOOM": ["tool doom work bench", "doom bench", "work bench", "workbench"],
    "STORAGE_CORNER_1": ["storage corner 1", "storage unit corner 1", "corner 1"],
    "STORAGE_CORNER_2": ["storage corner 2", "storage unit corner 2", "corner 2"],
    "STORAGE_CORNER_3": ["storage corner 3", "storage unit corner 3", "corner 3"],
    "STORAGE_CORNER_4": ["storage corner 4", "storage unit corner 4", "corner 4"],
    "STORAGE_CENTER": ["storage center", "storage unit center", "center"],
    "STORAGE_UNIT": ["storage unit", "storage"],
}

ECOSYSTEM_DEFAULT_BY_LOCATION: Dict[str, str] = {
    "VAN": ECO_HM,
    "GARAGE_ELECTRICAL": ECO_HM,
    "MIC": ECO_CIRC,
    "STORAGE_UNIT": ECO_STORAGE,
    "STORAGE_CORNER_1": ECO_STORAGE,
    "STORAGE_CORNER_2": ECO_STORAGE,
    "STORAGE_CORNER_3": ECO_STORAGE,
    "STORAGE_CORNER_4": ECO_STORAGE,
    "STORAGE_CENTER": ECO_STORAGE,
}


def normalize_location(text: str) -> Optional[str]:
    t = text.lower()
    for canonical, syns in LOCATION_SYNONYMS.items():
        for s in sorted(syns, key=len, reverse=True):
            if s in t:
                return canonical
    return None


def infer_ecosystem(text: str, canonical_location: Optional[str]) -> Optional[str]:
    t = text.lower()

    if any(s in t for s in AFFORDABLE_HOUSING_SYNS):
        return ECO_CIRC

    if "circular" in t or "circularity" in t or "reclaimed" in t:
        return ECO_CIRC

    if "storage" in t and "mic" not in t:
        return ECO_STORAGE

    if canonical_location and canonical_location in ECOSYSTEM_DEFAULT_BY_LOCATION:
        return ECOSYSTEM_DEFAULT_BY_LOCATION[canonical_location]

    return None


def basic_action(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["moved", "moving", "transfer", "transferred", "brought", "took to"]):
        return "MOVED"
    if any(w in t for w in ["found", "discovered"]):
        return "FOUND"
    if any(w in t for w in ["used", "installed", "consumed"]):
        return "USED"
    if any(w in t for w in ["donated", "sold", "scrapped", "trashed", "threw away"]):
        return "RELEASED"
    if any(w in t for w in ["allocated", "reserved", "for gardina", "for bryn", "for the week"]):
        return "ALLOCATED"
    return "PLACED"


def basic_qty_unit(text: str) -> Tuple[Optional[float], Optional[str]]:
    t = text.lower().strip()
    m = re.search(r"\b(about\s+)?(\d+(\.\d+)?)\s*(pcs|pc|pieces|piece|packs|pack|boxes|box|rolls|roll|tubes|tube)?\b", t)
    if not m:
        return None, None
    qty = float(m.group(2))
    unit = m.group(4)
    if unit:
        unit = unit.replace("pieces", "pcs").replace("piece", "pc")
    return qty, unit


def basic_item_guess(text: str) -> str:
    t = text.strip()
    t = re.sub(r"^(i\s+)?(putting|placing|moved|moving|found|using|used|storing|store|it'?s|its|they'?re|they are)\s+", "", t, flags=re.I)
    t = re.sub(r"\b(in|into|to)\s+the\s+.*$", "", t, flags=re.I)
    return (t[:120].strip() or "UNKNOWN ITEM")


def parse_guard_event(text: str, photo_url: Optional[str]) -> Dict[str, Any]:
    canonical_to = normalize_location(text) if text else None
    eco = infer_ecosystem(text or "", canonical_to)

    action = basic_action(text or "")
    item = basic_item_guess(text or "PHOTO")
    qty, unit = basic_qty_unit(text or "")

    return {
        "action": action,
        "item": item,
        "qty": qty,
        "unit": unit,
        "location_from": None,
        "location_to": canonical_to,
        "ecosystem": eco,
        "job": None,
        "notes": None,
        "photo_url": photo_url,
        "raw_text": text,
    }

@dataclass
class OrgSession:
    session_id: str
    channel_id: int
    started_ts: str
    started_by: int
    current_job: Optional[str] = None

def register_guard(bot):
    @bot.group(name="guard", invoke_without_command=True)
    async def guard_group(ctx: commands.Context):
        await ctx.send(
            "Guardabot commands:\n"
            "- `!guard organization` (start)\n"
            "- `!guard done` (finish + summary)\n"
            "- `!guard where <item>`\n"
            "- `!guard jobprep`"
        )