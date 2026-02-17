"""
Build id_table.csv and sources_table.csv from a dump of the e-lautedb.

Generated outputs:
- scripts/upload_to_RDM/tables/id_table.csv
- scripts/upload_to_RDM/tables/sources_table.csv

Usage:
- Generate both tables:
  python scripts/upload_to_RDM/build_tables_from_dump.py <path_to_dump.sql>

- Generate only id_table.csv:
  python scripts/upload_to_RDM/build_tables_from_dump.py <path_to_dump.sql> --tables id
- Generate only sources_table.csv:
  python scripts/upload_to_RDM/build_tables_from_dump.py <path_to_dump.sql> --tables sources
- Use custom output paths:
  python scripts/upload_to_RDM/build_tables_from_dump.py <path_to_dump.sql> --id-output <id_csv_path> --sources-output <sources_csv_path>
"""

from __future__ import annotations

import argparse
import csv
import html
import re
from pathlib import Path


WORK_ID_PATTERN = re.compile(r"_n\d+$")


def _unescape_mysql_string(value: str) -> str:
    out = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value):
            nxt = value[i + 1]
            mapping = {
                "0": "\0",
                "b": "\b",
                "n": "\n",
                "r": "\r",
                "t": "\t",
                "Z": "\x1a",
                "\\": "\\",
                "'": "'",
                '"': '"',
            }
            out.append(mapping.get(nxt, nxt))
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _decode_sql_value(token: str):
    token = token.strip()
    if token == "NULL":
        return None
    if token.startswith("'") and token.endswith("'"):
        return _unescape_mysql_string(token[1:-1])
    return token


def _split_sql_fields(tuple_content: str) -> list[str]:
    fields = []
    current = []
    in_quote = False
    escaped = False
    for ch in tuple_content:
        if in_quote:
            current.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "'":
                in_quote = False
            continue
        if ch == "'":
            in_quote = True
            current.append(ch)
        elif ch == ",":
            fields.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    fields.append("".join(current).strip())
    return fields


def _extract_tuples(values_sql: str) -> list[str]:
    tuples = []
    in_quote = False
    escaped = False
    depth = 0
    current = []

    for ch in values_sql:
        if in_quote:
            if depth > 0:
                current.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "'":
                in_quote = False
            continue

        if ch == "'":
            in_quote = True
            if depth > 0:
                current.append(ch)
            continue

        if ch == "(":
            if depth == 0:
                current = []
            else:
                current.append(ch)
            depth += 1
            continue

        if ch == ")":
            depth -= 1
            if depth == 0:
                tuples.append("".join(current))
                current = []
            else:
                current.append(ch)
            continue

        if depth > 0:
            current.append(ch)

    return tuples


def _iter_insert_tuples(dump_path: Path, table_name: str):
    prefix = f"INSERT INTO `{table_name}` VALUES"
    collecting = False
    buffer = []

    with dump_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            stripped = line.strip()

            if not collecting and stripped.startswith(prefix):
                collecting = True
                after_values = stripped.split("VALUES", 1)[1].strip()
                buffer = [after_values]
                if stripped.endswith(";"):
                    payload = " ".join(buffer).rstrip(";")
                    for tuple_content in _extract_tuples(payload):
                        yield tuple_content
                    collecting = False
                    buffer = []
                continue

            if collecting:
                buffer.append(stripped)
                if stripped.endswith(";"):
                    payload = " ".join(buffer).rstrip(";")
                    for tuple_content in _extract_tuples(payload):
                        yield tuple_content
                    collecting = False
                    buffer = []


def _first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            return cleaned
    return None


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    text = (
        text.replace("<br>", " ").replace("<br/>", " ").replace("<br />", " ")
    )
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _append_modern_title(
    original_title: str | None, modern_titles: list[str]
) -> str | None:
    base = _normalize_text(original_title)
    if not modern_titles:
        return base

    seen = set()
    cleaned_modern = []
    for title in modern_titles:
        cleaned = _normalize_text(title)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned_modern.append(cleaned)

    if not cleaned_modern:
        return base
    if not base:
        return " / ".join(cleaned_modern)
    return f"{base} ({' / '.join(cleaned_modern)})"


def _looks_like_modern_text(value: str | None) -> bool:
    text = _normalize_text(value)
    if not text:
        return False
    if len(text) < 8:
        return False
    if text.startswith("="):
        return False
    if text.count(" ") < 1:
        return False
    if any(token in text for token in ("syn.:", "cf.:", "http://", "https://")):
        return False
    return True


def build_id_table_rows(dump_path: Path):
    attrs = {}
    indexes = {}
    sententiae = {}
    sentence_ids_by_manuindex = {}

    for tuple_content in _iter_insert_tuples(dump_path, "tindexeattrs"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 7:
            continue
        attr_id = int(values[0])
        attrs[attr_id] = {
            "id_indexe": int(values[1]) if values[1] is not None else None,
            "title": values[2],
            "deleted_at": values[6],
        }

    for tuple_content in _iter_insert_tuples(dump_path, "tindexes"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 7:
            continue
        index_id = int(values[0])
        indexes[index_id] = {
            "title_modern": values[1],
            "deleted_at": values[6],
        }

    for tuple_content in _iter_insert_tuples(dump_path, "tsentencias"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 8:
            continue
        sent_id = int(values[0])
        sententiae[sent_id] = {
            "title": values[1],
            "title_trans": values[2],
            "deleted_at": values[7],
        }

    for tuple_content in _iter_insert_tuples(dump_path, "tmanuindpars"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 9:
            continue
        manuindex_id = int(values[1])
        sent_id = int(values[2])
        deleted_at = values[8]
        if deleted_at is not None:
            continue
        sentence_ids_by_manuindex.setdefault(manuindex_id, []).append(sent_id)

    best_by_work_id = {}
    for tuple_content in _iter_insert_tuples(dump_path, "tmanuindexes"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 16:
            continue

        row_id = int(values[0])
        id_indexeattr = int(values[2]) if values[2] is not None else None
        newfol = values[4]
        oldfol = values[5]
        ornumber = values[6]
        laden = values[7]
        work_id = values[8]
        updated_at = values[14]
        deleted_at = values[15]

        if deleted_at is not None or work_id is None:
            continue
        work_id = str(work_id).strip()
        if not WORK_ID_PATTERN.search(work_id):
            continue

        title = None
        attr = attrs.get(id_indexeattr)
        index_title_modern = None
        if attr and attr.get("deleted_at") is None:
            title = _first_nonempty(attr.get("title"))
            id_indexe = attr.get("id_indexe")
            idx = indexes.get(id_indexe)
            if idx and idx.get("deleted_at") is None:
                index_title_modern = _first_nonempty(idx.get("title_modern"))
            if not title:
                title = index_title_modern

        modern_titles = []
        if index_title_modern:
            modern_titles.append(index_title_modern)

        for sent_id in sentence_ids_by_manuindex.get(row_id, []):
            sent = sententiae.get(sent_id)
            if not sent or sent.get("deleted_at") is not None:
                continue
            modern = _first_nonempty(sent.get("title_trans"))
            if _looks_like_modern_text(modern):
                modern_titles.append(modern)

        title = _append_modern_title(title, modern_titles)
        fol_or_p = _first_nonempty(newfol, oldfol, laden, ornumber)

        candidate = {"IDs": work_id, "title": title, "fol_or_p": fol_or_p}
        rank = (
            1 if title else 0,
            1 if fol_or_p else 0,
            str(updated_at or ""),
            row_id,
        )
        existing = best_by_work_id.get(work_id)
        if existing is None or rank > existing["rank"]:
            best_by_work_id[work_id] = {"rank": rank, "row": candidate}

    rows = [v["row"] for v in best_by_work_id.values()]
    rows.sort(key=lambda r: r["IDs"])
    return rows


def _parse_manuscripts(dump_path: Path):
    manuscripts = {}
    for tuple_content in _iter_insert_tuples(dump_path, "tmanuscriptes"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 35:
            continue
        manuscript_id = int(values[0])
        manuscripts[manuscript_id] = {
            "signature": values[1],
            "digversionurl": values[3],
            "title": values[4],
            "titlealtern": values[14],
            "rismurl": values[13],
            "vd16": values[15],
            "vd16url": values[17],
            "deleted_at": values[34],
        }
    return manuscripts


def _pick_best_manuscript_id(source_counts, manuscripts):
    def score(item):
        manuscript_id, count = item
        m = manuscripts.get(manuscript_id, {})
        completeness = sum(
            1
            for value in [
                m.get("signature"),
                m.get("titlealtern"),
                m.get("title"),
                m.get("digversionurl"),
                m.get("rismurl"),
                m.get("vd16url"),
                m.get("vd16"),
            ]
            if _first_nonempty(value)
        )
        return (count, completeness, manuscript_id)

    return max(source_counts.items(), key=score)[0]


def build_sources_table_rows(dump_path: Path):
    manuscripts = _parse_manuscripts(dump_path)
    source_to_manuscript_counts = {}
    source_without_manuscript = set()

    for tuple_content in _iter_insert_tuples(dump_path, "tmanuindexes"):
        values = [
            _decode_sql_value(v) for v in _split_sql_fields(tuple_content)
        ]
        if len(values) < 16:
            continue
        manuscript_id = values[1]
        work_id = values[8]
        deleted_at = values[15]

        if deleted_at is not None or work_id is None:
            continue

        work_id = str(work_id).strip()
        marker_idx = work_id.rfind("_n")
        if marker_idx <= 0:
            continue
        source_id = work_id[:marker_idx]
        if not source_id:
            continue

        if manuscript_id is None:
            source_without_manuscript.add(source_id)
            continue

        manuscript_id = int(manuscript_id)
        if source_id not in source_to_manuscript_counts:
            source_to_manuscript_counts[source_id] = {}
        source_to_manuscript_counts[source_id][manuscript_id] = (
            source_to_manuscript_counts[source_id].get(manuscript_id, 0) + 1
        )

    all_source_ids = (
        set(source_to_manuscript_counts.keys()) | source_without_manuscript
    )

    rows = []
    existing_ids = set()
    existing_shelfmarks = set()
    for source_id in sorted(all_source_ids):
        manuscript = {}
        counts = source_to_manuscript_counts.get(source_id, {})
        if counts:
            best_manuscript_id = _pick_best_manuscript_id(counts, manuscripts)
            manuscript = manuscripts.get(best_manuscript_id, {}) or {}
            if manuscript.get("deleted_at") is not None:
                manuscript = {}

        row = {
            "ID": source_id,
            "Shelfmark": _first_nonempty(manuscript.get("signature")) or "",
            "Title": _first_nonempty(
                manuscript.get("titlealtern"), manuscript.get("title")
            )
            or "",
            "Source_link": _first_nonempty(manuscript.get("digversionurl"))
            or "",
            "RISM_link": _first_nonempty(manuscript.get("rismurl")) or "",
            "VD_16": _first_nonempty(
                manuscript.get("vd16url"), manuscript.get("vd16")
            )
            or "",
        }
        rows.append(row)
        existing_ids.add(row["ID"])
        if row["Shelfmark"]:
            existing_shelfmarks.add(row["Shelfmark"])

    for manuscript in manuscripts.values():
        if manuscript.get("deleted_at") is not None:
            continue
        shelfmark = _first_nonempty(manuscript.get("signature"))
        if not shelfmark:
            continue
        if shelfmark in existing_ids or shelfmark in existing_shelfmarks:
            continue

        row = {
            "ID": "",
            "Shelfmark": shelfmark,
            "Title": _first_nonempty(
                manuscript.get("titlealtern"), manuscript.get("title")
            )
            or "",
            "Source_link": _first_nonempty(manuscript.get("digversionurl"))
            or "",
            "RISM_link": _first_nonempty(manuscript.get("rismurl")) or "",
            "VD_16": _first_nonempty(
                manuscript.get("vd16url"), manuscript.get("vd16")
            )
            or "",
        }
        rows.append(row)
        existing_shelfmarks.add(row["Shelfmark"])

    rows.sort(key=lambda r: r["ID"])
    return rows


def write_id_table_csv(rows, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["IDs", "title", "fol_or_p"])
        writer.writeheader()
        writer.writerows(rows)


def write_sources_table_csv(rows, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ID",
                "Shelfmark",
                "Title",
                "Source_link",
                "RISM_link",
                "VD_16",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate id_table.csv and/or sources_table.csv from Schoenberg SQL dump."
    )
    parser.add_argument(
        "dump",
        help="Direct path to SQL dump file.",
    )
    parser.add_argument(
        "--id-output",
        default="scripts/upload_to_RDM/tables/id_table.csv",
        help="Output path for id_table.csv.",
    )
    parser.add_argument(
        "--sources-output",
        default="scripts/upload_to_RDM/tables/sources_table.csv",
        help="Output path for sources_table.csv.",
    )
    parser.add_argument(
        "--tables",
        choices=["both", "id", "sources"],
        default="both",
        help="Which table(s) to generate.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dump_path = Path(args.dump)
    id_output_path = Path(args.id_output)
    sources_output_path = Path(args.sources_output)

    if not dump_path.exists():
        raise FileNotFoundError(f"Dump not found: {dump_path}")

    if args.tables in ("both", "id"):
        id_rows = build_id_table_rows(dump_path)
        write_id_table_csv(id_rows, id_output_path)
        print(f"Wrote {len(id_rows)} rows to {id_output_path}")

    if args.tables in ("both", "sources"):
        source_rows = build_sources_table_rows(dump_path)
        write_sources_table_csv(source_rows, sources_output_path)
        print(f"Wrote {len(source_rows)} rows to {sources_output_path}")


if __name__ == "__main__":
    main()
