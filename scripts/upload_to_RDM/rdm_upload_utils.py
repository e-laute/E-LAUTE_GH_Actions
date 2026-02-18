"""
Utility functions for uploading to the TU-RDM platform.
"""

import requests
import os
import json
import hashlib
import argparse
from pathlib import Path
from urllib.parse import quote, urlparse

import pandas as pd

# from pathlib import Path


def get_id_from_api(url):
    """Get community ID from API URL with error handling"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching community ID from {url}: {e}")
        return None


def setup_for_rdm_api_access(TESTING_MODE=True):

    # TODO: remove need for mapping file and url list
    # fetch that info from RDM

    TESTING_MODE = TESTING_MODE  # Set to False for production

    # see Stackoverflow: https://stackoverflow.com/a/66593457 about use in GitHub Actions
    # variable/secret needs to be passed in the GitHub Action
    # - name: Test env vars for python
    #     run: TEST_SECRET=${{ secrets.MY_TOKEN }} python -c 'import os;print(os.environ['TEST_SECRET'])

    if TESTING_MODE:
        RDM_API_URL = "https://test.researchdata.tuwien.ac.at/api"
        ELAUTE_COMMUNITY_ID = get_id_from_api(
            f"{RDM_API_URL}/communities/e-laute-test"
        )
        print("🧪 Running in GitHubActions TESTING mode")
        RDM_API_TOKEN = os.environ["RDM_TEST_API_TOKEN_JJ"]
    else:
        RDM_API_URL = "https://researchdata.tuwien.ac.at/api"
        ELAUTE_COMMUNITY_ID = get_id_from_api(
            f"{RDM_API_URL}/communities/e-laute"
        )
        print(" 🚀 Running in GitHubActions PRODUCTION mode")
        RDM_API_TOKEN = os.environ["RDM_API_TOKEN_JJ"]

    # Use the repo root when called from GitHub Actions; fallback to home.
    FILES_PATH = os.environ.get("GITHUB_WORKSPACE", os.path.expanduser("~"))

    return (
        RDM_API_URL,
        RDM_API_TOKEN,
        FILES_PATH,
        ELAUTE_COMMUNITY_ID,
    )


def parse_rdm_cli_args(description):
    parser = argparse.ArgumentParser(description=description)
    env_group = parser.add_mutually_exclusive_group()
    env_group.add_argument(
        "--testing",
        action="store_true",
        help="Use the testing RDM instance (default).",
    )
    env_group.add_argument(
        "--production",
        action="store_true",
        help="Use the production RDM instance.",
    )
    args = parser.parse_args()

    testing_mode = not args.production
    if args.testing:
        testing_mode = True

    return testing_mode


# Utility: make HTML link
def make_html_link(url):
    return f'<a href="{url}" target="_blank">{url}</a>'


def load_sources_table_csv(
    sources_csv_path="scripts/upload_to_RDM/tables/sources_table.csv",
):
    """
    Load and normalize sources_table.csv generated from the Schoenberg dump.
    """
    csv_path = Path(sources_csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find sources table CSV at {csv_path}. "
            "Generate it via "
            "'python scripts/upload_to_RDM/build_tables_from_dump.py <path_to_dump.sql>'."
        )

    sources_df = pd.read_csv(csv_path, dtype="string")
    required_columns = [
        "ID",
        "Shelfmark",
        "Title",
        "Source_link",
        "RISM_link",
        "VD_16",
    ]
    missing = [col for col in required_columns if col not in sources_df.columns]
    if missing:
        raise KeyError(
            "sources_table.csv is missing required columns: "
            + ", ".join(missing)
        )

    sources_table = pd.DataFrame()
    source_id_series = sources_df["ID"].replace(r"^\s*$", pd.NA, regex=True)
    sources_table["source_id"] = source_id_series.fillna(
        sources_df["Shelfmark"]
    )
    sources_table["Title"] = sources_df["Title"]
    sources_table["Source_link"] = sources_df["Source_link"].fillna("")
    sources_table["RISM_link"] = sources_df["RISM_link"].fillna("")
    sources_table["VD_16"] = sources_df["VD_16"].fillna("")
    return sources_table


# Utility: look up source title in csv created from dump of e-lautedb
def look_up_source_title(sources_table, source_id):
    title_series = sources_table.loc[
        sources_table["source_id"] == source_id, "Title"
    ]
    if not title_series.empty:
        return title_series.values[0]
    return None


# Utility: look up source links (stub, replace with actual lookup if needed)
def look_up_source_links(sources_table, source_id):
    source_link_series = sources_table.loc[
        sources_table["source_id"] == source_id,
        "Source_link",
    ]
    rism_series = sources_table.loc[
        sources_table["source_id"] == source_id,
        "RISM_link",
    ]
    vd16_series = sources_table.loc[
        sources_table["source_id"] == source_id,
        "VD_16",
    ]

    source_link = (
        source_link_series.iloc[0] if not source_link_series.empty else ""
    )
    rism = rism_series.iloc[0] if not rism_series.empty else ""
    vd16 = vd16_series.iloc[0] if not vd16_series.empty else ""

    links = []
    if source_link:
        links.append(source_link)
    if rism:
        links.append(rism)
    if vd16:
        links.append(vd16)

    return links


def create_related_identifiers(links):
    related_identifiers = []
    for link in links:
        normalized_link = _normalize_url_identifier(link)
        if not normalized_link:
            print("Skipping non-URL related identifier value: " f"{repr(link)}")
            continue
        related_identifiers.append(
            {
                "identifier": normalized_link,
                "relation_type": {
                    "id": "ispartof",
                    "title": {"en": "Is part of"},
                },
                "resource_type": {
                    "id": "other",
                    "title": {"de": "Anderes", "en": "Other"},
                },
                "scheme": "url",
            },
        )
    return related_identifiers


def _normalize_url_identifier(value):
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    candidate = text
    if candidate.startswith("www."):
        candidate = f"https://{candidate}"
    elif "://" not in candidate and " " not in candidate:
        # Handle URLs in the source table that omit scheme.
        head = candidate.split("/", 1)[0]
        if "." in head:
            candidate = f"https://{candidate}"

    parsed = urlparse(candidate)
    if parsed.scheme in ("http", "https") and parsed.netloc:
        return candidate

    return None


def set_headers(RDM_API_TOKEN):

    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RDM_API_TOKEN}",
    }
    fh = {
        "Accept": "application/json",
        "Content-Type": "application/octet-stream",
        "Authorization": f"Bearer {RDM_API_TOKEN}",
    }
    return h, fh


def get_records_from_RDM(RDM_API_TOKEN, RDM_API_URL, ELAUTE_COMMUNITY_ID):
    """
    Fetch records from the RDM API.
    """
    h, _fh = set_headers(RDM_API_TOKEN)
    response = requests.get(
        f"{RDM_API_URL}/communities/{ELAUTE_COMMUNITY_ID}/records",
        headers=h,
    )

    if not response.status_code == 200:
        print(f"Error fetching records from RDM: {response.status_code}")
        return None

    records = []
    hits = response.json().get("hits", {}).get("hits", [])
    for hit in hits:
        record_id = hit.get("id")
        parent_id = hit.get("parent", {}).get("id")
        file_count = hit.get("files", {}).get("count")
        created = hit.get("created")
        updated = hit.get("updated")
        # Try to extract elaute_id from identifiers (other)
        elaute_id = None
        metadata = hit.get("metadata", {})
        identifiers = metadata.get("identifiers")
        for ident in identifiers or []:
            if ident.get("scheme") == "other":
                elaute_id = ident.get("identifier")
                break
        # if not elaute_id:
        # print(f"Unknown E-LAUTE ID for record {record_id}")
        records.append(
            {
                "elaute_id": elaute_id,
                "record_id": record_id,
                "parent_id": parent_id,
                "file_count": file_count,
                "created": created,
                "updated": updated,
            }
        )
    return pd.DataFrame(records)


def _extract_metadata_payload(metadata):
    if isinstance(metadata, dict) and "metadata" in metadata:
        return metadata.get("metadata") or {}
    return metadata or {}


def _ensure_elaute_identifier(metadata, elaute_id):
    """
    Ensure metadata contains the E-LAUTE id as an "other" identifier.
    This keeps dedup/update mapping stable across all upload scripts.
    """
    if not isinstance(metadata, dict) or not elaute_id:
        return metadata

    metadata_payload = metadata.setdefault("metadata", {})
    identifiers = metadata_payload.setdefault("identifiers", [])
    if not isinstance(identifiers, list):
        identifiers = []
        metadata_payload["identifiers"] = identifiers

    has_identifier = any(
        isinstance(item, dict)
        and item.get("scheme") == "other"
        and str(item.get("identifier", "")).strip() == str(elaute_id)
        for item in identifiers
    )
    if not has_identifier:
        identifiers.append({"identifier": str(elaute_id), "scheme": "other"})

    return metadata


def _metadata_changed_for_update(
    current_record, new_metadata_payload, fields_to_compare
):
    current_metadata = current_record.get("metadata", {})
    changed_fields = []
    for field in fields_to_compare:
        current_value = current_metadata.get(field)
        new_value = new_metadata_payload.get(field)
        if not compare_hashed_files(current_value, new_value):
            changed_fields.append(field)
    return bool(changed_fields), changed_fields


def _get_remote_file_info(record_id, headers, api_url):
    try:
        response = requests.get(
            f"{api_url}/records/{record_id}/files", headers=headers
        )
        if response.status_code != 200:
            print(
                f"Failed to fetch file list for record {record_id} "
                f"(code: {response.status_code})"
            )
            return None
        entries = response.json().get("entries", [])
    except Exception as exc:
        print(f"Failed to fetch file list for record {record_id}: {exc}")
        return None

    remote_info = {}
    for entry in entries:
        key = entry.get("key")
        if not key:
            continue
        remote_info[key] = {
            "size": entry.get("size"),
            "checksum": entry.get("checksum"),
        }
    return remote_info


def _calculate_local_md5_checksum(file_path):
    digest = hashlib.md5()
    with file_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            digest.update(chunk)
    return f"md5:{digest.hexdigest()}"


def _files_changed_for_update(record_id, local_file_paths, headers, api_url):
    remote_info = _get_remote_file_info(record_id, headers, api_url)
    if remote_info is None:
        print("Could not verify remote files; assuming changes exist.")
        return True

    local_names = {path.name for path in local_file_paths}
    remote_names = set(remote_info.keys())

    if local_names != remote_names:
        missing = sorted(local_names - remote_names)
        extra = sorted(remote_names - local_names)
        if missing:
            print(f"Remote missing files: {missing}")
        if extra:
            print(f"Remote has extra files: {extra}")
        return True

    for path in local_file_paths:
        remote_entry = remote_info.get(path.name, {})
        remote_checksum = remote_entry.get("checksum")
        if not remote_checksum:
            print(
                "Remote checksum missing for "
                f"{path.name}; assuming changes exist."
            )
            return True

        local_checksum = _calculate_local_md5_checksum(path)
        if not compare_hashed_files(remote_checksum, local_checksum):
            print(
                "Checksum mismatch for "
                f"{path.name}: remote={remote_checksum}, local={local_checksum}"
            )
            return True

    return False


def _upload_initialized_files(
    file_entries, local_file_paths, headers, file_headers
):
    file_links_by_key = {}
    for entry in file_entries:
        key = entry.get("key")
        links = entry.get("links", {})
        if key:
            file_links_by_key[key] = links

    for file_path in local_file_paths:
        filename = file_path.name
        links = file_links_by_key.get(filename, {})
        if "content" not in links or "commit" not in links:
            raise AssertionError(f"Missing upload/commit links for {filename}.")

        with file_path.open("rb") as fp:
            response = requests.put(
                links["content"],
                data=fp,
                headers=file_headers,
            )
        assert response.status_code == 200, (
            f"Failed to upload file content {filename} "
            f"(code: {response.status_code})"
        )

        response = requests.post(links["commit"], headers=headers)
        if response.status_code != 200:
            print(
                f"[WARN] Commit failed for {filename} "
                f"(code: {response.status_code}); retrying once."
            )
            response = requests.post(links["commit"], headers=headers)
        assert response.status_code == 200, (
            f"Failed to commit file {filename} (code: {response.status_code}) "
            f"response: {response.text[:300]}"
        )


def _ensure_community_review_request(
    record_id, headers, api_url, community_id, failed_uploads, elaute_id
):
    if not community_id:
        print(
            "ELAUTE_COMMUNITY_ID is not set; cannot submit review for "
            f"record {record_id}."
        )
        failed_uploads.append(elaute_id)
        return False

    response = requests.put(
        f"{api_url}/records/{record_id}/draft/review",
        headers=headers,
        data=json.dumps(
            {
                "receiver": {"community": community_id},
                "type": "community-submission",
            }
        ),
    )
    if response.status_code != 200:
        print(
            "Failed to set review for record "
            f"{record_id} (code: {response.status_code}) "
            f"response: {response.text[:300]}"
        )
        failed_uploads.append(elaute_id)
        return False
    return True


def _submit_review(record_id, headers, api_url, failed_uploads, elaute_id):
    response = requests.post(
        f"{api_url}/records/{record_id}/draft/actions/submit-review",
        headers=headers,
    )
    if response.status_code != 202:
        print(
            "Failed to submit review for record "
            f"{record_id} (code: {response.status_code}) "
            f"response: {response.text[:300]}"
        )
        failed_uploads.append(elaute_id)
        return False
    return True


def _sync_draft_files_with_local(
    record_id,
    local_file_paths,
    links,
    headers,
    file_headers,
    api_url,
):
    """
    Start from previous-version files, then apply local delta:
    - keep unchanged files
    - delete removed files from draft
    - re-upload only new/changed files
    """
    response = requests.post(
        f"{api_url}/records/{record_id}/draft/actions/files-import",
        headers=headers,
    )
    assert response.status_code in (200, 201), (
        "Failed to import files from previous version for record "
        f"{record_id} (code: {response.status_code}) "
        f"response: {response.text[:300]}"
    )

    response = requests.get(
        f"{api_url}/records/{record_id}/draft/files", headers=headers
    )
    assert response.status_code == 200, (
        f"Failed to list draft files for record {record_id} "
        f"(code: {response.status_code}) response: {response.text[:300]}"
    )
    draft_entries = response.json().get("entries", [])
    draft_files_by_key = {
        entry.get("key"): entry for entry in draft_entries if entry.get("key")
    }

    local_names = {path.name for path in local_file_paths}
    draft_names = set(draft_files_by_key.keys())

    # Remove files that are no longer present locally.
    for filename in sorted(draft_names - local_names):
        encoded_filename = quote(filename, safe="")
        response = requests.delete(
            f"{api_url}/records/{record_id}/draft/files/{encoded_filename}",
            headers=headers,
        )
        assert response.status_code in (200, 204), (
            f"Failed to delete draft file {filename} from record {record_id} "
            f"(code: {response.status_code}) response: {response.text[:300]}"
        )

    files_to_upload = []
    for file_path in local_file_paths:
        filename = file_path.name
        entry = draft_files_by_key.get(filename)
        if not entry:
            files_to_upload.append(file_path)
            continue

        remote_checksum = entry.get("checksum")
        local_checksum = _calculate_local_md5_checksum(file_path)
        if not remote_checksum or not compare_hashed_files(
            remote_checksum, local_checksum
        ):
            encoded_filename = quote(filename, safe="")
            response = requests.delete(
                f"{api_url}/records/{record_id}/draft/files/{encoded_filename}",
                headers=headers,
            )
            assert response.status_code in (200, 204), (
                f"Failed to replace changed draft file {filename} "
                f"(code: {response.status_code}) response: {response.text[:300]}"
            )
            files_to_upload.append(file_path)

    if files_to_upload:
        file_entries = [{"key": path.name} for path in files_to_upload]
        response = requests.post(
            links["files"],
            data=json.dumps(file_entries),
            headers=headers,
        )
        assert response.status_code == 201, (
            f"Failed to initialize files for record {record_id} "
            f"(code: {response.status_code}) response: {response.text[:300]}"
        )
        _upload_initialized_files(
            response.json().get("entries", []),
            files_to_upload,
            headers,
            file_headers,
        )


def upload_to_rdm(
    metadata,
    elaute_id,
    file_paths,
    RDM_API_TOKEN,
    RDM_API_URL,
    ELAUTE_COMMUNITY_ID,
    record_id=None,
):
    metadata = _ensure_elaute_identifier(metadata, elaute_id)
    new_upload = record_id is None
    records_api_url = f"{RDM_API_URL}/records"

    failed_uploads = []
    local_file_paths = [Path(path) for path in file_paths]
    print(f"Processing {elaute_id}: {len(local_file_paths)} files")
    h, fh = set_headers(RDM_API_TOKEN)

    if not new_upload:
        metadata_payload = _extract_metadata_payload(metadata)
        fields_to_compare = [
            "title",
            "creators",
            "contributors",
            "description",
            "identifiers",
            "dates",
            "publisher",
            "references",
            "related_identifiers",
            "resource_type",
            "rights",
        ]

        r = requests.get(f"{RDM_API_URL}/records/{record_id}", headers=h)
        if r.status_code != 200:
            print(f"Failed to fetch record {record_id} (code: {r.status_code})")
            failed_uploads.append(elaute_id)
            return failed_uploads

        metadata_changed, changed_fields = _metadata_changed_for_update(
            r.json(),
            metadata_payload,
            fields_to_compare,
        )
        files_changed = _files_changed_for_update(
            record_id, local_file_paths, h, RDM_API_URL
        )

        if not metadata_changed and not files_changed:
            print(
                "No metadata or file changes detected for "
                f"{elaute_id}; skipping update."
            )
            return failed_uploads
        if metadata_changed:
            print(
                "Metadata changes detected for "
                f"{elaute_id} in fields: {', '.join(changed_fields)}"
            )
        elif files_changed:
            print(
                "Metadata unchanged but file changes detected for "
                f"{elaute_id}; creating new version."
            )

        # Create a new version/draft for the record
        r = requests.post(
            f"{RDM_API_URL}/records/{record_id}/versions",
            headers=h,
        )
        if r.status_code != 201:
            print(
                f"Failed to create new version for record {record_id} (code: {r.status_code})"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads  # Stop further processing

        new_version_data = r.json()
        new_record_id = new_version_data["id"]
        print(f"Created new version {new_record_id} for elaute_id {elaute_id}")

        # Update the draft with new metadata
        r = requests.put(
            f"{RDM_API_URL}/records/{new_record_id}/draft",
            data=json.dumps(metadata),
            headers=h,
        )
        if r.status_code != 200:
            print(
                f"Failed to update draft {new_record_id} (code: {r.status_code})"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads

        # Use new_record_id for subsequent steps
        record_id = new_record_id
        # Get links from the draft update response
        links = r.json()["links"]
        record_id = r.json()["id"]

        # If file differences were detected, start from previous-version files
        # and only apply local delta.
        if files_changed:
            _sync_draft_files_with_local(
                record_id=record_id,
                local_file_paths=local_file_paths,
                links=links,
                headers=h,
                file_headers=fh,
                api_url=RDM_API_URL,
            )
    else:
        # Create new draft record
        r = requests.post(
            records_api_url,
            data=json.dumps(metadata),
            headers=h,
        )
        assert (
            r.status_code == 201
        ), f"Failed to create record (code: {r.status_code})"
        links = r.json()["links"]
        record_id = r.json()["id"]
        # Keep current per-file initialization behavior for new records.
        for file_path in local_file_paths:
            filename = file_path.name
            r = requests.post(
                links["files"],
                data=json.dumps([{"key": filename}]),
                headers=h,
            )
            assert r.status_code == 201, (
                f"Failed to initialize file {filename} (code: {r.status_code}) "
                f"response: {r.text[:300]}"
            )
            _upload_initialized_files(
                r.json().get("entries", []),
                [file_path],
                h,
                fh,
            )

    if new_upload:
        # Set community review request first.
        r = requests.put(
            f"{records_api_url}/{record_id}/draft/review",
            headers=h,
            data=json.dumps(
                {
                    "receiver": {"community": ELAUTE_COMMUNITY_ID},
                    "type": "community-submission",
                }
            ),
        )
        if r.status_code != 200:
            print(
                "Failed to set review for record "
                f"{record_id} (code: {r.status_code}) "
                f"response: {r.text[:300]}"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads

        # Create curation request for the record.
        r = requests.post(
            f"{RDM_API_URL}/curations",
            headers=h,
            data=json.dumps({"topic": {"record": record_id}}),
        )
        if r.status_code != 201:
            print(
                "Failed to create curation for record "
                f"{record_id} (code: {r.status_code}) "
                f"response: {r.text[:300]}"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads

        # Submit community review request.
        r = requests.post(
            f"{records_api_url}/{record_id}/draft/actions/submit-review",
            headers=h,
        )
        if r.status_code != 202:
            print(
                "Failed to submit review for record "
                f"{record_id} (code: {r.status_code}) "
                f"response: {r.text[:300]}"
            )
            failed_uploads.append(elaute_id)
            return failed_uploads

        print(
            "[INFO] Draft upload complete and submitted for review. "
            f"Draft: {RDM_API_URL}/uploads/{record_id}"
        )
    else:
        # Keep updates as drafts only.
        if files_changed:
            print(
                "[INFO] Update draft saved with file changes. "
                f"Draft: {RDM_API_URL}/uploads/{record_id}"
            )
        else:
            print(
                "[INFO] Metadata-only update draft saved. "
                f"Draft: {RDM_API_URL}/uploads/{record_id}"
            )

    return failed_uploads


def compare_hashed_files(current_value, new_value):
    """
    Compare two metadata values by hashing their raw JSON representation.
    Any change (including whitespace, empty fields, ordering before JSON
    normalization, etc.) will count as a difference.
    """

    def _hash_value(value):
        try:
            serialized = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            serialized = str(value)

        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    return _hash_value(current_value) == _hash_value(new_value)


# if __name__ == "__main__":
#     pass
