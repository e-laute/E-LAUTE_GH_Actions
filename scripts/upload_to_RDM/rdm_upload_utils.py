"""
Utility functions for uploading to the TU-RDM platform.
"""

import requests
import os
import json
import hashlib
import argparse
import time

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


# Utility: look up source title (stub, replace with actual lookup if needed)
def look_up_source_title(sources_table, source_id):
    # This should look up the title from a table or database; placeholder:
    title_series = sources_table.loc[
        sources_table["source_id"] == source_id, "Title"
    ]
    if not title_series.empty:
        return title_series.values[0]
    return None


# Utility: look up source links (stub, replace with actual lookup if needed)
def look_up_source_links(sources_table, source_id):
    source_link = sources_table.loc[
        sources_table["source_id"] == source_id,
        "Source_link",
    ].values[0]
    rism = sources_table.loc[
        sources_table["source_id"] == source_id,
        "RISM_link",
    ].values[0]
    vd16 = sources_table.loc[
        sources_table["source_id"] == source_id,
        "VD_16",
    ].values[0]

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
        related_identifiers.append(
            {
                "identifier": link,
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
        if not elaute_id:
            print(f"Unknown E-LAUTE ID for record {record_id}")
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


def upload_to_rdm(
    metadata,
    elaute_id,
    file_paths,
    RDM_API_TOKEN,
    RDM_API_URL,
    ELAUTE_COMMUNITY_ID,
    record_id=None,
):
    new_upload = record_id is None

    failed_uploads = []
    print(f"Processing {elaute_id}: {len(file_paths)} files")
    h, fh = set_headers(RDM_API_TOKEN)

    print("record_id:", record_id)

    if not new_upload:
        # Create a new version/draft for the record
        r = requests.post(
            f"{RDM_API_URL}/records/{record_id}/versions", headers=h
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
        print(links)
    else:
        # Create new draft record
        r = requests.post(
            f"{RDM_API_URL}/records", data=json.dumps(metadata), headers=h
        )
        assert (
            r.status_code == 201
        ), f"Failed to create record (code: {r.status_code})"
        links = r.json()["links"]
        record_id = r.json()["id"]
        print(links)

    # Upload each file with its own init/upload/commit lifecycle.
    file_keys = [os.path.basename(file_path) for file_path in file_paths]
    if len(file_keys) != len(set(file_keys)):
        duplicates = sorted({key for key in file_keys if file_keys.count(key) > 1})
        raise AssertionError(
            "Duplicate filenames in upload payload are not allowed: "
            + ", ".join(duplicates)
        )
    for idx, file_path in enumerate(file_paths, start=1):
        filename = os.path.basename(file_path)
        r = requests.post(
            links["files"],
            data=json.dumps([{"key": filename}]),
            headers=h,
        )
        assert (
            r.status_code == 201
        ), f"Failed to initialize file {filename} (code: {r.status_code})"
        response_entries = r.json().get("entries", [])
        selected_entry = next(
            (entry for entry in response_entries if entry.get("key") == filename),
            None,
        )
        if selected_entry is None and response_entries:
            selected_entry = response_entries[-1]
        assert (
            selected_entry is not None
        ), f"No initialized file entry returned for '{filename}'."
        file_links = selected_entry["links"]

        # Upload file content by streaming the data
        with open(file_path, "rb") as fp:
            r = requests.put(file_links["content"], data=fp, headers=fh)
        assert (
            r.status_code == 200
        ), f"Failed to upload file content {filename} (code: {r.status_code})"

        # Commit the file (retry transient 5xx errors).
        commit_ok = False
        last_commit_code = None
        for attempt in range(1, 4):
            r = requests.post(file_links["commit"], headers=h)
            last_commit_code = r.status_code
            if r.status_code == 200:
                commit_ok = True
                break
            if r.status_code >= 500:
                time.sleep(2)
                continue
            break
        assert commit_ok, (
            f"Failed to commit file {filename} (code: {last_commit_code})"
        )
        print(
            f"[INFO] Uploaded file {idx}/{len(file_paths)} for {elaute_id}: {filename}"
        )

    # Verify all expected files are completed before review/publish.
    expected_keys = set(file_keys)
    verified = False
    for attempt in range(1, 7):
        r = requests.get(links["files"], headers=h)
        if r.status_code != 200:
            print(
                f"[WARN] Could not list draft files for verification "
                f"(attempt {attempt}, code: {r.status_code})."
            )
            time.sleep(2)
            continue

        entries = r.json().get("entries", [])
        completed_keys = {
            entry.get("key")
            for entry in entries
            if entry.get("status") == "completed"
        }
        missing_keys = sorted(expected_keys - completed_keys)
        print(
            f"[INFO] Draft file verification attempt {attempt}: "
            f"{len(completed_keys)}/{len(expected_keys)} completed."
        )
        if not missing_keys:
            verified = True
            break
        if attempt < 6:
            print(
                f"[WARN] Waiting for pending files before review: {missing_keys}"
            )
            time.sleep(2)

    if not verified:
        print(
            f"[ERROR] Not all files completed for {elaute_id}; "
            "aborting review submission to avoid partial record."
        )
        failed_uploads.append(elaute_id)
        return failed_uploads

    # Add to E-LAUTE community review (new records only).
    if new_upload:
        if ELAUTE_COMMUNITY_ID:
            r = requests.put(
                f"{RDM_API_URL}/records/{record_id}/draft/review",
                headers=h,
                data=json.dumps(
                    {
                        "receiver": {"community": ELAUTE_COMMUNITY_ID},
                        "type": "community-submission",
                    }
                ),
            )
            assert (
                r.status_code == 200
            ), f"Failed to set review for record {record_id} (code: {r.status_code})"
        else:
            print(
                "Warning: ELAUTE_COMMUNITY_ID not set, skipping community submission"
            )

    # Create curation request (new records only).
    if new_upload:
        r = requests.post(
            f"{RDM_API_URL}/curations",
            headers=h,
            data=json.dumps({"topic": {"record": record_id}}),
        )
        assert (
            r.status_code == 201
        ), f"Failed to create curation for record {record_id} (code: {r.status_code})"

    # Submit draft for review (all records).
    r = requests.post(
        f"{RDM_API_URL}/records/{record_id}/draft/actions/submit-review",
        headers=h,
    )
    if r.status_code != 202:
        print(
            f"Failed to submit review for record {record_id} (code: {r.status_code})"
        )
        failed_uploads.append(elaute_id)
        return failed_uploads

    print(
        "[INFO] Draft upload complete and submitted for review; publish skipped for manual approval. "
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
