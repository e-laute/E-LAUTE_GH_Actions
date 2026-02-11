"""
Release orchestration script for the upload of MEI files to the TU-RDM platform in the E-LAUTE GitHub Actions workflow.

"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable

import validate_encodings


# TODO: how to proceed with original ILT or FLT files?
# For now, only CMN and GLT allowed because some repos contain old ILT/FLT files.
FILENAME_PATTERN = re.compile(r"^.+_enc_(?:ed|dipl)_(?:CMN|GLT)\.mei$")
WORK_ID_PATTERN = re.compile(r"^(.+_n\d+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run release steps for the upload of MEI files to the TU-RDM platform in the E-LAUTE GitHub Actions workflow."
    )
    parser.add_argument(
        "--caller-repo-path",
        default=os.environ.get("CALLER_REPO_PATH", ""),
        help="Path to caller repo. Defaults to CALLER_REPO_PATH env or <repo_root>/caller-repo.",
    )
    parser.add_argument(
        "--upload-mode",
        choices=["testing", "production"],
        default=os.environ.get("RELEASE_UPLOAD_MODE", "testing"),
        help="Mode used for upload_meis.py.",
    )
    return parser.parse_args()


def resolve_caller_repo_path(
    caller_repo_path_arg: str, repo_root: Path
) -> Path:
    if caller_repo_path_arg:
        return Path(caller_repo_path_arg).resolve()
    return repo_root.joinpath("caller-repo").resolve()


def parse_excluded_ids(caller_repo_path: Path) -> set[str]:
    exclude_file = caller_repo_path.joinpath("EXCLUDE.md")
    if not exclude_file.exists():
        print(f"[INFO] Optional exclude file not found: {exclude_file}")
        return set()

    excluded: set[str] = set()
    for raw_line in exclude_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        line = re.sub(r"^[-*]\s*\[[ xX]\]\s*", "", line)
        line = re.sub(r"^[-*]\s*", "", line)
        line = line.strip().strip("`")
        if not line:
            continue

        candidate = re.split(r"\s+", line, maxsplit=1)[0]
        candidate = candidate.strip("`").strip(",;")
        if candidate:
            excluded.add(candidate)

    print(f"[INFO] Excluded work_ids from EXCLUDE.md: {len(excluded)}")
    return excluded


def get_work_id_from_filename(file_name: str) -> str:
    """
    Derive a work_id from an MEI filename.
    Example: Jud_1523-2_n10_18v_enc_dipl_GLT.mei -> Jud_1523-2_n10
    """
    base_name = file_name.removesuffix(".mei")
    match = WORK_ID_PATTERN.match(base_name)
    if match:
        return match.group(1)
    if "_" in base_name:
        return base_name.rsplit("_", 1)[0]
    return base_name


def filename_matches(file_name: str) -> bool:
    return bool(FILENAME_PATTERN.match(file_name))


def discover_eligible_ids(
    caller_repo_path: Path,
) -> list[str]:
    candidate_ids = sorted(
        folder.name
        for folder in caller_repo_path.iterdir()
        if folder.is_dir() and not folder.name.startswith(".")
    )
    eligible_ids = candidate_ids

    matched_files_count = 0
    invalid_files_count = 0
    for folder_id in eligible_ids:
        folder_path = caller_repo_path.joinpath(folder_id)
        for file_path in sorted(
            p for p in folder_path.rglob("*") if p.is_file()
        ):
            if not filename_matches(file_path.name):
                invalid_files_count += 1
                continue
            matched_files_count += 1

    print(f"[INFO] Eligible IDs discovered: {len(eligible_ids)}")
    print(
        "[INFO] Source file scan complete: "
        f"{matched_files_count} matching files, {invalid_files_count} ignored"
    )

    return eligible_ids


def run_validation(caller_repo_path: Path, eligible_ids: Iterable[str]) -> bool:
    print("[STEP] Validation started.")
    failed_id_folders: list[str] = []

    for folder_id in eligible_ids:
        id_folder = caller_repo_path.joinpath(folder_id)
        print(f"[INFO] Validating folder: {id_folder}")
        try:
            folder_valid = validate_encodings.main(str(id_folder))
        except Exception as exc:
            print(f"[ERROR] Validation crashed for {folder_id}: {exc}")
            folder_valid = False

        if not folder_valid:
            failed_id_folders.append(folder_id)

    if failed_id_folders:
        print(
            "[WARN] Validation failed. All folders were checked before stopping."
        )
        print("[WARN] Failed folders:")
        for folder_id in failed_id_folders:
            print(f"  - {folder_id}")
        return False

    print("[STEP] Validation completed successfully.")
    return True


def run_subprocess(
    command: list[str], cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    print(f"[CMD] {' '.join(command)}")
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result


def run_derive_on_id_folders(
    repo_root: Path, caller_repo_path: Path, eligible_ids: Iterable[str]
) -> bool:
    print("[STEP] Derive alternate notation files started.")
    derive_script = repo_root.joinpath(
        "scripts", "derive-alternate-tablature-notation-types.py"
    )
    if not derive_script.exists():
        print(f"[ERROR] Derive script not found: {derive_script}")
        return False

    failed_ids: list[str] = []
    for folder_id in eligible_ids:
        id_folder = caller_repo_path.joinpath(folder_id)
        result = run_subprocess(
            [sys.executable, str(derive_script), str(id_folder)],
            cwd=repo_root,
        )
        if result.returncode != 0:
            failed_ids.append(folder_id)

    if failed_ids:
        print("[ERROR] Derive step failed for folders:")
        for folder_id in failed_ids:
            print(f"  - {folder_id}")
        return False

    print("[STEP] Derive step completed.")
    return True


def export_generated_files_stub(generated_files: Iterable[str]) -> None:
    print(
        "[TODO] Export generated files to external target repository (not caller-repo)."
    )
    print(
        f"[TODO] Generated files pending external export: {len(list(generated_files))}"
    )


def stage_converted_mei_files_by_id(
    caller_repo_path: Path,
    eligible_ids: Iterable[str],
    excluded_ids: set[str],
) -> tuple[Path, dict[str, list[str]]]:
    staging_root = Path(tempfile.mkdtemp(prefix="elaute_release_")).resolve()
    files_by_id: dict[str, list[str]] = {}
    excluded_files_count = 0
    for folder_id in eligible_ids:
        folder_root = caller_repo_path.joinpath(folder_id)
        converted_sources = sorted(
            path.resolve()
            for path in folder_root.rglob("*.mei")
            if "converted" in path.parts
        )
        staged_files: list[str] = []
        for source_path in converted_sources:
            work_id = get_work_id_from_filename(source_path.name)
            if work_id in excluded_ids:
                excluded_files_count += 1
                continue
            relative_path = source_path.relative_to(folder_root)
            target_path = staging_root.joinpath(folder_id, relative_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
            staged_files.append(str(target_path.resolve()))

        files_by_id[folder_id] = staged_files
        print(
            f"[INFO] Converted MEI files prepared for upload ({folder_id}): "
            f"{len(staged_files)}"
        )
    if excluded_files_count > 0:
        print(
            "[INFO] Converted MEI files excluded by EXCLUDE.md: "
            f"{excluded_files_count}"
        )
    print(f"[INFO] Staging root for converted files: {staging_root}")
    return staging_root, files_by_id


def cleanup_converted_directories(
    caller_repo_path: Path, eligible_ids: Iterable[str]
) -> None:
    removed_count = 0
    for folder_id in eligible_ids:
        folder_root = caller_repo_path.joinpath(folder_id)
        for converted_dir in sorted(
            path for path in folder_root.rglob("converted") if path.is_dir()
        ):
            shutil.rmtree(converted_dir, ignore_errors=True)
            removed_count += 1
    print(
        f"[INFO] Removed converted directories from caller repo: {removed_count}"
    )


def run_upload_on_id_folders(
    repo_root: Path,
    scripts_path: Path,
    workspace_root_for_upload: Path,
    eligible_ids: Iterable[str],
    converted_files_by_id: dict[str, list[str]],
    upload_mode: str,
) -> bool:
    print(f"[STEP] Upload step started in mode '{upload_mode}'.")
    failed_ids: list[str] = []

    base_env = os.environ.copy()
    python_path = base_env.get("PYTHONPATH", "")
    if python_path:
        base_env["PYTHONPATH"] = f"{scripts_path}{os.pathsep}{python_path}"
    else:
        base_env["PYTHONPATH"] = str(scripts_path)

    for folder_id in eligible_ids:
        id_folder = workspace_root_for_upload.joinpath(folder_id)
        selected_files = sorted(set(converted_files_by_id.get(folder_id, [])))
        if not selected_files:
            print(
                f"[ERROR] No converted files found for '{folder_id}'. "
                "Upload requires converted/*.mei files."
            )
            failed_ids.append(folder_id)
            continue

        manifest_path = ""
        env = base_env.copy()
        env["GITHUB_WORKSPACE"] = str(id_folder)
        env["ELAUTE_ONLY_CONVERTED"] = "1"
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".txt",
                encoding="utf-8",
                delete=False,
            ) as manifest_file:
                manifest_file.write("\n".join(selected_files))
                manifest_path = manifest_file.name
            env["ELAUTE_UPLOAD_FILE_LIST"] = manifest_path
            result = run_subprocess(
                [
                    sys.executable,
                    "-m",
                    "upload_to_RDM.upload_meis",
                    f"--{upload_mode}",
                ],
                cwd=repo_root,
                env=env,
            )
        finally:
            if manifest_path:
                Path(manifest_path).unlink(missing_ok=True)
        if result.returncode != 0:
            failed_ids.append(folder_id)

    if failed_ids:
        print("[ERROR] Upload step failed for folders:")
        for folder_id in failed_ids:
            print(f"  - {folder_id}")
        return False

    print("[STEP] Upload step completed successfully.")
    return True


def main() -> int:
    args = parse_args()
    scripts_path = Path(__file__).resolve().parent
    repo_root = scripts_path.parent

    caller_repo_path = resolve_caller_repo_path(
        args.caller_repo_path, repo_root
    )
    print(f"[INFO] Caller repo path: {caller_repo_path}")
    if not caller_repo_path.is_dir():
        print(f"[ERROR] Caller repo path not found: {caller_repo_path}")
        return 1

    excluded_ids = parse_excluded_ids(caller_repo_path)
    eligible_ids = discover_eligible_ids(caller_repo_path)
    if not eligible_ids:
        print("[WARN] No eligible ID folders found. Nothing to process.")
        return 0

    validation_ok = run_validation(caller_repo_path, eligible_ids)
    if not validation_ok:
        print("[ERROR] Release process stopped due to validation errors.")
        return 1

    derive_ok = run_derive_on_id_folders(
        repo_root, caller_repo_path, eligible_ids
    )
    if not derive_ok:
        print("[ERROR] Release process stopped due to derive step errors.")
        return 1

    staging_root, converted_files_by_id = stage_converted_mei_files_by_id(
        caller_repo_path, eligible_ids, excluded_ids
    )
    converted_files = [
        file_path
        for folder_id in eligible_ids
        for file_path in converted_files_by_id.get(folder_id, [])
    ]
    print(f"[INFO] Converted files prepared for upload: {len(converted_files)}")

    cleanup_converted_directories(caller_repo_path, eligible_ids)

    export_generated_files_stub(converted_files)

    try:
        upload_ok = run_upload_on_id_folders(
            repo_root=repo_root,
            scripts_path=scripts_path,
            workspace_root_for_upload=staging_root,
            eligible_ids=eligible_ids,
            converted_files_by_id=converted_files_by_id,
            upload_mode=args.upload_mode,
        )
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
        print(f"[INFO] Removed staging workspace: {staging_root}")
    if not upload_ok:
        print("[ERROR] Release process stopped due to upload errors.")
        return 1

    print("[INFO] Release steps completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
